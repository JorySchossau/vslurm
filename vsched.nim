## vsched: the execution daemon. Once per second: lock DB, reap finished
## jobs, enforce time limits, evaluate dependencies + CPU budget, launch
## PENDING jobs, purge long-finished ones, rewrite the DB in place.
## The only component that ever executes job code.

import os, algorithm, sequtils, strutils, strtabs, tables, times, osproc
import posix except Time
import vslurm_common

const
  purgeSeconds = 300
  cancelGraceSeconds = 5

type
  DepResult = enum depSatisfied, depWaiting, depUnresolvable

proc countCPUs(): int =
  let n = posix.sysconf(posix.SC_NPROCESSORS_ONLN)
  if n > 0: n else: 1

proc pidAlive(pid: int): bool =
  ## kill(pid, 0) probes existence; ESRCH means gone, EPERM (not our child)
  ## still means the process exists.
  if pid <= 0: return false
  if posix.kill(Pid(pid), 0) == 0: return true
  result = posix.errno == posix.EPERM

proc buildEnv(j: Job; jobs: seq[Job]): StringTableRef =
  result = newStringTable(modeCaseSensitive)
  for k, v in envPairs():
    result[k] = v
  result["SLURM_JOB_ID"] = $j.id
  result["SLURM_JOB_NAME"] = j.name
  result["SLURM_SUBMIT_DIR"] = j.chdir
  result["SLURM_NTASKS"] = $j.ntasks
  result["SLURM_CPUS_PER_TASK"] = $j.cpus
  if j.dep.len > 0:
    result["SLURM_JOB_DEPENDENCY"] = j.dep
  if j.isArrayJob:
    var count = 0
    for k in jobs:
      if k.isElementJob and k.arrayId == j.arrayId: inc count
    result["SLURM_ARRAY_JOB_ID"] = $j.arrayId
    result["SLURM_ARRAY_TASK_ID"] = $j.arrayTask
    result["SLURM_ARRAY_TASK_COUNT"] = $count

proc runCommand(j: Job): string =
  ## The exact command vsched executes: either the submitted script (run
  ## under its shebang interpreter if it has one, else $SHELL) with the
  ## recorded args, or the --wrap payload under $SHELL -c.
  if j.wrap.len > 0:
    return getEnv("SHELL", "/bin/sh") & " -c " & shellQuote(j.wrap)
  let argv = if j.args.len > 0: shellWords(j.args) else: @[]
  var interp = ""
  var fh: File
  let scriptPath = resolvePath(j.chdir, j.script)
  if fh.open(scriptPath):
    let first = fh.readLine()
    fh.close()
    if first.len >= 2 and first[0] == '#' and first[1] == '!':
      interp = first[2 .. ^1].strip()
      # an interpreter arg like /usr/bin/env bash splits into two words
      let words = shellWords(interp)
      if words.len == 0 or words[0].len == 0 or words[0][0] != '/':
        interp = ""
      else:
        interp = words.join(" ")
  if interp.len == 0:
    interp = getEnv("SHELL", "/bin/sh")
  result = interp
  result &= " " & shellQuote(scriptPath)
  for a in argv:
    result &= " " & shellQuote(a)

proc launch(j: Job; jobs: seq[Job]; procs: var Table[int, Process]): int =
  let outPath = resolvePath(j.chdir, j.output)
  let dir = outPath.parentDir()
  if dir.len > 0: createDir(dir)
  var execLine = "exec " & runCommand(j)
  execLine &= " > " & shellQuote(outPath)
  if j.errorFile.len > 0:
    let errPath = resolvePath(j.chdir, j.errorFile)
    let edir = errPath.parentDir()
    if edir.len > 0: createDir(edir)
    execLine &= " 2> " & shellQuote(errPath)
  else:
    execLine &= " 2>&1"
  let p = startProcess("/bin/sh", args = ["-c", execLine], env = buildEnv(j, jobs),
    workingDir = j.chdir, options = {})
  procs[j.id] = p
  result = p.processID

proc resolveDepRef*(refStr: string; jobs: seq[Job]): tuple[found: bool, job: Job] =
  ## Resolve a dependency target: a plain id names a row, `master_task`
  ## names that array element's own row.
  let js = parseJobSpec(refStr)
  for k in jobs:
    if js.task < 0 and k.id == js.job:
      return (true, k)
    if js.task >= 0 and k.arrayId == js.job and k.arrayTask == js.task:
      return (true, k)
  (false, Job())

proc depGroupState(j: Job; group: string; jobs: seq[Job]; maxId: int): DepResult =
  ## Evaluate one comma group `type:id[:id...]`. A dep on an array master id
  ## means "that array finished"; element deps use `master_task` refs.
  let parts = group.split(':')
  if parts.len < 2: return depSatisfied
  let t = parts[0]
  for idStr in parts[1 .. ^1]:
    if idStr.len == 0: continue
    let r = resolveDepRef(idStr, jobs)
    if not r.found:
      # Not in the DB: either finished long ago and was purged (satisfied,
      # since jobs.seq guarantees the ID was allocated) or never existed.
      let js = parseJobSpec(idStr)
      if js.job <= maxId: continue
      return depUnresolvable
    let s = r.job.state
    var ok = false
    case t
    of "afterok": ok = s == stCompleted
    of "afternotok": ok = r.job.isTerminal and s != stCompleted
    of "afterany", "after": ok = r.job.isTerminal
    else: ok = true # unknown type: satisfied (warned at submit)
    if not ok: return depWaiting
  depSatisfied

proc depsSatisfied(j: Job; jobs: seq[Job]; maxId: int): DepResult =
  if j.dep.len == 0: return depSatisfied
  result = depSatisfied
  for group in j.dep.split(','):
    if group.len == 0: continue
    let r = depGroupState(j, group, jobs, maxId)
    if r == depUnresolvable: return depUnresolvable
    if r == depWaiting: result = depWaiting

proc tick(procs: var Table[int, Process]; cpuCap: int) =
  let db = dbPath()
  var f: File
  if not openDb(db, f):
    stderr.writeLine("vsched: cannot open " & db)
    quit(1)
  try:
    var jobs = loadJobs(f)
    let now = getTime()
    let nowStr = formatDbTime(now)
    let maxId = allocatedMaxId(db)

    # Phase A: reap finished children / adopt orphans.
    for i in 0 ..< jobs.len:
      if jobs[i].state != stRunning: continue
      if procs.hasKey(jobs[i].id):
        let ec = procs[jobs[i].id].peekExitCode
        if ec >= 0:
          procs[jobs[i].id].close()
          procs.del(jobs[i].id)
          jobs[i].state = if ec == 0: stCompleted else: stFailed
          jobs[i].exitcode = ec
          jobs[i].hasExit = true
          jobs[i].endTime = nowStr
      else:
        # Orphan (server restarted): if the PID is gone we can only mark it
        # finished — the true exit code is unknowable.
        if not pidAlive(jobs[i].pid):
          jobs[i].state = stCompleted
          jobs[i].exitcode = -1
          jobs[i].hasExit = false
          jobs[i].endTime = nowStr

    # Phase B: escalate lingering terminal jobs still holding a process.
    for i in 0 ..< jobs.len:
      if not jobs[i].isTerminal: continue
      if not procs.hasKey(jobs[i].id): continue
      if jobs[i].endTime.len == 0: continue
      let age = (now - parseDbTime(jobs[i].endTime)).inSeconds
      if procs[jobs[i].id].running:
        if age > cancelGraceSeconds: procs[jobs[i].id].kill()
      else:
        procs[jobs[i].id].close()
        procs.del(jobs[i].id)

    # Phase B2: an array master whose elements all reached a terminal state
    # is itself terminal (its completion means "the array finished").
    for i in 0 ..< jobs.len:
      if not jobs[i].isMasterJob: continue
      if jobs[i].isTerminal: continue
      var elems: seq[Job] = @[]
      for k in jobs:
        if k.isElementJob and k.arrayId == jobs[i].id: elems.add k
      if elems.len > 0 and elems.allIt(it.isTerminal):
        jobs[i].state = if elems.allIt(it.state == stCompleted): stCompleted else: stFailed
        jobs[i].endTime = nowStr

    # Phase C: fail PENDING jobs whose dependencies reference unallocated IDs.
    for i in 0 ..< jobs.len:
      if jobs[i].state != stPending: continue
      if depsSatisfied(jobs[i], jobs, maxId) == depUnresolvable:
        jobs[i].state = stFailed
        jobs[i].endTime = nowStr

    # Phase D: time-limit enforcement.
    for i in 0 ..< jobs.len:
      if jobs[i].state != stRunning: continue
      if not jobs[i].hasMinutes or jobs[i].minutes <= 0: continue
      if jobs[i].start.len == 0: continue
      let elapsed = (now - parseDbTime(jobs[i].start)).inSeconds
      if elapsed >= jobs[i].minutes * 60:
        if procs.hasKey(jobs[i].id):
          if procs[jobs[i].id].running: procs[jobs[i].id].kill()
        elif jobs[i].pid > 0:
          discard posix.kill(Pid(jobs[i].pid), SIGKILL)
        jobs[i].state = stTimeout
        jobs[i].endTime = nowStr

    # Phase E: launch PENDING jobs within the CPU budget.
    var usedCpus = 0
    for j in jobs:
      if j.state == stRunning: usedCpus += j.ntasks * j.cpus
    let cap = cpuCap
    var pend: seq[int] = @[]
    for k in 0 ..< jobs.len:
      if jobs[k].state == stPending: pend.add k
    pend.sort(proc(a, b: int): int = jobs[a].id - jobs[b].id)
    for idx in pend:
      if jobs[idx].isMasterJob: continue # masters aggregate; elements run
      if depsSatisfied(jobs[idx], jobs, maxId) != depSatisfied: continue
      if not dirExists(jobs[idx].chdir):
        jobs[idx].state = stFailed
        jobs[idx].endTime = nowStr
        continue
      # per-array concurrency cap (%N in the --array spec)
      if jobs[idx].arrayLimit > 0:
        var run = 0
        for k in jobs:
          if k.isElementJob and k.arrayId == jobs[idx].arrayId and
              k.state == stRunning:
            run += 1
        if run >= jobs[idx].arrayLimit: continue
      let need = jobs[idx].ntasks * jobs[idx].cpus
      if usedCpus + need <= cap:
        let pid = launch(jobs[idx], jobs, procs)
        jobs[idx].state = stRunning
        jobs[idx].start = nowStr
        jobs[idx].pid = pid
        usedCpus += need

    # Phase F: purge terminal rows past the retention window.
    var kept: seq[Job] = @[]
    for j in jobs:
      if j.isTerminal and j.endTime.len > 0 and
          (now - parseDbTime(j.endTime)).inSeconds > purgeSeconds:
        continue
      kept.add(j)
    jobs = kept

    saveJobs(f, jobs)
  finally:
    closeDb(f)

proc usage(): void =
  stderr.writeLine("Usage: vsched [--cpus N] [--once]")
  stderr.writeLine("  --cpus N   CPU budget (default: this machine's online core count).")
  stderr.writeLine("             N may exceed the real core count, to test concurrent")
  stderr.writeLine("             workflows on a machine with fewer cores.")
  quit(1)

proc main() =
  var procs: Table[int, Process] = initTable[int, Process]()
  var cpuCap = countCPUs()
  var once = false
  var i = 1
  while i <= paramCount():
    let a = paramStr(i)
    if a == "--once":
      once = true
    elif a == "--cpus" and i + 1 <= paramCount():
      inc i
      if paramStr(i).len > 0 and paramStr(i).allCharsInSet(Digits) and
          paramStr(i).parseInt > 0:
        cpuCap = paramStr(i).parseInt
      else:
        stderr.writeLine("vsched: error: invalid --cpus value '" & paramStr(i) & "'")
        quit(1)
    elif a.startsWith("--cpus="):
      let v = a[7 .. ^1]
      if v.len > 0 and v.allCharsInSet(Digits) and v.parseInt > 0:
        cpuCap = v.parseInt
      else:
        stderr.writeLine("vsched: error: invalid --cpus value '" & v & "'")
        quit(1)
    else:
      usage()
    inc i
  if once:
    tick(procs, cpuCap)
  else:
    while true:
      tick(procs, cpuCap)
      sleep(1000)

main()
