## slurmctld: the execution daemon. Once per second: lock DB, reap finished
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
  DepResult = enum depSatisfied, depWaiting, depUnresolvable, depFailed

proc countCPUs(): int =
  let n = posix.sysconf(posix.SC_NPROCESSORS_ONLN)
  if n > 0: n else: 1

proc log(msg: string) =
  ## One concise line per observable event (never per-tick chatter), so the
  ## terminal where slurmctld runs shows the daemon's lifecycle at a glance.
  stdout.writeLine("slurmctld: " & msg)
  stdout.flushFile()

proc describe*(j: Job): string =
  "job " & j.displayId & " (" & j.name & ")"

proc elapsedStr(j: Job; now: times.Time): string =
  if j.start.len > 0:
    " after " & $(now - parseDbTime(j.start)).inSeconds & "s"
  else:
    ""

proc pidAlive(pid: int): bool =
  ## kill(pid, 0) probes existence; ESRCH means gone, EPERM (not our child)
  ## still means the process exists.
  if pid <= 0: return false
  if posix.kill(Pid(pid), 0) == 0: return true
  result = posix.errno == posix.EPERM

proc buildEnv(j: Job; jobs: seq[Job]): StringTableRef =
  ## Each job's shell session is built from the submitter's snapshot
  ## (captured by sbatch/srun at submit time), not the daemon's env — so
  ## `FOO=1 sbatch job.sb` reaches the job. Ambient SLURM_*/SBATCH_* keys
  ## are dropped first: ours below are the only SLURM vars a job sees,
  ## even when the snapshot was taken from inside another job.
  result = newStringTable(modeCaseSensitive)
  for k, v in parseEnvSnapshot(j.envData).pairs:
    if k.startsWith("SLURM_") or k.startsWith("SBATCH_"): continue
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
  ## The exact command slurmctld executes: either the submitted script (run
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

proc depAtomState(j: Job; atom: DepAtom; jobs: seq[Job]; maxId: int;
                   now: times.Time): DepResult =
  ## Evaluate one atom `type:target[:target...]`. A dep on an array master id
  ## means "that array finished"; element deps use `master_task` refs.
  ## Terminology follows SLURM's test_job_dependency(): a dep atom is
  ## fulfilled, not fulfilled (waiting) or failed (can never be met).
  if atom.kind == "singleton":
    # Satisfied unless another same-name job is RUNNING or was submitted
    # earlier and is still PENDING (single user, no suspended state here).
    for k in jobs:
      if k.arrayId > 0 and k.arrayId == j.arrayId: continue
      if k.name != j.name: continue
      if k.state == stRunning or (k.state == stPending and k.id < j.id):
        return depWaiting
    return depSatisfied
  for t in atom.targets:
    let r = resolveDepRef(t.refStr, jobs)
    if not r.found:
      # Not in the DB: either finished long ago and was purged (satisfied,
      # since jobs.seq guarantees the ID was allocated) or never existed.
      if t.spec.job <= maxId: continue
      return depUnresolvable
    let s = r.job.state
    var ok = false
    case atom.kind
    of "afterok":
      if r.job.isTerminal and s != stCompleted: return depFailed
      ok = s == stCompleted
    of "afternotok":
      if r.job.isTerminal and s == stCompleted: return depFailed
      ok = r.job.isTerminal and s != stCompleted
    of "afterany": ok = r.job.isTerminal
    of "after":
      # Fires once the target has STARTED (or was cancelled); +minutes adds
      # a delay measured from that start/cancellation. Docs: "no time" = no
      # delay; `after` never fails on termination state.
      let startOrCancel = r.job.start.len > 0 or
        (r.job.isTerminal and r.job.state == stCancelled)
      if not startOrCancel: return depWaiting
      if t.delay >= 0:
        let base = if r.job.start.len > 0: parseDbTime(r.job.start)
          else: parseDbTime(r.job.endTime)
        if base.toUnix == 0: return depWaiting
        if (now - base).inSeconds < t.delay * 60: return depWaiting
      ok = true
    of "aftercorr":
      # Corresponding array task of the target must have completed with
      # exit 0; a non-array dependent or non-array target behaves as afterok
      # on the target as a whole (master = the whole array).
      if j.isArrayJob and j.isElementJob and t.spec.task < 0:
        # this job is array element M of its own master; the corresponding
        # task is the target array's element M
        let corr: tuple[job, task: int] = (t.spec.job, j.arrayTask)
        var found = false
        var corrJob: Job
        for k in jobs:
          if k.arrayId == corr.job and k.arrayTask == corr.task:
            found = true
            corrJob = k
            break
        if found:
          if corrJob.state == stCompleted: ok = true
          elif corrJob.isTerminal: return depFailed
          else: return depWaiting
        else:
          # target row purged: treat like any purged dep
          if t.spec.job <= maxId: ok = true else: return depUnresolvable
      else:
        if r.job.isTerminal and s != stCompleted: return depFailed
        ok = s == stCompleted
    of "afterburstbuffer":
      # No burst buffers exist here; stage-out is vacuously instant.
      ok = r.job.isTerminal
    else: ok = true # unknown type: satisfied (warned at submit)
    if not ok: return depWaiting
  depSatisfied

proc depsSatisfied(j: Job; jobs: seq[Job]; maxId: int; now: times.Time): DepResult =
  if j.dep.len == 0: return depSatisfied
  let d = parseDepDeps(j.dep)
  if d.atoms.len == 0: return depSatisfied
  if d.anyOf:
    # `?`: any fulfilled atom releases the job; an atom that failed (or
    # references an id that was never allocated) just drops out, and the
    # job is cancelled only when no atom can ever be met — SLURM's
    # or_flag/or_satisfied handling.
    result = depFailed
    for a in d.atoms:
      var r = depAtomState(j, a, jobs, maxId, now)
      if r == depUnresolvable: r = depFailed
      if r == depSatisfied: return depSatisfied
      if r == depWaiting: result = depWaiting
  else:
    result = depSatisfied
    for a in d.atoms:
      let r = depAtomState(j, a, jobs, maxId, now)
      if r == depUnresolvable: return depUnresolvable
      if r == depFailed: return depFailed
      if r == depWaiting: result = depWaiting

proc tick(procs: var Table[int, Process]; cpuCap: int;
           known: var Table[int, string]; seeded: var bool) =
  let db = dbPath()
  var f: File
  if not openDb(db, f):
    stderr.writeLine("slurmctld: cannot open " & db)
    quit(1)
  try:
    var jobs = loadJobs(f)
    let now = getTime()
    let nowStr = formatDbTime(now)
    let maxId = allocatedMaxId(db)

    # Phase 0: report state changes other tools made between ticks. slurmctld
    # holds the DB lock only while ticking, so sbatch/srun submissions and
    # scancel (or srun signal) cancellations land here, not in a phase below.
    # The first tick just seeds the snapshot: rows already in the DB predate
    # this server and get their own logs (adoption, launch) elsewhere.
    if not seeded:
      for j in jobs: known[j.id] = j.state
      seeded = true
    else:
      var newElems = initCountTable[int]()
      for j in jobs:
        if j.isElementJob and not known.hasKey(j.id): newElems.inc(j.arrayId)
      for j in jobs:
        if not known.hasKey(j.id):
          if j.isElementJob: continue # covered by the master's line
          if j.isMasterJob and newElems.hasKey(j.id):
            log("submitted " & describe(j) & ", array of " &
              $newElems[j.id] & " elements")
          else:
            log("submitted " & describe(j))
        elif known[j.id] != j.state and j.state == stCancelled:
          log(describe(j) & " cancelled by external request" &
            elapsedStr(j, now))

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
          log(describe(jobs[i]) & " " & jobs[i].state.toLowerAscii &
            " exit " & $ec & elapsedStr(jobs[i], now))
      else:
        # Orphan (server restarted): if the PID is gone we can only mark it
        # finished — the true exit code is unknowable.
        if not pidAlive(jobs[i].pid):
          jobs[i].state = stCompleted
          jobs[i].exitcode = -1
          jobs[i].hasExit = false
          jobs[i].endTime = nowStr
          log(describe(jobs[i]) & " completed (adopted orphan, exit code unknown)" &
            elapsedStr(jobs[i], now))

    # Phase B: escalate lingering terminal jobs still holding a process.
    for i in 0 ..< jobs.len:
      if not jobs[i].isTerminal: continue
      if not procs.hasKey(jobs[i].id): continue
      if jobs[i].endTime.len == 0: continue
      let age = (now - parseDbTime(jobs[i].endTime)).inSeconds
      if procs[jobs[i].id].running:
        if age > cancelGraceSeconds:
          procs[jobs[i].id].kill()
          log(describe(jobs[i]) & " still running " & $age &
            "s after " & jobs[i].state.toLowerAscii & "; sent SIGKILL")
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
        log(describe(jobs[i]) & " " & jobs[i].state.toLowerAscii &
          " (array master, all " & $elems.len & " elements finished)")

    # Phase C: resolve invalid dependencies, like slurmctld's
    # handle_invalid_dependency(). A dep on an ID that was never allocated
    # fails the job (real SLURM rejects it at submit); a dep that can never
    # be satisfied (afterok on a failed job, afternotok on a completed one)
    # cancels it.
    for i in 0 ..< jobs.len:
      if jobs[i].state != stPending: continue
      case depsSatisfied(jobs[i], jobs, maxId, now)
      of depUnresolvable:
        jobs[i].state = stFailed
        jobs[i].endTime = nowStr
        log(describe(jobs[i]) & " failed: dependency " & jobs[i].dep &
          " references an id that was never allocated")
      of depFailed:
        jobs[i].state = stCancelled
        jobs[i].endTime = nowStr
        log(describe(jobs[i]) & " cancelled: dependency " & jobs[i].dep &
          " can never be satisfied")
      else: discard

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
        log(describe(jobs[i]) & " timed out after " & $jobs[i].minutes &
          "m limit; sent SIGKILL")

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
      if depsSatisfied(jobs[idx], jobs, maxId, now) != depSatisfied: continue
      if not dirExists(jobs[idx].chdir):
        jobs[idx].state = stFailed
        jobs[idx].endTime = nowStr
        log(describe(jobs[idx]) & " failed: chdir " & jobs[idx].chdir &
          " no longer exists")
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
        log("launched " & describe(jobs[idx]) & " pid " & $pid &
          ", cpus " & $usedCpus & "/" & $cap)

    # Phase F: purge terminal rows past the retention window.
    var kept: seq[Job] = @[]
    var purged = 0
    for j in jobs:
      if j.isTerminal and j.endTime.len > 0 and
          (now - parseDbTime(j.endTime)).inSeconds > purgeSeconds:
        inc purged
        continue
      kept.add(j)
    if purged > 0:
      log("purged " & $purged & " finished job" & (if purged > 1: "s" else: "") &
        " older than " & $(purgeSeconds div 60) & "m from the DB")
    jobs = kept

    # Snapshot the states this tick wrote, so the next tick's Phase 0 only
    # reports transitions slurmctld did not make itself.
    known.clear()
    for j in jobs: known[j.id] = j.state

    saveJobs(f, jobs)
  finally:
    closeDb(f)

proc usage(): void =
  stderr.writeLine("Usage: slurmctld [--cpus N] [--once]")
  stderr.writeLine("  --cpus N   CPU budget (default: this machine's online core count).")
  stderr.writeLine("             N may exceed the real core count, to test concurrent")
  stderr.writeLine("             workflows on a machine with fewer cores.")
  quit(1)

proc main() =
  var procs: Table[int, Process] = initTable[int, Process]()
  var known: Table[int, string] = initTable[int, string]()
  var seeded = false
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
        stderr.writeLine("slurmctld: error: invalid --cpus value '" & paramStr(i) & "'")
        quit(1)
    elif a.startsWith("--cpus="):
      let v = a[7 .. ^1]
      if v.len > 0 and v.allCharsInSet(Digits) and v.parseInt > 0:
        cpuCap = v.parseInt
      else:
        stderr.writeLine("slurmctld: error: invalid --cpus value '" & v & "'")
        quit(1)
    else:
      usage()
    inc i
  log("scheduler started: cpu budget " & $cpuCap & ", db " & dbPath())
  if once:
    tick(procs, cpuCap, known, seeded)
  else:
    while true:
      tick(procs, cpuCap, known, seeded)
      sleep(1000)

main()
