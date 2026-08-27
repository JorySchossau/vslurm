## srun: submit a command line as a single job, wait for the scheduler to
## run it, stream its output/error to the terminal as the files grow, then
## exit with the job's exit code. SIGINT/SIGTERM cancel the job (the same DB
## transition scancel makes) and exit 1. Only slurmctld executes job code;
## srun just submits, watches and reports.

import os, strutils, times
import posix except Time
import vslurm_common except warn

const shortMap = [('J', "job-name"), ('o', "output"), ('e', "error"),
  ('t', "time"), ('n', "ntasks"), ('c', "cpus-per-task"), ('D', "chdir"),
  ('d', "depend"), ('v', "verbose"), ('l', "label"), ('u', "unbuffered"),
  ('N', "nodes"), ('p', "partition"), ('A', "account"), ('q', "qos"),
  ('m', "mem"), ('W', "wait")]

const valueOpts = ["job-name", "output", "error", "depend", "ntasks",
  "cpus-per-task", "chdir", "time", "export", "wait", "account", "partition",
  "qos", "nodes", "nodelist", "mem", "mem-per-cpu", "gres", "begin",
  "cluster", "mail-user", "mail-type", "constraint", "reservation",
  "cpu-bind", "mem-bind", "hint", "mpi"]

type
  Opts = object
    name: string
    output: string
    errorFile: string
    dep: string
    chdir: string
    minutes: int
    hasMinutes: bool
    ntasks: int
    cpus: int

proc applyOption(opt, val: string; o: var Opts; warned: var seq[string]) =
  case opt
  of "job-name": o.name = val
  of "output": o.output = val
  of "error": o.errorFile = val
  of "chdir": o.chdir = val
  of "depend":
    let sepErr = depSeparatorError("srun", val)
    if sepErr.len > 0:
      stderr.writeLine(sepErr)
      quit(1)
    o.dep = validateDep("srun", val, warned)
  of "time":
    let m = parseTimeSpec(val)
    if m >= 0:
      o.minutes = m
      o.hasMinutes = true
    else:
      warnOnce("srun: warning: invalid time limit '" & val & "' ignored", warned)
  of "ntasks", "cpus-per-task":
    if val.len > 0 and val.allCharsInSet(Digits):
      if opt == "ntasks": o.ntasks = val.parseInt else: o.cpus = val.parseInt
    else:
      warnOnce("srun: warning: invalid value '" & val & "' for '--" & opt &
        "' ignored", warned)
  of "export":
    if val != "ALL":
      warnOnce("srun: warning: unsupported option '--export' ignored", warned)
  of "verbose":
    discard
  of "account", "partition", "qos", "nodes", "nodelist", "mem", "mem-per-cpu",
     "gres", "exclusive", "share", "begin", "cluster", "mail-user",
     "mail-type", "constraint", "reservation", "cpu-bind", "mem-bind",
     "hint", "mpi", "label", "unbuffered", "wait":
    warnOnce("srun: warning: unsupported option '" & displayOpt(opt) & "' ignored", warned)
  else:
    warnOnce("srun: warning: unknown option '" & displayOpt(opt) & "' ignored", warned)

var cancelRequested = false

proc onSignal(sig: cint) {.noconv.} =
  discard sig
  cancelRequested = true

proc streamFile(path: string; stream: File; offset: var int) =
  ## Append any bytes written to `path` since the last call to `stream`.
  var f: File
  if not f.open(path): return
  defer: f.close()
  let size = getFileSize(path)
  if size < offset: offset = 0 # file was recreated
  if size == offset: return
  f.setFilePos(offset)
  let data = f.readAll()
  if data.len > 0:
    stream.write(data)
    stream.flushFile()
    offset += data.len

proc cancelJob(db: string; id: int) =
  var f: File
  if not openDb(db, f): return
  try:
    var jobs = loadJobs(f)
    for i in 0 ..< jobs.len:
      if jobs[i].id != id: continue
      if jobs[i].state == stPending or jobs[i].state == stRunning:
        if jobs[i].state == stRunning and jobs[i].pid > 0 and
            posix.kill(Pid(jobs[i].pid), 0) == 0:
          discard posix.kill(Pid(jobs[i].pid), SIGTERM)
        jobs[i].state = stCancelled
        jobs[i].endTime = formatDbTime(getTime())
    saveJobs(f, jobs)
  finally:
    closeDb(f)

proc findJob(db: string; id: int): tuple[found: bool, job: Job] =
  for k in readJobs(db):
    if k.id == id:
      return (true, k)
  (false, Job())

proc finish(j: Job): int =
  case j.state
  of stCompleted:
    if j.hasExit: j.exitcode else: 0
  of stFailed:
    if j.hasExit: j.exitcode else: 1
  of stTimeout:
    stderr.writeLine("srun: error: job " & $j.id & ": TIMEOUT")
    1
  else: # cancelled, or anything unexpected
    stderr.writeLine("srun: job " & $j.id & " cancelled")
    1

proc waitForJob(db: string; id: int): int =
  var outOff = 0
  var errOff = 0
  var pendTicks = 0
  var announced = false
  var outPath = ""
  var errPath = ""
  while true:
    if cancelRequested:
      let r = findJob(db, id)
      if r.found and r.job.isTerminal:
        quit(finish(r.job))
      cancelJob(db, id)
      stderr.writeLine("srun: job " & $id & " cancelled")
      quit(1)
    let r = findJob(db, id)
    if not r.found:
      stderr.writeLine("srun: error: job " & $id & " vanished from " & db)
      quit(1)
    let j = r.job
    if outPath.len == 0 and j.output.len > 0:
      outPath = resolvePath(j.chdir, j.output)
      if j.errorFile.len > 0: errPath = resolvePath(j.chdir, j.errorFile)
    streamFile(outPath, stdout, outOff)
    streamFile(errPath, stderr, errOff)
    # announce only once the wait is noticeable (~1 s), like real srun
    if j.state == stPending:
      inc pendTicks
      if pendTicks >= 5 and not announced:
        stderr.writeLine("srun: job " & $id & " queued and waiting for resources")
        announced = true
    else:
      pendTicks = 0
    if j.isTerminal:
      quit(finish(j))
    sleep(200)

proc main() =
  var o = Opts(ntasks: 1, cpus: 1)
  var warned: seq[string] = @[]
  let scanned = scanArgs(commandLineParams(), shortMap, valueOpts, true)
  for c in scanned.calls:
    applyOption(c.opt, c.val, o, warned)
  let cmd = scanned.positionals
  if cmd.len == 0:
    stderr.writeLine("Usage: srun [options] <command> [args...]")
    quit(1)

  if o.name.len == 0: o.name = extractFilename(cmd[0])
  if o.output.len == 0: o.output = "slurm-%j.out"
  if o.chdir.len == 0: o.chdir = getCurrentDir()
  if not o.chdir.isAbsolute: o.chdir = getCurrentDir() / o.chdir

  discard posix.signal(SIGINT, onSignal)
  discard posix.signal(SIGTERM, onSignal)

  let db = dbPath()
  var f: File
  if not openDb(db, f):
    stderr.writeLine("srun: error: cannot open " & db)
    quit(1)
  let id =
    try:
      let id = nextJobId(db)
      var warned2 = warned
      let outPath = expandSpec(o.output, id, id, -1, o.name, warned2)
      let errPath = expandSpec(o.errorFile, id, id, -1, o.name, warned2)
      # Empty the files up front (slurmctld re-truncates with `>` at launch) so
      # streaming can start at offset 0 with no stale bytes from a wiped DB.
      for p in [resolvePath(o.chdir, outPath), resolvePath(o.chdir, errPath)]:
        if p.len > 0:
          var tf: File
          if tf.open(p, fmWrite): tf.close()
      var jobs = loadJobs(f)
      jobs.add Job(
        id: id,
        state: stPending,
        name: o.name,
        submit: formatDbTime(getTime()),
        pid: -1,
        exitcode: -1,
        minutes: o.minutes,
        hasMinutes: o.hasMinutes,
        ntasks: o.ntasks,
        cpus: o.cpus,
        dep: o.dep,
        output: outPath,
        errorFile: errPath,
        chdir: o.chdir,
        wrap: shellJoin(cmd),
        arrayId: -1,
        arrayTask: -1,
        arrayLimit: -1,
      )
      saveJobs(f, jobs)
      id
    finally:
      closeDb(f)

  quit(waitForJob(db, id))

main()
