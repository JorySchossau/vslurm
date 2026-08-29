## srun: submit a command line as a single job, wait for the scheduler to
## run it, stream its output/error to the terminal as the files grow, then
## exit with the job's exit code. SIGINT/SIGTERM cancel the job (the same DB
## transition scancel makes) and exit 1. Only slurmctld executes job code;
## srun just submits, watches and reports.

import os, strutils, times
import posix except Time
import vslurm_common except warn
import vslurm_diag

const shortMap = [('J', "job-name"), ('o', "output"), ('e', "error"),
  ('t', "time"), ('n', "ntasks"), ('c', "cpus-per-task"), ('D', "chdir"),
  ('d', "dependency"), ('v', "verbose"), ('l', "label"), ('u', "unbuffered"),
  ('N', "nodes"), ('p', "partition"), ('A', "account"), ('q', "qos"),
  ('m', "mem"), ('W', "wait")]

const valueOpts = ["job-name", "output", "error", "dependency", "depend", "ntasks",
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

## Input environment variables per the srun man page: command line
## always overrides the environment. srun's supported options are read;
## documented-but-unsupported ones warn like unsupported options. Keys srun
## itself doesn't document (PMI_*, etc.) are left alone entirely — they may
## mean something to the job's own MPI stack, so they must survive into
## the snapshot and must not warn.
const envOptPairs = @[
  ("SLURM_JOB_NAME", "job-name"),
  ("SRUN_OUTPUT", "output"),
  ("SRUN_ERROR", "error"),
  ("SLURM_DEPENDENCY", "dependency"),
  ("SLURM_TIMELIMIT", "time"),
  ("SLURM_NTASKS", "ntasks"),
  ("SLURM_NPROCS", "ntasks"),
  ("SLURM_CPUS_PER_TASK", "cpus-per-task"),
  ("SLURM_REMOTE_CWD", "chdir"),
  ("SLURM_EXPORT_ENV", "export"),
]

## every other option-equivalent variable the srun man page documents.
## SLURM_UMASK is honored by slurmctld at launch; SLURM_EXIT_ERROR is
## silently ignored.
const envUnsupported = ["SLURM_ACCOUNT", "SLURM_ACCTG_FREQ", "SLURM_BCAST",
  "SLURM_BCAST_EXCLUDE", "SLURM_BURST_BUFFER", "SLURM_CLUSTERS",
  "SLURM_COMPRESS", "SLURM_CONF", "SLURM_CONSTRAINT", "SLURM_CORE_SPEC",
  "SLURM_CPU_BIND", "SLURM_CPU_FREQ_REQ", "SLURM_CPUS_PER_GPU",
  "SLURM_DEBUG", "SLURM_DEBUG_FLAGS", "SLURM_DELAY_BOOT",
  "SLURM_DISABLE_STATUS", "SLURM_DIST_PLANESIZE", "SLURM_DISTRIBUTION",
  "SLURM_EPILOG", "SLURM_EXACT", "SLURM_EXCLUSIVE",
  "SLURM_EXIT_IMMEDIATE", "SLURM_GPU_BIND", "SLURM_GPU_FREQ",
  "SLURM_GPUS", "SLURM_GPUS_PER_NODE", "SLURM_GPUS_PER_TASK",
  "SLURM_GRES", "SLURM_GRES_FLAGS", "SLURM_HINT", "SLURM_IMMEDIATE",
  "SLURM_JOB_NUM_NODES", "SLURM_KILL_BAD_EXIT",
  "SLURM_LABELIO", "SLURM_MEM_BIND", "SLURM_MEM_PER_CPU",
  "SLURM_MEM_PER_GPU", "SLURM_MEM_PER_NODE", "SLURM_MPI_TYPE",
  "SLURM_NETWORK", "SLURM_NNODES", "SLURM_NO_KILL",
  "SLURM_NTASKS_PER_CORE", "SLURM_NTASKS_PER_GPU",
  "SLURM_NTASKS_PER_NODE", "SLURM_NTASKS_PER_SOCKET", "SLURM_OOMKILLSTEP",
  "SLURM_OPEN_MODE", "SLURM_OVERCOMMIT", "SLURM_OVERLAP",
  "SLURM_PARTITION", "SLURM_POWER", "SLURM_PROFILE", "SLURM_PROLOG",
  "SLURM_QOS", "SLURM_REQ_SWITCH", "SLURM_RESERVATION",
  "SLURM_RESV_PORTS", "SLURM_SEND_LIBS", "SLURM_SIGNAL",
  "SLURM_SPREAD_JOB", "SLURM_STEP_GRES", "SLURM_TASK_EPILOG",
  "SLURM_TASK_PROLOG", "SLURM_THREADS", "SLURM_THREAD_SPEC",
  "SLURM_THREADS_PER_CORE", "SLURM_TRES_BIND", "SLURM_TRES_PER_TASK",
  "SLURM_UNBUFFEREDIO", "SLURM_USE_MIN_NODES", "SLURM_WAIT",
  "SLURM_WCKEY", "SRUN_CONTAINER", "SRUN_CONTAINER_ID",
  "SRUN_CONTAINER_TYPE", "SRUN_EXPORT_ENV", "SRUN_INPUT",
  "SRUN_SEGMENT_SIZE"]

proc applyOption(opt, val: string; o: var Opts; warned: var seq[string]) =
  case opt
  of "job-name": o.name = val
  of "output": o.output = val
  of "error": o.errorFile = val
  of "chdir": o.chdir = val
  of "dependency", "depend":
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
  # environment first, command line after it — the man page's rule that
  # CLI settings override environment settings. Inside a running job
  # (SLURM_JOB_ID set in our own env) srun ignores the name/dependency
  # vars, per the man page's within-an-allocation exceptions — inheriting a
  # parent's dependency could deadlock a `singleton` child against its own
  # parent. SLURM_JOB_ID itself is silently dropped in that case too: there
  # are no allocations to attach to, and warning on every nested srun
  # would be noise.
  let insideJob = getEnv("SLURM_JOB_ID").len > 0
  for k, v in envPairs():
    if insideJob and (k == "SLURM_JOB_NAME" or k == "SLURM_DEPENDENCY" or
        k == "SLURM_JOB_ID"):
      continue
    var opt = ""
    for (ek, eopt) in envOptPairs:
      if ek == k:
        opt = eopt
        break
    if opt.len > 0:
      applyOption(opt, v, o, warned)
    elif k in envUnsupported:
      warnOnce("srun: warning: unsupported environment variable '" & k &
        "' ignored", warned)
  for c in scanned.calls:
    applyOption(c.opt, c.val, o, warned)
  drainPlain(warned)
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
  let envSnap = snapshotEnv()
  let id =
    try:
      let id = nextJobId(db)
      var warned2: seq[string] = @[]
      let outPath = expandSpec(o.output, id, id, -1, o.name, warned2)
      let errPath = expandSpec(o.errorFile, id, id, -1, o.name, warned2)
      drainPlain(warned2)
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
        envData: envSnap,
      )
      saveJobs(f, jobs)
      id
    finally:
      closeDb(f)

  quit(waitForJob(db, id))

main()
