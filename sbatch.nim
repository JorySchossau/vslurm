## sbatch: parse CLI options and #SBATCH directives, insert PENDING rows
## into the jobs DB under lock. Never launches anything — slurmctld owns
## execution. One submit maps to one row, or to N rows for a job array
## (a master + N-1 elements) sharing the master's job id.

import os, strutils, times
import posix except Time
import vslurm_common except warn

const shortMap = [('J', "job-name"), ('o', "output"), ('e', "error"),
  ('t', "time"), ('d', "dependency"), ('n', "ntasks"), ('c', "cpus-per-task"),
  ('D', "chdir"), ('a', "array"), ('A', "account")]

const valueOpts = ["job-name", "output", "error", "dependency", "depend", "ntasks",
  "cpus-per-task", "chdir", "wrap", "export", "time", "account", "partition",
  "qos", "nodes", "nodelist", "mem", "mem-per-cpu", "gres", "begin", "array",
  "cluster", "mail-user", "mail-type", "constraint", "reservation"]

type
  Opts = object
    name: string
    output: string
    errorFile: string
    dep: string
    chdir: string
    wrap: string
    minutes: int
    hasMinutes: bool
    ntasks: int
    cpus: int
    arraySpec: string
    script: string
    args: seq[string]
    parsable: bool

proc applyOption(opt, val: string; o: var Opts; warned: var seq[string]) =
  case opt
  of "job-name":
    o.name = val
  of "output": o.output = val
  of "error": o.errorFile = val
  of "chdir": o.chdir = val
  of "wrap": o.wrap = val
  of "dependency", "depend":
    let sepErr = depSeparatorError("sbatch", val)
    if sepErr.len > 0:
      stderr.writeLine(sepErr)
      quit(1)
    o.dep = validateDep("sbatch", val, warned)
  of "array":
    o.arraySpec = val
  of "parsable":
    o.parsable = true
  of "time":
    let m = parseTimeSpec(val)
    if m >= 0:
      o.minutes = m
      o.hasMinutes = true
    else:
      warnOnce("sbatch: warning: invalid time limit '" & val & "' ignored", warned)
  of "ntasks", "cpus-per-task":
    if val.len > 0 and val.allCharsInSet(Digits):
      if opt == "ntasks": o.ntasks = val.parseInt else: o.cpus = val.parseInt
    else:
      warnOnce("sbatch: warning: invalid value '" & val & "' for '--" & opt &
        "' ignored", warned)
  of "export":
    if val != "ALL":
      warnOnce("sbatch: warning: unsupported option '--export' ignored", warned)
  of "account", "partition", "qos", "nodes", "nodelist", "mem", "mem-per-cpu",
     "gres", "exclusive", "share", "begin", "cluster", "mail-user",
     "mail-type", "constraint", "reservation":
    warnOnce("sbatch: warning: unsupported option '" & displayOpt(opt) & "' ignored", warned)
  else:
    warnOnce("sbatch: warning: unknown option '" & displayOpt(opt) & "' ignored", warned)

proc parseDirectives(path: string; o: var Opts; warned: var seq[string]) =
  ## Apply #SBATCH directives from the top of the script until the first line
  ## that is neither blank nor #-prefixed.
  var f: File
  if not f.open(path): return
  defer: f.close()
  for line in f.lines:
    let s = line.strip()
    if s.len == 0: continue
    if not s.startsWith("#"): break
    if s.len > 7 and s[0 .. 6] == "#SBATCH" and s[7] in {' ', '\t'}:
      let rest = s[8 .. ^1].strip()
      if rest.len == 0: continue
      let scanned = scanArgs(shellWords(rest), shortMap, valueOpts, false)
      for c in scanned.calls:
        applyOption(c.opt, c.val, o, warned)

proc main() =
  var o = Opts(ntasks: 1, cpus: 1)
  var warned: seq[string] = @[]
  let scanned = scanArgs(commandLineParams(), shortMap, valueOpts, false)
  let positionals = scanned.positionals

  if positionals.len > 0:
    o.script = positionals[0]
    if positionals.len > 1: o.args = positionals[1 .. ^1]

  if o.script.len > 0 and not fileExists(o.script):
    stderr.writeLine("sbatch: error: " & o.script & ": No such file or directory")
    quit(1)

  # directives first, then the command line, which wins per real sbatch
  if o.script.len > 0:
    parseDirectives(o.script, o, warned)
  for c in scanned.calls:
    applyOption(c.opt, c.val, o, warned)

  if o.wrap.len > 0 and o.script.len > 0:
    stderr.writeLine("sbatch: error: --wrap is incompatible with a script")
    quit(1)
  if o.script.len == 0 and o.wrap.len == 0:
    stderr.writeLine("Usage: sbatch [options] <script> [args...] | sbatch --wrap <command>")
    quit(1)

  if o.name.len == 0:
    o.name = if o.script.len > 0: extractFilename(o.script) else: "wrap"
  if o.output.len == 0:
    o.output = if o.arraySpec.len > 0: "slurm-%A_%a.out" else: "slurm-%j.out"
  if o.chdir.len == 0: o.chdir = getCurrentDir()
  if not o.chdir.isAbsolute: o.chdir = getCurrentDir() / o.chdir
  # store absolute paths so slurmctld can launch from any cwd; directives were
  # already parsed from the same file, only the recorded path changes
  if o.script.len > 0 and not o.script.isAbsolute:
    o.script = getCurrentDir() / o.script

  var tasks: seq[int] = @[]
  var arrayLimit = -1
  var arrayed = false
  if o.arraySpec.len > 0:
    let ex = expandArraySpec(o.arraySpec, warned)
    tasks = ex.ids
    arrayLimit = ex.limit
    if tasks.len == 0:
      stderr.writeLine("sbatch: error: invalid --array value '" & o.arraySpec & "'")
      quit(1)
    arrayed = true

  let db = dbPath()
  var f: File
  if not openDb(db, f):
    stderr.writeLine("sbatch: error: cannot open " & db)
    quit(1)
  let argsStr = shellJoin(o.args)
  try:
    let masterId = nextJobId(db)
    var jobs = loadJobs(f)
    # row 0 is the plain job itself, or the non-executing array master
    for i in 0 ..< (if arrayed: tasks.len + 1 else: 1):
      let isMaster = arrayed and i == 0
      let id = if i == 0: masterId else: nextJobId(db)
      let task = if isMaster: -1 elif arrayed: tasks[i - 1] else: -1
      # every element gets its own row id; %A and %a key off the master
      let name = expandSpec(o.name, id, masterId, task, o.name, warned)
      let outPath = expandSpec(o.output, id, masterId, task, name, warned)
      let errPath = expandSpec(o.errorFile, id, masterId, task, name, warned)
      jobs.add Job(
        id: id,
        state: stPending,
        name: name,
        submit: formatDbTime(getTime()),
        start: "",
        endTime: "",
        pid: -1,
        exitcode: -1,
        hasExit: false,
        minutes: o.minutes,
        hasMinutes: o.hasMinutes,
        ntasks: o.ntasks,
        cpus: o.cpus,
        dep: o.dep,
        output: outPath,
        errorFile: errPath,
        chdir: o.chdir,
        script: o.script,
        args: argsStr,
        wrap: o.wrap,
        arrayId: if arrayed: masterId else: -1,
        arrayTask: task,
        arrayLimit: if arrayed: arrayLimit else: -1,
      )
    saveJobs(f, jobs)
    if o.parsable: echo masterId else: echo "Submitted batch job ", masterId
  finally:
    closeDb(f)

main()
