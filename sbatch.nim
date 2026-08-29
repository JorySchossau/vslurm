## sbatch: parse CLI options and #SBATCH directives, insert PENDING rows
## into the jobs DB under lock. Never launches anything — slurmctld owns
## execution. One submit maps to one row, or to N rows for a job array
## (a master + N-1 elements) sharing the master's job id.
##
## All user-facing problems are reported as vslurm_diag diagnostics:
## spanned, explained, and accumulated so one submit shows every mistake
## at once instead of the first one only.

import os, strutils, times
import posix except Time
import vslurm_common except warn
import vslurm_diag

const shortMap = [('J', "job-name"), ('o', "output"), ('e', "error"),
  ('t', "time"), ('d', "dependency"), ('n', "ntasks"), ('c', "cpus-per-task"),
  ('D', "chdir"), ('a', "array"), ('A', "account")]

const valueOpts = ["job-name", "output", "error", "dependency", "depend", "ntasks",
  "cpus-per-task", "chdir", "wrap", "export", "time", "account", "partition",
  "qos", "nodes", "nodelist", "mem", "mem-per-cpu", "gres", "begin", "array",
  "cluster", "mail-user", "mail-type", "constraint", "reservation"]

## every long option sbatch recognizes — the did-you-mean candidate set
const knownOpts = @valueOpts & @["parsable", "exclusive", "share"]

const unsupportedOpts = ["account", "partition", "qos", "nodes", "nodelist",
  "mem", "mem-per-cpu", "gres", "exclusive", "share", "begin", "cluster",
  "mail-user", "mail-type", "constraint", "reservation"]

type
  Opts = object
    name: string
    nameSpan: Span
    namePath: string
    output: string
    outSpan: Span
    outPath2: string
    errorFile: string
    errSpan: Span
    errPath2: string
    dep: string
    chdir: string
    wrap: string
    wrapSpan: Span
    wrapPath: string
    minutes: int
    hasMinutes: bool
    ntasks: int
    cpus: int
    arraySpec: string
    arraySpan: Span
    arrayPath: string
    arrayRaw: string
    script: string
    args: seq[string]
    parsable: bool

  ## where an option came from: the script+span of a directive, or the CLI
  Origin = object
    path: string
    flag: Span
    val: Span

proc cliOrigin(): Origin = Origin(path: "", flag: noSpan(), val: noSpan())

proc loc(d: Diag; o: Origin; which: Span; label = ""): Diag =
  ## attach a script location when one exists; CLI messages stay bare
  if o.path.len > 0 and which.line > 0: d.with(o.path, which, label) else: d

proc newWarns(diags: var seq[Diag]; o: Origin; warned: var seq[string];
    mark: int; tool = "sbatch") =
  ## Convert messages that shared helpers (validateDep, expandArraySpec)
  ## appended since `mark` into spanned diagnostics; they arrive
  ## pre-formatted as "<tool>: warning: <text>".
  for m in warned[mark .. ^1]:
    let pfx = tool & ": warning: "
    let epfx = tool & ": error: "
    if m.startsWith(pfx):
      diags.add plain(sevWarning, tool, m[pfx.len .. ^1]).loc(o, o.val)
    elif m.startsWith(epfx):
      diags.add plain(sevError, tool, m[epfx.len .. ^1]).loc(o, o.val)
    else:
      diags.add plain(sevWarning, tool, m)
  warned.setLen(mark)

proc applyOption(opt, val: string; o: var Opts; diags: var seq[Diag];
    warned: var seq[string]; org: Origin; raw = "") =
  let dym = didYouMean(opt, knownOpts)
  case opt
  of "job-name":
    o.name = val
    o.nameSpan = org.val
    o.namePath = org.path
  of "output":
    o.output = val
    o.outSpan = org.val
    o.outPath2 = org.path
  of "error":
    o.errorFile = val
    o.errSpan = org.val
    o.errPath2 = org.path
  of "chdir": o.chdir = val
  of "wrap":
    o.wrap = val
    o.wrapSpan = org.val
    o.wrapPath = org.path
  of "dependency", "depend":
    let sepErr = depSeparatorError("sbatch", val)
    if sepErr.len > 0:
      # keep the phrase tests grep for ("cannot be mixed")
      let msg = sepErr.substr(len("sbatch: error: "))
      diags.add plain(sevError, "sbatch", msg).loc(org, org.val,
        "separators cannot be mixed").help(
        "groups are joined by ',' (all must finish) or '?' (any one), never both",
        "afterok:1,afterany:2")
      return
    let mark = warned.len
    o.dep = validateDep("sbatch", val, warned)
    newWarns(diags, org, warned, mark)
  of "array":
    o.arraySpec = val
    o.arraySpan = org.val
    o.arrayPath = org.path
    o.arrayRaw = raw
  of "parsable":
    o.parsable = true
  of "time":
    let m = parseTimeSpec(val)
    if m >= 0:
      o.minutes = m
      o.hasMinutes = true
    else:
      diags.add plain(sevWarning, "sbatch",
        "invalid time limit '" & val & "' ignored").loc(org, org.val,
        "not a valid time").note(
        "a plain number is minutes; colons are HH:MM:SS or MM:SS").help(
        "try '-t 5' for 5 minutes, '-t 2:00:00' for 2 hours",
        if org.path.len > 0 and raw.len > 0:
          "#SBATCH --time=" & (if val.len > 0 and val.allCharsInSet(Digits): val else: "5")
        else: "")
  of "ntasks", "cpus-per-task":
    if val.len > 0 and val.allCharsInSet(Digits):
      if opt == "ntasks": o.ntasks = val.parseInt else: o.cpus = val.parseInt
    else:
      diags.add plain(sevWarning, "sbatch",
        "invalid value '" & val & "' for '--" & opt & "' ignored").loc(org,
        org.val, "expected a whole number")
  of "export":
    if val != "ALL":
      diags.add plain(sevWarning, "sbatch",
        "unsupported option '--export' ignored").loc(org, org.flag).note(
        "only 'ALL' is meaningful here; the environment is always inherited")
  of unsupportedOpts:
    diags.add plain(sevWarning, "sbatch",
      "unsupported option '" & displayOpt(opt) & "' ignored").loc(org,
      org.flag, "no effect in vslurm").note(
      "accepted for compatibility with real sbatch scripts")
  else:
    var d = plain(sevWarning, "sbatch",
      "unknown option '" & displayOpt(opt) & "' ignored").loc(org, org.flag,
      "unknown option")
    if dym.len > 0:
      d = d.help("did you mean '--" & dym & "'?",
        if org.path.len > 0 and raw.len > 0:
          let eq = raw.find('=')
          if eq > 0: "#SBATCH --" & dym & raw[eq .. ^1]
          elif raw.len >= 2 and raw[0] == '-' and raw[1] != '-':
            "#SBATCH --" & dym & (if val.len > 0: "=" & val else: "")
          else: "#SBATCH --" & dym
        else: "")
    diags.add d

## Input environment variables, per real sbatch's precedence
## directives < environment < command line. Supported keys route through
## the same applyOption as directives (so invalid values warn the same
## way); documented-but-unsupported ones warn like unsupported options.
## Unrecognized SBATCH_* keys stay silent, like real sbatch.
const envOptPairs = @[
  ("SBATCH_JOB_NAME", "job-name"),
  ("SBATCH_OUTPUT", "output"),
  ("SBATCH_ERROR", "error"),
  ("SBATCH_TIMELIMIT", "time"),
  ("SBATCH_ARRAY_INX", "array"),
  ("SBATCH_EXPORT", "export"),
]

## every other input variable the sbatch man page documents, none of
## which map to an option vslurm implements (SLURM_UMASK is honored by
## slurmctld at launch instead; SLURM_EXIT_ERROR is silently ignored)
const envUnsupported = ["SBATCH_ACCOUNT", "SBATCH_ACCTG_FREQ", "SBATCH_BATCH",
  "SBATCH_CLUSTERS", "SLURM_CLUSTERS", "SBATCH_CONSTRAINT",
  "SBATCH_CONTAINER", "SBATCH_CONTAINER_ID", "SBATCH_CONTAINER_TYPE",
  "SBATCH_CORE_SPEC", "SBATCH_CPUS_PER_GPU", "SBATCH_DEBUG",
  "SBATCH_DELAY_BOOT", "SBATCH_DISTRIBUTION", "SBATCH_EXCLUSIVE",
  "SBATCH_GET_USER_ENV", "SBATCH_GPU_BIND", "SBATCH_GPU_FREQ",
  "SBATCH_GPUS", "SBATCH_GPUS_PER_NODE", "SBATCH_GPUS_PER_TASK",
  "SBATCH_GRES", "SBATCH_GRES_FLAGS", "SBATCH_HINT", "SLURM_HINT",
  "SBATCH_IGNORE_PBS", "SBATCH_INPUT", "SBATCH_MEM_BIND",
  "SBATCH_MEM_PER_CPU", "SBATCH_MEM_PER_GPU", "SBATCH_MEM_PER_NODE",
  "SBATCH_NETWORK", "SBATCH_NO_KILL", "SBATCH_NO_REQUEUE",
  "SBATCH_OPEN_MODE", "SBATCH_OVERCOMMIT", "SBATCH_PARTITION",
  "SBATCH_POWER", "SBATCH_PROFILE", "SBATCH_QOS", "SBATCH_REQ_SWITCH",
  "SBATCH_REQUEUE", "SBATCH_RESERVATION", "SBATCH_SEGMENT_SIZE",
  "SBATCH_SIGNAL", "SBATCH_SPREAD_JOB", "SBATCH_THREAD_SPEC",
  "SBATCH_THREADS_PER_CORE", "SBATCH_TRES_BIND", "SBATCH_TRES_PER_TASK",
  "SBATCH_USE_MIN_NODES", "SBATCH_WAIT", "SBATCH_WAIT4SWITCH",
  "SBATCH_WAIT_ALL_NODES", "SBATCH_WCKEY", "SLURM_CONF",
  "SLURM_DEBUG_FLAGS", "SLURM_STEP_KILLED_MSG_NODE_ID"]

proc applyEnvOptions(o: var Opts; diags: var seq[Diag]; warned: var seq[string]) =
  for k, v in envPairs():
    var opt = ""
    for (ek, eopt) in envOptPairs:
      if ek == k:
        opt = eopt
        break
    if opt.len > 0:
      applyOption(opt, v, o, diags, warned, cliOrigin())
    elif k in envUnsupported:
      diags.add plain(sevWarning, "sbatch",
        "unsupported environment variable '" & k & "' ignored").note(
        "it stands for an option vslurm does not implement")

proc parseDirectives(path: string; o: var Opts; diags: var seq[Diag];
    warned: var seq[string]) =
  ## Apply #SBATCH directives from the top of the script until the first line
  ## that is neither blank nor #-prefixed — and point at directives the user
  ## probably meant to take effect but that real sbatch would silently skip.
  var f: File
  if not f.open(path): return
  defer: f.close()
  var lineNo = 0
  # line of the first command; #SBATCH lines below it are never read
  var headerDone = -1
  for line in f.lines:
    inc lineNo
    let s = line.strip()
    if s.len == 0: continue
    if not s.startsWith("#"):
      if headerDone < 0: headerDone = lineNo
      continue
    if s.len > 7 and s[0 .. 6] == "#SBATCH" and s[7] in {' ', '\t'}:
      let rest = s[8 .. ^1].strip()
      if rest.len == 0: continue
      if headerDone > 0 and lineNo > headerDone:
        # a directive below the first command: real sbatch never reads
        # these, and neither do we — say so instead of staying silent
        let col = line.find("#SBATCH") + 1
        diags.add plain(sevWarning, "sbatch",
          "this directive is ignored").with(path, at(lineNo, col, 7),
          "after the script starts").note(
          "sbatch reads #SBATCH lines only before the first command (line " &
          $headerDone & ")").help("move it above the first non-comment line")
        continue
      let base = line.find(rest[0])
      let scanned = scanDirective(spannedWords(rest, base), lineNo,
        shortMap, valueOpts)
      for p in scanned.positionals:
        diags.add plain(sevWarning, "sbatch",
          "unexpected argument '" & p.tok & "' in directive").with(path,
          at(lineNo, p.col, p.rawLen), "not an option").note(
          "#SBATCH lines take options only, like the sbatch command line")
      for c in scanned.calls:
        let org = Origin(path: path, flag: c.optSpan, val: c.valSpan)
        if not c.hasVal and c.opt in valueOpts and c.opt != "parsable":
          diags.add plain(sevWarning, "sbatch",
            "option '--" & c.opt & "' needs a value and was ignored").with(
            path, c.optSpan, "value missing").help(
            "write the value inline or as the next word",
            "#SBATCH --" & c.opt & "=VALUE")
          continue
        applyOption(c.opt, c.val, o, diags, warned, org, c.raw)
    elif s.len > 2 and didYouMean(s.strip(chars = {'#', ' ', '\t'}), ["SBATCH"]).len > 0:
      # e.g. `#sbatch` / `#SBATCH--time=5`: looks like a directive but the
      # spelling is off, so sbatch (and vslurm) silently treat it as a comment
      let word = s.strip(chars = {'#', ' ', '\t'})
      let col = line.find(word[0]) + 1
      diags.add plain(sevWarning, "sbatch",
        "line looks like a directive but is treated as a comment").with(path,
        at(lineNo, col, word.len), "not '#SBATCH'").note(
        "the directive prefix is case-sensitive and needs a space").help(
        "spell it exactly '#SBATCH'",
        "#SBATCH" & s[word.len + 1 .. ^1] & "  # <- was: " & s)

proc specSpan(patVal, msg: string; base: Span): Span =
  ## Locate the exact `%X` (or `%3X`) a specifier warning complains about,
  ## so the carets underline the specifier itself, not the whole pattern.
  let q0 = msg.find("'%")
  if q0 < 0 or base.line == 0: return base
  let letter = msg[q0 + 2]
  var i = 0
  while i < patVal.len:
    if patVal[i] == '%':
      var k = i + 1
      var w = 0
      while k < patVal.len and patVal[k] in {'0'..'9'} and w < 3:
        inc k
        inc w
      if k < patVal.len and patVal[k] == letter:
        return at(base.line, base.col + i, k - i + 1)
      i = k
    else:
      inc i
  base

proc drainSpecWarns(warned: var seq[string]; o: Opts) =
  ## Filename-pattern warnings, pointed at the field that produced them.
  ## Warnings were collected name -> output -> error, in that order.
  var fields: seq[tuple[val: string, sp: Span, pp: string]] = @[]
  fields.add (o.name, o.nameSpan, o.namePath)
  fields.add (o.output, o.outSpan, o.outPath2)
  fields.add (o.errorFile, o.errSpan, o.errPath2)
  var fi = 0
  for m in warned:
    let pfx = "sbatch: warning: "
    let body = if m.startsWith(pfx): m[pfx.len .. ^1] else: m
    var d = plain(sevWarning, "sbatch", body)
    if fi < fields.len and "format specifier" in m:
      let f = fields[fi]
      inc fi
      if f.sp.line > 0 and f.pp.len > 0:
        d = d.with(f.pp, specSpan(f.val, m, f.sp),
          "unsupported here").note(
          "recognized: %j %A %a %x %t %N %u %J %%, optional width like %3j")
    emit(d)
  warned.setLen(0)

proc emitAll(diags: seq[Diag]): bool =
  ## print every diagnostic in collection order; true if any was fatal
  var fatal = false
  for d in diags:
    emit(d)
    if d.sev == sevError: fatal = true
  fatal

proc main() =
  var o = Opts(ntasks: 1, cpus: 1)
  var warned: seq[string] = @[]
  var diags: seq[Diag] = @[]
  let scanned = scanArgs(commandLineParams(), shortMap, valueOpts, false)
  let positionals = scanned.positionals

  if positionals.len > 0:
    o.script = positionals[0]
    if positionals.len > 1: o.args = positionals[1 .. ^1]

  if o.script.len > 0 and not fileExists(o.script):
    emit(plain(sevError, "sbatch", o.script & ": No such file or directory"))
    quit(1)

  # directives, then the environment, then the command line — each layer
  # overrides the last, real sbatch's documented precedence
  if o.script.len > 0:
    parseDirectives(o.script, o, diags, warned)
  applyEnvOptions(o, diags, warned)
  for c in scanned.calls:
    applyOption(c.opt, c.val, o, diags, warned, cliOrigin())

  if o.wrap.len > 0 and o.script.len > 0:
    var d = plain(sevError, "sbatch", "--wrap is incompatible with a script")
    if o.wrapPath.len > 0:
      d = d.with(o.wrapPath, o.wrapSpan, "--wrap given here").note(
        "the script is " & o.script)
    diags.add d
  if emitAll(diags):
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
    let mark = warned.len
    let ex = expandArraySpec(o.arraySpec, warned)
    tasks = ex.ids
    arrayLimit = ex.limit
    # expandArraySpec's messages arrive pre-formatted; re-render them
    # pointing at the value, fatal when no index survived
    # short flags stay short (`-a=1-10` is wrong); only long ones take '='
    let eq = if o.arrayRaw.startsWith("--"): "=" else: " "
    for m in warned[mark .. ^1]:
      let pfx = "sbatch: error: "
      var d = plain(if tasks.len == 0: sevError else: sevWarning, "sbatch",
        if m.startsWith(pfx): m[pfx.len .. ^1] else: m)
      if o.arrayPath.len > 0 and o.arraySpan.line > 0:
        d = d.with(o.arrayPath, o.arraySpan, "in --array")
      var fix = ""
      if o.arrayPath.len > 0:
        fix = "#SBATCH " & (if o.arrayRaw.len > 0 and
          o.arrayRaw.startsWith("-"): o.arrayRaw else: "--array") & eq & "1-10"
      elif o.arrayRaw.startsWith("--"):
        fix = o.arrayRaw & "=1-10"
      d = d.help("ranges 1-10, lists 1,3,7, optional %N concurrency limit", fix)
      diags.add d
    warned.setLen(mark)
    discard emitAll(diags)
    diags.setLen(0)
    if tasks.len == 0: quit(1)
    arrayed = true

  let db = dbPath()
  var f: File
  if not openDb(db, f):
    stderr.writeLine("sbatch: error: cannot open " & db)
    quit(1)
  let argsStr = shellJoin(o.args)
  let envSnap = snapshotEnv()
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
        envData: envSnap,
      )
    saveJobs(f, jobs)
    drainSpecWarns(warned, o)
    if o.parsable: echo masterId else: echo "Submitted batch job ", masterId
  finally:
    closeDb(f)

main()
