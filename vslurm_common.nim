## Shared contract for all vslurm tools: the Job type, the jobs.csv schema
## (CSV quoting included), lockf-based locking, in-place DB rewrite and
## sequence-based job-ID allocation. Plus small shared helpers (shell word
## splitting, filename-pattern expansion) used by several tools.

import os, sequtils, strutils, times
import posix except Time

const dbHeader* =
  "jobid,state,name,submit,start,end,pid,exitcode,minutes,ntasks,cpus,dep,output,error,chdir,script,args,wrap,arrayid,arraytask,arraylimit"

const dbColumns* = 21

const
  stPending* = "PENDING"
  stRunning* = "RUNNING"
  stCompleted* = "COMPLETED"
  stFailed* = "FAILED"
  stCancelled* = "CANCELLED"
  stTimeout* = "TIMEOUT"

proc stateAbbr*(s: string): string =
  case s
  of stPending: "PD"
  of stRunning: "R"
  of stCompleted: "CD"
  of stFailed: "F"
  of stCancelled: "CA"
  of stTimeout: "TO"
  else: s

type
  Job* = object
    id*: int
    state*: string
    name*: string
    submit*: string
    start*: string
    endTime*: string
    pid*: int
    exitcode*: int
    hasExit*: bool
    minutes*: int
    hasMinutes*: bool
    ntasks*: int
    cpus*: int
    dep*: string
    output*: string
    errorFile*: string
    chdir*: string
    script*: string
    args*: string
    wrap*: string
    arrayId*: int
    arrayTask*: int
    arrayLimit*: int

proc isArrayJob*(j: Job): bool = j.arrayId > 0
## A job array is one non-executing master row (arrayTask -1) plus one row
## per index; masters aggregate, elements execute.
proc isMasterJob*(j: Job): bool = j.arrayId > 0 and j.arrayTask < 0
proc isElementJob*(j: Job): bool = j.arrayId > 0 and j.arrayTask >= 0

proc isTerminal*(j: Job): bool =
  j.state == stCompleted or j.state == stFailed or
    j.state == stCancelled or j.state == stTimeout

proc displayId*(j: Job): string =
  ## squeue-style display id: `<masterid>_<task>` for array elements,
  ## plain id otherwise (matches SLURM's `100_5` convention).
  if j.isElementJob: $j.arrayId & "_" & $j.arrayTask else: $j.id

proc csvEscape*(s: string): string =
  if s.len == 0: return s
  for c in s:
    if c == ',' or c == '"' or c == '\n' or c == '\r':
      return '"' & s.replace("\"", "\"\"") & '"'
  s

proc csvSplit*(line: string): seq[string] =
  result = @[]
  var cell = ""
  var i = 0
  var inQuotes = false
  # a cell is quoted iff it starts with '"'; "" inside quotes is one '"'
  while i < line.len:
    let c = line[i]
    if inQuotes:
      if c == '"':
        if i + 1 < line.len and line[i + 1] == '"':
          cell.add '"'
          inc i
        else:
          inQuotes = false
      else:
        cell.add c
    elif c == '"' and cell.len == 0:
      inQuotes = true
    elif c == ',':
      result.add cell
      cell = ""
    else:
      cell.add c
    inc i
  result.add cell

proc toRow*(j: Job): string =
  # -1 int cells serialize back to empty so load→save round-trips exactly
  proc s(i: int): string =
    if i == -1: "" else: $i

  result = csvEscape($j.id) & "," & csvEscape(j.state) & "," & csvEscape(j.name) & "," &
    csvEscape(j.submit) & "," & csvEscape(j.start) & "," & csvEscape(j.endTime) & "," &
    csvEscape(s(j.pid)) & "," & (if j.hasExit: csvEscape($j.exitcode) else: "") & "," &
    (if j.hasMinutes: csvEscape($j.minutes) else: "") & "," & csvEscape($j.ntasks) & "," &
    csvEscape($j.cpus) & "," & csvEscape(j.dep) & "," & csvEscape(j.output) & "," &
    csvEscape(j.errorFile) & "," & csvEscape(j.chdir) & "," &
    csvEscape(j.script) & "," & csvEscape(j.args) & "," & csvEscape(j.wrap) & "," &
    csvEscape(s(j.arrayId)) & "," & csvEscape(s(j.arrayTask)) & "," &
    csvEscape(s(j.arrayLimit))

proc pi(s: string): int =
  if s.len == 0: -1 else: s.parseInt

proc parseJob*(cells: seq[string]): Job =
  Job(
    id: pi(cells[0]),
    state: cells[1],
    name: cells[2],
    submit: cells[3],
    start: cells[4],
    endTime: cells[5],
    pid: pi(cells[6]),
    exitcode: pi(cells[7]),
    hasExit: cells[7].len > 0,
    minutes: pi(cells[8]),
    hasMinutes: cells[8].len > 0,
    ntasks: pi(cells[9]),
    cpus: pi(cells[10]),
    dep: cells[11],
    output: cells[12],
    errorFile: cells[13],
    chdir: cells[14],
    script: cells[15],
    args: cells[16],
    wrap: cells[17],
    arrayId: pi(cells[18]),
    arrayTask: pi(cells[19]),
    arrayLimit: pi(cells[20]),
  )

proc dbPath*(): string =
  ## Explicit override wins; otherwise the DB lives in the user's XDG state
  ## dir so installed tools behave identically from any working directory.
  let p = getEnv("VSLURM_JOBS")
  if p.len > 0: return p
  let state = getEnv("XDG_STATE_HOME")
  let base = if state.len > 0: state else: getHomeDir() / ".local" / "state"
  base / "vslurm" / "jobs.csv"

proc ensureParent*(path: string) =
  ## Create the containing directory on first use (a fresh install has no
  ## state dir yet). Failure is left to the subsequent open() to report.
  let d = path.parentDir()
  if d.len == 0: return
  try: createDir(d)
  except OSError: discard

proc parseDbTime*(s: string): times.Time =
  s.parse("yyyy-MM-dd'T'HH:mm:ss").toTime

proc formatDbTime*(t: Time): string =
  t.local.format("yyyy-MM-dd'T'HH:mm:ss")

proc lockDb*(f: File) =
  while posix.lockf(f.getFileHandle(), posix.F_LOCK, 0) != 0:
    sleep(50)

proc unlockDb*(f: File) =
  discard posix.lockf(f.getFileHandle(), posix.F_ULOCK, 0)

proc loadJobs*(f: File): seq[Job] =
  ## Quoted cells may contain newlines (e.g. a --wrap payload with several
  ## commands), so records span physical lines: keep joining lines while the
  ## record's double quotes are unbalanced (inside a quoted cell).
  result = @[]
  var pending = ""
  var carrying = false
  for line in f.lines:
    let line = line.strip(chars = {'\r'}, trailing = true)
    if not carrying and line.len == 0: continue
    let rec = if carrying: pending & "\n" & line else: line
    var quotes = 0
    for c in rec:
      if c == '"': inc quotes
    if quotes mod 2 == 1:
      pending = rec
      carrying = true
      continue
    carrying = false
    let cells = csvSplit(rec)
    if cells.len != dbColumns: continue
    if cells[0].len == 0 or cells[0][0] notin {'0'..'9'}: continue
    result.add parseJob(cells)

proc saveJobs*(f: File, jobs: seq[Job]) =
  f.setFilePos(0)
  f.writeLine(dbHeader)
  for j in jobs:
    f.writeLine(toRow(j))
  f.flushFile()
  discard posix.ftruncate(f.getFileHandle(), f.getFilePos())

proc ensureFile*(path: string) =
  ## Atomically create-if-absent; fmReadWrite would truncate, and an
  ## unconditional fmWrite create would race between processes.
  let fd = posix.open(path.cstring, posix.O_RDWR or posix.O_CREAT, 0o644)
  if fd >= 0: discard posix.close(fd)

proc openDb*(path: string; f: var File): bool =
  ## Open (creating if needed) and lock the jobs DB, position at 0.
  ensureParent(path)
  ensureFile(path)
  if not f.open(path, fmReadWriteExisting):
    return false
  f.setFilePos(0)
  lockDb(f)
  true

proc closeDb*(f: File) =
  unlockDb(f)
  f.close()

proc nextJobId*(path: string): int =
  let seqPath = path.changeFileExt("seq")
  ensureParent(seqPath)
  ensureFile(seqPath)
  var sf: File
  if not sf.open(seqPath, fmReadWriteExisting):
    raise newException(IOError, "cannot open " & seqPath)
  sf.setFilePos(0)
  lockDb(sf)
  var content = ""
  try:
    content = sf.readAll()
    let n = if content.strip().len == 0: 0 else: content.strip().parseInt
    let id = n + 1
    sf.setFilePos(0)
    sf.write($id)
    sf.flushFile()
    discard posix.ftruncate(sf.getFileHandle(), sf.getFilePos())
    result = id
  finally:
    unlockDb(sf)
    sf.close()

proc readJobs*(path: string): seq[Job] =
  ## Snapshot the DB under a shared lock; a missing or unreadable DB is an
  ## empty job list (read-only tools print nothing rather than failing).
  result = @[]
  if not fileExists(path): return
  var f: File
  if not f.open(path, fmReadWriteExisting): return
  f.setFilePos(0)
  lockDb(f)
  try:
    result = loadJobs(f)
  finally:
    unlockDb(f)
    f.close()

proc resolvePath*(chdir, p: string): string =
  ## Relative output/error/script paths belong to the job's working
  ## directory, which is stored absolute at submit time.
  if p.isAbsolute: p else: chdir / p

proc allocatedMaxId*(db: string): int =
  ## Highest job ID ever allocated, per the monotonic sequence file.
  let seqPath = db.changeFileExt("seq")
  if not fileExists(seqPath): return 0
  var f: File
  if not f.open(seqPath): return 0
  defer: f.close()
  let content = f.readAll().strip()
  if content.len == 0: 0 else: content.parseInt

# ---------------------------------------------------------------------------
# Shell helpers shared by sbatch (quoting script args back into a command
# line) and slurmctld (building the exec line).

proc shellQuote*(s: string): string =
  "'" & s.replace("'", "'\\''") & "'"

proc shellWords*(s: string): seq[string] =
  ## Split a command line into words honoring single and double quotes and
  ## backslash escapes. Unterminated quoting is tolerated (rest is one word).
  result = @[]
  var cur = ""
  var have = false
  var i = 0
  while i < s.len:
    let c = s[i]
    case c
    of ' ', '\t':
      if have:
        result.add cur
        cur = ""
        have = false
    of '\'':
      have = true
      inc i
      while i < s.len and s[i] != '\'':
        cur.add s[i]
        inc i
    of '"':
      have = true
      inc i
      while i < s.len and s[i] != '"':
        if s[i] == '\\' and i + 1 < s.len and s[i + 1] == '"':
          cur.add '"'
          inc i
        else:
          cur.add s[i]
        inc i
    of '\\':
      have = true
      if i + 1 < s.len:
        cur.add s[i + 1]
        inc i
      else:
        cur.add '\\'
    else:
      cur.add c
      have = true
    inc i
  if have: result.add cur

proc localHostname*(): string =
  var buf = newString(256)
  buf[0] = '\0'
  if posix.gethostname(buf.cstring, 255) == 0:
    result = $buf.cstring
    setLen(result, result.len)
  else:
    result = "localhost"

proc currentUser*(): string =
  let u = getEnv("USER")
  if u.len > 0: u else: "unknown"

# ---------------------------------------------------------------------------
# sbatch --array / -a and --depend spec parsing.

proc warn*(msg: string; warned: var seq[string]) =
  warned.add msg
  stderr.writeLine(msg)

proc parseJobSpec*(s: string): tuple[job: int, task: int] =
  ## `123` -> (123, -1); `123_4` -> (123, 4). Invalid input -> (0, -1).
  result = (0, -1)
  if s.len == 0: return
  let us = s.find('_')
  if us < 0:
    if s.allCharsInSet(Digits): result = (s.parseInt, -1)
  else:
    let a = s[0 ..< us]
    let b = s[us + 1 .. ^1]
    if a.len > 0 and b.len > 0 and a.allCharsInSet(Digits) and
        b.allCharsInSet(Digits):
      result = (a.parseInt, b.parseInt)

proc displaySpec*(spec: tuple[job, task: int]): string =
  if spec.task >= 0: $spec.job & "_" & $spec.task else: $spec.job

proc expandArraySpec*(spec: string; warned: var seq[string]): tuple[ids: seq[int], limit: int] =
  ## Expand `1,3,5-7:2[%4]` into [1,3,5,7] plus a max-concurrent limit.
  ## Invalid pieces warn and are dropped; a wholly invalid spec yields no ids
  ## (job submitted as non-array, matching sbatch's failure mode).
  result.ids = @[]
  result.limit = -1
  var body = spec
  if body.len == 0: return
  # SLURM's documented form is `1-4%2`; the bracketed `1-4[%2]` is also seen
  var limitStr = ""
  let lb = body.rfind('[')
  let rb = body.rfind(']')
  if lb >= 0 and rb > lb:
    limitStr = body[lb + 1 ..< rb].replace("%", "")
    body = body[0 ..< lb] & body[rb + 1 .. ^1]
  elif lb >= 0 or rb >= 0:
    warn("sbatch: error: invalid --array value '" & spec & "'", warned)
    return
  else:
    let pct = body.rfind('%')
    if pct >= 0:
      limitStr = body[pct + 1 .. ^1]
      body = body[0 ..< pct]
  if limitStr.len > 0:
    if limitStr.allCharsInSet(Digits) and limitStr.parseInt > 0:
      result.limit = limitStr.parseInt
    else:
      warn("sbatch: error: invalid --array concurrency limit '" & limitStr &
        "' ignored", warned)
  for piece in body.split(','):
    let p = piece.strip()
    if p.len == 0: continue
    let stepAt = p.find(':')
    var rng = p
    var step = 1
    var valid = true
    if stepAt >= 0:
      rng = p[0 ..< stepAt]
      let st = p[stepAt + 1 .. ^1]
      if st.len == 0 or not st.allCharsInSet(Digits) or st.parseInt == 0:
        valid = false
      else:
        step = st.parseInt
    let dash = rng.find('-')
    if not valid: discard
    elif dash < 0:
      if rng.len == 0 or not rng.allCharsInSet(Digits):
        valid = false
      else:
        result.ids.add rng.parseInt
    else:
      let a = rng[0 ..< dash]
      let b = rng[dash + 1 .. ^1]
      if a.len == 0 or b.len == 0 or not a.allCharsInSet(Digits) or
          not b.allCharsInSet(Digits):
        valid = false
      else:
        let lo = a.parseInt
        let hi = b.parseInt
        if hi < lo:
          valid = false
        else:
          var k = lo
          while k <= hi:
            result.ids.add k
            inc k, step
    if not valid:
      warn("sbatch: error: invalid --array value '" & p & "'", warned)
  # de-duplicate while preserving first-seen order
  if result.ids.len > 0:
    var seen: seq[int] = @[]
    var uniq: seq[int] = @[]
    for v in result.ids:
      if v notin seen:
        seen.add v
        uniq.add v
    result.ids = uniq

proc needsQuoting*(s: string): bool =
  s.len == 0 or s.anyIt(it in {' ', '\t', '\'', '"', '\\', ',', '%', '$', ';'})

proc shellJoin*(args: seq[string]): string =
  ## Inverse of shellWords: quote only the words that need it, so plain
  ## argument lists round-trip through the DB unchanged.
  for i, a in args:
    if i > 0: result.add ' '
    if needsQuoting(a): result.add shellQuote(a) else: result.add a

proc expandSpec*(s: string; id, arrId, task: int; name: string; warned: var seq[string]): string =
  ## Expand sbatch filename-pattern specifiers: %j/%A are the job's own id
  ## and (for arrays) the master id; `task` is the array index or -1.
  ## %a for a non-array job expands to 4294967294 (SLURM's NO_VAL)
  ## rather than warning, matching real sbatch.
  proc num(v: int; pad: int): string =
    if pad > 1: align($v, pad, '0') else: $v
  var i = 0
  while i < s.len:
    let c = s[i]
    if c == '%' and i + 1 < s.len:
      var k = i + 1
      var pad = 0
      while k < s.len and s[k] in {'0'..'9'}:
        pad = pad * 10 + (ord(s[k]) - ord('0'))
        inc k
      if k >= s.len:
        result.add '%'
        break
      case s[k]
      of '%': result.add '%'
      of 'j': result.add num(id, pad)
      of 'A': result.add num(arrId, pad)
      of 'a':
        if task >= 0: result.add num(task, pad)
        else: result.add num(4294967294, pad)
      of 'x': result.add name
      of 't':
        if task >= 0: result.add num(task, pad)
        else: result.add num(0, pad)
      of 'N': result.add localHostname()
      of 'u': result.add currentUser()
      of 'J':
        result.add num(id, pad)
        result.add ".0"
      else:
        warn("sbatch: warning: unsupported format specifier '%" & s[k] &
          "' ignored", warned)
        result.add '%'
        result.add s[k]
      i = k + 1
    else:
      result.add c
      inc i

# ---------------------------------------------------------------------------
# Shared CLI plumbing used by sbatch, srun and sacct: option scanning,
# sbatch-style time-limit and --depend parsing, warn-once bookkeeping and
# SLURM elapsed-time formatting.

type OptCall* = tuple[opt, val: string]

proc scanArgs*(tokens: seq[string];
    shortMap: openArray[tuple[s: char, l: string]];
    valueOpts: openArray[string]; stopAtPositional: bool):
    tuple[calls: seq[OptCall], positionals: seq[string]] =
  ## Record (option, value) calls and collect positionals. A value-taking
  ## option consumes the next token as `--opt v` / `-o v`, or inline as
  ## `--opt=v` / `-ov`; unknown options never consume the next token.
  ## With stopAtPositional (srun) every token from the first positional on
  ## belongs to the command line.
  result.calls = @[]
  result.positionals = @[]
  var i = 0
  while i < tokens.len:
    let t = tokens[i]
    if t == "--":
      for j in i + 1 ..< tokens.len: result.positionals.add tokens[j]
      break
    if stopAtPositional and not (t.len > 1 and t[0] == '-'):
      for j in i ..< tokens.len: result.positionals.add tokens[j]
      break
    if t.startsWith("--") and t.len > 2:
      let body = t[2 .. ^1]
      let eq = body.find('=')
      if eq >= 0:
        result.calls.add (body[0 ..< eq], body[eq + 1 .. ^1])
      elif body in valueOpts and i + 1 < tokens.len:
        inc i
        result.calls.add (body, tokens[i])
      else:
        result.calls.add (body, "")
    elif t.len >= 2 and t[0] == '-':
      let c = t[1]
      var long = ""
      for m in shortMap:
        if m.s == c:
          long = m.l
          break
      if long == "":
        result.calls.add ($c, "")
      elif t.len > 2:
        result.calls.add (long, t[2 .. ^1])
      elif long in valueOpts and i + 1 < tokens.len:
        inc i
        result.calls.add (long, tokens[i])
      else:
        result.calls.add (long, "")
    else:
      result.positionals.add t
    inc i

proc warnOnce*(msg: string; warned: var seq[string]) =
  if msg notin warned:
    warned.add msg
    stderr.writeLine(msg)

proc displayOpt*(name: string): string =
  if name.len == 1: "-" & name else: "--" & name

proc parseTimeSpec*(v: string): int =
  ## plain int = minutes; MM:SS (rounded up); HH:MM:SS; D-HH:MM:SS. -1 = invalid.
  result = -1
  var days = 0
  var body = v
  let dash = v.find('-')
  if dash >= 0:
    let dp = v[0 ..< dash]
    if dash == 0 or dp.len == 0 or not dp.allCharsInSet(Digits): return
    days = dp.parseInt
    body = v[dash + 1 .. ^1]
  let parts = body.split(':')
  if parts.len == 1:
    if dash >= 0 or body.len == 0 or not body.allCharsInSet(Digits): return
    result = body.parseInt
  elif parts.len == 2:
    if parts[0].len == 0 or parts[1].len == 0 or
        not (parts[0].allCharsInSet(Digits) and parts[1].allCharsInSet(Digits)): return
    let m = parts[0].parseInt
    let s = parts[1].parseInt
    result = (m * 60 + s + 59) div 60
  elif parts.len == 3:
    if parts[0].len == 0 or parts[1].len == 0 or parts[2].len == 0 or
        not (parts[0].allCharsInSet(Digits) and parts[1].allCharsInSet(Digits) and
          parts[2].allCharsInSet(Digits)): return
    let h = parts[0].parseInt
    let m = parts[1].parseInt
    let s = parts[2].parseInt
    result = days * 1440 + h * 60 + m + (if s > 0: 1 else: 0)

const depTypes* = ["afterok", "afternotok", "afterany", "after"]

proc validateDep*(tool, v: string; warned: var seq[string]): string =
  var groups: seq[string] = @[]
  for g in v.split(','):
    if g.len == 0: continue
    let parts = g.split(':')
    let t = parts[0]
    if t notin depTypes:
      warnOnce(tool & ": warning: unsupported dependency type '" & t & "' ignored", warned)
      continue
    var ids: seq[string] = @[]
    var ok = parts.len > 1
    for p in parts[1 .. ^1]:
      let js = parseJobSpec(p)
      if p.len > 0 and js.job > 0:
        ids.add p # keep SLURM's master_task form for element refs
      else:
        warnOnce(tool & ": warning: invalid job id '" & p & "' in dependency '" &
          g & "' ignored", warned)
        ok = false
    if ok and ids.len > 0:
      groups.add t & ":" & ids.join(":")
  result = groups.join(",")

proc fmtElapsed*(secs: int): string =
  ## SLURM's [D-]HH:MM:SS elapsed-time format, as squeue/sacct print it.
  let s = max(secs, 0)
  let days = s div 86400
  let rem = s mod 86400
  let h = rem div 3600
  let m = (rem mod 3600) div 60
  let sec = rem mod 60
  result = align($h, 2, '0') & ":" & align($m, 2, '0') & ":" &
    align($sec, 2, '0')
  if days > 0: result = $days & "-" & result
