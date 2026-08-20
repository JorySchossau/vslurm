## sacct: accounting view over the jobs DB. Reports jobs with their states,
## elapsed times and exit codes. Supports the subset of real sacct a basic
## user needs: job/state filters, a custom field list (--format), no-header
## and parsable output. By default array masters aggregate their elements,
## so every row (master, element, plain job) is shown; -X collapses arrays
## to the master row only.

import os, algorithm, sequtils, strutils, times
import vslurm_common except warn

const shortMap = [('j', "jobs"), ('s', "state"), ('o', "format"),
  ('n', "noheader"), ('p', "parsable"), ('P', "parsable2"),
  ('X', "allocations"), ('a', "allusers"), ('u', "user"),
  ('S', "starttime"), ('E', "endtime")]

const valueOpts = ["jobs", "state", "format", "starttime", "endtime", "user"]

const defaultFormat = "JobID,JobName,State,Elapsed,ExitCode"

type
  Opts = object
    jobs: seq[tuple[job, task: int]]
    states: seq[string]
    format: string
    noHeader: bool
    parsable: bool
    parsable2: bool
    mastersOnly: bool

proc stateMatches(filter, state: string): bool =
  ## Accept full state names or the squeue abbreviations (PD/R/CD/F/CA/TO).
  let f = filter.toUpperAscii
  let full = case f
    of "PD": stPending
    of "R": stRunning
    of "CD": stCompleted
    of "F": stFailed
    of "CA": stCancelled
    of "TO": stTimeout
    else: f
  full == state

proc jobElapsed(j: Job): string =
  if j.state == stPending or j.start.len == 0: return "00:00:00"
  let endTime = if j.endTime.len > 0: parseDbTime(j.endTime) else: getTime()
  fmtElapsed((endTime - parseDbTime(j.start)).inSeconds)

proc jobState(j: Job): string =
  ## sacct prints full state names, with the exit code when one was recorded.
  case j.state
  of stCompleted, stFailed:
    if j.hasExit: j.state & "(" & $j.exitcode & ")" else: j.state
  else:
    j.state

proc jobExitCode(j: Job): string =
  ## sacct's `code:signal` form; empty when never recorded.
  if j.hasExit: $j.exitcode & ":0" else: ""

proc timeField(s: string): string =
  if s.len == 0: "Unknown" else: s

proc field(j: Job; name: string): string =
  case name.toLowerAscii
  of "jobid": displayId(j)
  of "jobidraw": $j.id
  of "jobname", "name": j.name
  of "state": jobState(j)
  of "elapsed": jobElapsed(j)
  of "exitcode": jobExitCode(j)
  of "start": timeField(j.start)
  of "end": timeField(j.endTime)
  of "submit": timeField(j.submit)
  of "alloccpus", "ncpus", "cpus": $(j.ntasks * j.cpus)
  of "ntasks": $j.ntasks
  of "timelimit":
    if j.hasMinutes: fmtElapsed(j.minutes * 60) else: "unset"
  of "workdir": j.chdir
  of "arrayjobid":
    if j.isArrayJob: $j.arrayId else: ""
  of "arraytaskid":
    if j.isElementJob: $j.arrayTask else: ""
  else: ""

proc parseFormat(spec: string): seq[tuple[name: string, width: int]] =
  ## `JobID,State,JobName%20` → fields; `%N` sets a minimum column width.
  for piece in spec.split(','):
    let p = piece.strip()
    if p.len == 0: continue
    let pct = p.find('%')
    if pct >= 0:
      let name = p[0 ..< pct]
      let w = p[pct + 1 .. ^1]
      let width = if w.len > 0 and w.allCharsInSet(Digits): w.parseInt else: 0
      result.add (name, width)
    else:
      result.add (p, 0)
  if result.len == 0:
    result.add ("JobID", 0)

proc applyOption(opt, val: string; o: var Opts) =
  case opt
  of "jobs":
    for p in val.split(','):
      if p.len == 0: continue
      let js = parseJobSpec(p)
      if js.job > 0:
        o.jobs.add js
      else:
        stderr.writeLine("sacct: error: Invalid job id specified: " & p)
        quit(1)
  of "state":
    for p in val.split(','):
      if p.strip().len > 0: o.states.add p.strip()
  of "format": o.format = val
  of "noheader": o.noHeader = true
  of "parsable": o.parsable = true
  of "parsable2": o.parsable2 = true
  of "allocations": o.mastersOnly = true
  of "allusers", "user", "starttime", "endtime":
    discard # accepted, single-user single-node: no effect
  else:
    stderr.writeLine("sacct: error: Unknown option: " & displayOpt(opt))
    quit(1)

proc wanted(j: Job; o: Opts): bool =
  if o.mastersOnly and j.isElementJob: return false
  if o.jobs.len > 0:
    var matched = false
    for flt in o.jobs:
      if flt.job <= 0: continue
      if flt.task < 0:
        # master id matches the master row and every element of that array
        if j.id == flt.job or j.arrayId == flt.job: matched = true
      else:
        if j.arrayId == flt.job and j.arrayTask == flt.task: matched = true
    if not matched: return false
  if o.states.len > 0:
    var matched = false
    for s in o.states:
      if stateMatches(s, j.state): matched = true
    if not matched: return false
  true

proc main() =
  var o = Opts(format: defaultFormat)
  let scanned = scanArgs(commandLineParams(), shortMap, valueOpts, true)
  for c in scanned.calls:
    applyOption(c.opt, c.val, o)

  let fields = parseFormat(o.format)

  let jobs = sortedByIt(readJobs(dbPath()), it.id)

  var rows: seq[seq[string]] = @[]
  for j in jobs:
    if not wanted(j, o): continue
    var row: seq[string] = @[]
    for fld in fields:
      row.add field(j, fld.name)
    rows.add row

  if o.parsable or o.parsable2:
    # -p appends a trailing pipe, -P does not
    let sep = if o.parsable: "|" else: ""
    if not o.noHeader:
      echo fields.mapIt(it.name).join("|") & sep
    for row in rows:
      echo row.join("|") & sep
    quit(0)

  # columnar: width = max(field name, every cell), plus one space of padding
  var widths = newSeq[int](fields.len)
  for i, fld in fields:
    widths[i] = max(fld.width, fld.name.len)
  for row in rows:
    for i, cell in row:
      widths[i] = max(widths[i], cell.len)

  if not o.noHeader:
    var line = ""
    for i, fld in fields:
      if i > 0: line.add "  "
      line.add fld.name.alignLeft(widths[i])
    echo line.strip(trailing = false)

  for row in rows:
    var line = ""
    for i, cell in row:
      if i > 0: line.add "  "
      line.add cell.alignLeft(widths[i])
    echo line.strip(trailing = false)

main()
