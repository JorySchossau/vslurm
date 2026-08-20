import os, strutils, times
import vslurm_common

proc fmtTime(secs: int): string =
  if secs < 0: return "0:00"
  let days = secs div 86400
  let rem = secs mod 86400
  let h = rem div 3600
  let m = (rem mod 3600) div 60
  let s = rem mod 60
  if days > 0:
    $days & "-" & align($h, 2, '0') & ":" & align($m, 2, '0') & ":" & align($s, 2, '0')
  else:
    align($h, 2, '0') & ":" & align($m, 2, '0') & ":" & align($s, 2, '0')

proc jobTime(j: Job): string =
  if j.state == stPending: return "0:00"
  if j.start.len == 0: return "0:00"
  if j.state == stRunning:
    fmtTime((getTime() - parseDbTime(j.start)).inSeconds)
  elif j.endTime.len == 0:
    "0:00"
  else:
    fmtTime((parseDbTime(j.endTime) - parseDbTime(j.start)).inSeconds)

proc printRow(jobid, name, state, time: string) =
  let jid = jobid[0 .. min(jobid.high, 13)]
  let n = name[0 .. min(name.high, 18)]
  echo jid.alignLeft(15) & n.alignLeft(20) & state.alignLeft(8) & time

proc main() =
  var noHeader = false
  var filter: seq[tuple[job, task: int]] = @[]
  let args = commandLineParams()
  var i = 0
  while i < args.len:
    if args[i] == "-h":
      noHeader = true
    elif args[i] == "-j" and i + 1 < args.len:
      inc i
      for p in args[i].split(','):
        if p.len > 0: filter.add parseJobSpec(p)
    elif args[i].startsWith("-j="):
      for p in args[i][3 .. ^1].split(','):
        if p.len > 0: filter.add parseJobSpec(p)
    inc i

  if not noHeader:
    printRow("JOBID", "NAME", "STATE", "TIME")

  let jobs = readJobs(dbPath())

  for j in jobs:
    if filter.len > 0:
      var matched = false
      for flt in filter:
        if flt.job <= 0: continue
        if flt.task < 0:
          # master id matches the master row and every element of that array
          if j.id == flt.job or j.arrayId == flt.job: matched = true
        else:
          if j.arrayId == flt.job and j.arrayTask == flt.task: matched = true
      if not matched: continue
    printRow(displayId(j), j.name, stateAbbr(j.state), jobTime(j))

main()
