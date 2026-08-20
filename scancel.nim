## scancel: mark PENDING/RUNNING jobs CANCELLED and SIGTERM their process.
## The row's state is authoritative; vsched escalates to SIGKILL if needed.
## Accepts plain ids, array master ids (cancels the whole array) and
## `master_task` element ids.

import os, times
import posix except Time
import vslurm_common

proc usage(): void =
  stderr.writeLine("Usage: scancel <jobid...>")
  quit(1)

proc main() =
  let args = commandLineParams()
  if args.len == 0: usage()
  var ids: seq[tuple[job, task: int]] = @[]
  for a in args:
    let js = parseJobSpec(a)
    if js.job > 0:
      ids.add js
    else:
      stderr.writeLine("scancel: error: invalid job id " & a)
      quit(1)

  let db = dbPath()
  var f: File
  if not openDb(db, f):
    for idv in ids:
      stderr.writeLine("scancel: error: Job " & displaySpec(idv) & " not found")
    quit(1)

  var anyMissing = false
  try:
    var jobs = loadJobs(f)
    let nowStr = formatDbTime(getTime())
    for idv in ids:
      var found = false
      for i in 0 ..< jobs.len:
        let j = jobs[i]
        let matches = if idv.task < 0:
            j.id == idv.job or j.arrayId == idv.job
          else:
            j.arrayId == idv.job and j.arrayTask == idv.task
        if not matches: continue
        found = true
        if j.state == stPending:
          jobs[i].state = stCancelled
          jobs[i].endTime = nowStr
        elif j.state == stRunning:
          if j.pid > 0 and posix.kill(Pid(j.pid), 0) == 0:
            discard posix.kill(Pid(j.pid), SIGTERM)
          jobs[i].state = stCancelled
          jobs[i].endTime = nowStr
        # terminal states: no change, no message
      if not found:
        stderr.writeLine("scancel: error: Job " & displaySpec(idv) & " not found")
        anyMissing = true
    saveJobs(f, jobs)
  finally:
    closeDb(f)
  if anyMissing: quit(1)
  quit(0)

main()
