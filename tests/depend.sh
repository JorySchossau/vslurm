#!/usr/bin/env bash
# Dependency-semantics acceptance test for vslurm. Run from the repo root.
#
# Matches real SLURM (verified on a live cluster and against slurmctld's
# handle_invalid_dependency/_kill_dependent): "Once a job dependency
# fails due to the termination state of a preceding job, the dependent
# job will never be run" — it is CANCELLED, never left PENDING.
#
# Verification uses only local evidence: the job ids sbatch returns and
# the jobs' own output files (a file exists iff slurmctld launched the job;
# payloads timestamp themselves so ordering is provable from the files).
# Nothing is read back from slurmctld's output or the jobs DB, so the test
# also works against an already-running scheduler:
#
#   tests/depend.sh existing
#
# which skips starting slurmctld and submits to whatever VSLURM_JOBS (or the
# default DB) the ambient environment points at.
set -euo pipefail

cd "$(dirname "$0")/.."
repo=$(pwd)

if [ $# -gt 1 ] || { [ $# -eq 1 ] && [ "$1" != "existing" ]; }; then
  echo "usage: $0 [existing]" >&2
  exit 2
fi

make -s all
w=$(mktemp -d)

sched_pid=""
if [ "${1:-}" != "existing" ]; then
  export VSLURM_JOBS=$w/jobs.csv
  ./slurmctld > "$w/slurmctld.log" 2>&1 &
  sched_pid=$!
fi

cleanup() {
  if [ -n "$sched_pid" ]; then
    kill "$sched_pid" 2>/dev/null || true
    wait "$sched_pid" 2>/dev/null || true
  fi
  rm -rf "$w"
}
trap cleanup EXIT

pass=0
fail=0

note_fail() {
  echo "FAIL: $1"
  fail=$((fail + 1))
}

# Wait until $file contains $pat: the file only appears once the job has
# actually been launched, so this doubles as "job reached this point".
wait_grep() {
  local file=$1 pat=$2 timeout=${3:-30}
  local waited=0
  while [ $waited -le $((timeout * 10)) ]; do
    if [ -f "$file" ] && grep -q "$pat" "$file"; then
      return 0
    fi
    sleep 0.1
    waited=$((waited + 1))
  done
  return 1
}

# Give a misbehaving scheduler $secs seconds to (wrongly) launch a job,
# then check that its output file never appeared.
absent_after() {
  local file=$1 secs=$2
  sleep "$secs"
  [ ! -e "$file" ]
}

cd "$w"

# Case 1: dependent job RUNS when its dependency succeeds — and only then.
idA=$("$repo/sbatch" --parsable -o "$w/ok-a-%j.out" --wrap 'sleep 2; echo "A-done $(date +%s)"')
idB=$("$repo/sbatch" --parsable -o "$w/ok-b-%j.out" -d "afterok:$idA" --wrap 'echo "B-ran $(date +%s)"')
if wait_grep "$w/ok-a-$idA.out" "A-done" 30; then
  if wait_grep "$w/ok-b-$idB.out" "B-ran" 30; then
    ta=$(awk '/A-done/{print $2}' "$w/ok-a-$idA.out")
    tb=$(awk '/B-ran/{print $2}' "$w/ok-b-$idB.out")
    if [ "$tb" -ge "$ta" ]; then
      pass=$((pass + 1)); echo "PASS 1 afterok runs after dependency succeeds"
    else
      note_fail "case 1: B ran at $tb, before A finished at $ta"
    fi
  else
    note_fail "case 1: B never ran after A completed"
  fi
else
  note_fail "case 1: A never started"
fi

# Case 2: dependent job is never run when its dependency fails. F printing
# its marker proves the payload executed; whether its exit 3 was treated as
# failure is observable only through G, which must never launch.
idF=$("$repo/sbatch" --parsable -o "$w/bad-f-%j.out" --wrap 'sleep 2; echo "F-ran"; exit 3')
idG=$("$repo/sbatch" --parsable -o "$w/bad-g-%j.out" -d "afterok:$idF" --wrap 'echo "G-ran"')
if wait_grep "$w/bad-f-$idF.out" "F-ran" 30; then
  if absent_after "$w/bad-g-$idG.out" 6; then
    pass=$((pass + 1)); echo "PASS 2 afterok dependency fails -> never run"
  else
    note_fail "case 2: dependent job ran despite FAILED dependency"
  fi
else
  note_fail "case 2: F never ran"
fi

# Case 3: mirror image — afternotok on a job that succeeds also never runs.
idH=$("$repo/sbatch" --parsable -o "$w/ok-h-%j.out" --wrap 'sleep 2; echo "H-done"')
idI=$("$repo/sbatch" --parsable -o "$w/bad-i-%j.out" -d "afternotok:$idH" --wrap 'echo "I-ran"')
if wait_grep "$w/ok-h-$idH.out" "H-done" 30; then
  if absent_after "$w/bad-i-$idI.out" 6; then
    pass=$((pass + 1)); echo "PASS 3 afternotok dependency succeeds -> never run"
  else
    note_fail "case 3: afternotok job ran after dependency COMPLETED"
  fi
else
  note_fail "case 3: H never ran"
fi

# Case 4: `?` any-of — dependent runs as soon as ONE branch completes.
idJ=$("$repo/sbatch" --parsable -o "$w/q-j-%j.out" --wrap 'sleep 3; echo "J-done"')
idK=$("$repo/sbatch" --parsable -o "$w/q-k-%j.out" --wrap 'echo "K-done"')
idL=$("$repo/sbatch" --parsable -o "$w/q-l-%j.out" -d "afterok:$idJ?afterok:$idK" --wrap 'echo "L-ran"')
if wait_grep "$w/q-k-$idK.out" "K-done" 30 && \
   ! grep -q "J-done" "$w/q-j-$idJ.out"; then
  if wait_grep "$w/q-l-$idL.out" "L-ran" 10; then
    pass=$((pass + 1)); echo "PASS 4 '?' any-of releases on first satisfied branch"
  else
    note_fail "case 4: any-of dependent never ran after one branch completed"
  fi
else
  note_fail "case 4: setup failed (K not done or J finished too early)"
fi

# Case 5: `?` with every branch failed cancels the dependent job.
idM=$("$repo/sbatch" --parsable -o "$w/q-m-%j.out" --wrap 'sleep 2; echo "M-ran"; exit 4')
idN=$("$repo/sbatch" --parsable -o "$w/q-n-%j.out" --wrap 'sleep 2; echo "N-ran"; exit 4')
idO=$("$repo/sbatch" --parsable -o "$w/q-o-%j.out" -d "afterok:$idM?afterok:$idN" --wrap 'echo "O-ran"')
if wait_grep "$w/q-m-$idM.out" "M-ran" 30 && wait_grep "$w/q-n-$idN.out" "N-ran" 30; then
  if absent_after "$w/q-o-$idO.out" 6; then
    pass=$((pass + 1)); echo "PASS 5 '?' all branches failed -> never run"
  else
    note_fail "case 5: any-of dependent ran despite all branches failing"
  fi
else
  note_fail "case 5: setup failed (a branch never FAILED)"
fi

# Case 6: mixed ','/'?' separators are a hard submit error.
if "$repo/sbatch" -d "afterok:1,afterany:2?afterok:3" --wrap 'true' \
     > "$w/sep.out" 2> "$w/sep.err"; then
  note_fail "case 6: mixed separators accepted (should be fatal)"
else
  if grep -q "cannot be mixed" "$w/sep.err"; then
    pass=$((pass + 1)); echo "PASS 6 mixed separators rejected at submit"
  else
    note_fail "case 6: mixed separators failed without the expected message"
  fi
fi

# Case 7: singleton — dependent waits for same-name job to finish.
idP=$("$repo/sbatch" --parsable -J singleton-test -o "$w/sg-p-%j.out" --wrap 'sleep 3; echo "P-done"')
idQ=$("$repo/sbatch" --parsable -J singleton-test -o "$w/sg-q-%j.out" -d singleton --wrap 'echo "Q-ran"')
if wait_grep "$w/sg-p-$idP.out" "P-done" 30; then
  if wait_grep "$w/sg-q-$idQ.out" "Q-ran" 10; then
    if [ ! -e "$w/sg-q-$idQ.out" ]; then
      note_fail "case 7: singleton job never ran"
    else
      pass=$((pass + 1)); echo "PASS 7 singleton waits for same-name job"
    fi
  else
    note_fail "case 7: singleton job never ran"
  fi
else
  note_fail "case 7: first same-name job never finished"
fi

# Case 8: after:JOBID — dependent runs once the dependency has STARTED,
# long before it finishes.
idR=$("$repo/sbatch" --parsable -o "$w/af-r-%j.out" --wrap 'sleep 5; echo "R-done"')
idS=$("$repo/sbatch" --parsable -o "$w/af-s-%j.out" -d "after:$idR" --wrap 'echo "S-ran $(date +%s)"')
if wait_grep "$w/af-s-$idS.out" "S-ran" 10; then
  # S launched while R (sleep 5) was still running: its marker must predate
  # R's own completion marker in the same tick window.
  if ! grep -q "R-done" "$w/af-r-$idR.out"; then
    pass=$((pass + 1)); echo "PASS 8 after: fires on dependency START"
  else
    note_fail "case 8: dependent ran only after dependency finished"
  fi
  wait_grep "$w/af-r-$idR.out" "R-done" 30 || true
else
  note_fail "case 8: dependent never ran while dependency was running"
fi

# Case 9: after:JOBID+1 — the +minutes delay holds the job back.
idT=$("$repo/sbatch" --parsable -o "$w/af-t-%j.out" --wrap 'echo "T-start $(date +%s)"')
idU=$("$repo/sbatch" --parsable -o "$w/af-u-%j.out" -d "after:$idT+1" --wrap 'echo "U-ran"')
if wait_grep "$w/af-t-$idT.out" "T-start" 30; then
  if absent_after "$w/af-u-$idU.out" 5; then
    pass=$((pass + 1)); echo "PASS 9 after:+minutes delay holds the job"
  else
    note_fail "case 9: dependent ran despite +1 minute delay"
  fi
else
  note_fail "case 9: dependency never started"
fi

# Case 10: aftercorr — element-wise gating between two arrays.
idV=$("$repo/sbatch" --parsable -a 1-2 -o "$w/ac-v-%A_%a.out" --wrap \
  'if [ "$SLURM_ARRAY_TASK_ID" = 2 ]; then exit 5; fi; echo "V-$SLURM_ARRAY_TASK_ID"')
sleep 1
idW=$("$repo/sbatch" --parsable -a 1-2 -o "$w/ac-w-%A_%a.out" -d "aftercorr:$idV" --wrap \
  'echo "W-$SLURM_ARRAY_TASK_ID"')
# %A in the dependent array's pattern expands to ITS OWN master id (idW)
if wait_grep "$w/ac-w-${idW}_1.out" "W-1" 30; then
  if absent_after "$w/ac-w-${idW}_2.out" 6; then
    pass=$((pass + 1)); echo "PASS 10 aftercorr gates per array element"
  else
    note_fail "case 10: aftercorr element 2 ran despite failed counterpart"
  fi
else
  note_fail "case 10: aftercorr element 1 never ran"
fi

# Case 11: afterburstbuffer behaves like afterany here.
idX=$("$repo/sbatch" --parsable -o "$w/bb-x-%j.out" --wrap 'sleep 2; echo "X-done"')
idY=$("$repo/sbatch" --parsable -o "$w/bb-y-%j.out" -d "afterburstbuffer:$idX" --wrap 'echo "Y-ran"')
if wait_grep "$w/bb-y-$idY.out" "Y-ran" 30; then
  pass=$((pass + 1)); echo "PASS 11 afterburstbuffer satisfied on termination"
else
  note_fail "case 11: afterburstbuffer dependent never ran"
fi

echo "PASS $pass/11"
if [ $fail -gt 0 ]; then
  exit 1
fi
