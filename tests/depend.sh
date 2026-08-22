#!/usr/bin/env bash
# Dependency-semantics acceptance test for vslurm. Run from the repo root.
#
# Matches real SLURM (verified on a live cluster and against slurmctld's
# handle_invalid_dependency/_kill_dependent): "Once a job dependency
# fails due to the termination state of a preceding job, the dependent
# job will never be run" — it is CANCELLED, never left PENDING.
#
# Verification uses only local evidence: the job ids sbatch returns and
# the jobs' own output files (a file exists iff vsched launched the job;
# payloads timestamp themselves so ordering is provable from the files).
# Nothing is read back from vsched's output or the jobs DB, so the test
# also works against an already-running scheduler:
#
#   tests/depend.sh existing
#
# which skips starting vsched and submits to whatever VSLURM_JOBS (or the
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
  ./vsched > "$w/vsched.log" 2>&1 &
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

echo "PASS $pass/3"
if [ $fail -gt 0 ]; then
  exit 1
fi
