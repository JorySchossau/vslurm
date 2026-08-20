#!/usr/bin/env bash
# Installed-on-PATH acceptance test: verifies the tools work when invoked by
# name from arbitrary directories with VSLURM_JOBS unset, i.e. that no
# path or file reference silently depends on the caller's cwd.
set -euo pipefail

cd "$(dirname "$0")/.."
repo=$(pwd)

root=$(mktemp -d)
make -s install PREFIX="$root"
w=$(mktemp -d)
export XDG_STATE_HOME="$w/state"
unset VSLURM_JOBS
PATH="$root/bin:$PATH"
export PATH

pass=0
fail=0

note_fail() {
  echo "FAIL: $1"
  fail=$((fail + 1))
}

# scheduler runs from an unrelated directory
mkdir -p "$w/scheddir"
(cd "$w/scheddir" && vsched > "$w/vsched.log" 2>&1) &
sched_pid=$!
trap 'kill $sched_pid 2>/dev/null || true; wait $sched_pid 2>/dev/null || true; rm -rf "$w" "$root"' EXIT

wait_state() {
  local id=$1 state=$2 timeout=${3:-30}
  local waited=0
  while [ $waited -le $((timeout * 2)) ]; do
    if squeue -h | awk -v id="$id" -v st="$state" '$1==id && $3==st{f=1} END{exit f?0:1}'; then
      return 0
    fi
    sleep 0.5
    waited=$((waited + 1))
  done
  return 1
}

# 1. submit from dir A with a relative script path; output lands in dir A
mkdir -p "$w/dirA"
cat > "$w/dirA/job.sb" <<EOF
#!/bin/sh
echo "submit-dir=\$(pwd) script=\$0"
EOF
cd "$w/dirA"
id1=$(sbatch job.sb | awk '{print $4}')
cd /
if wait_state "$id1" CD 20; then
  if grep -q "submit-dir=$w/dirA script=$w/dirA/job.sb" "$w/dirA/slurm-$id1.out"; then
    pass=$((pass + 1)); echo "PASS 1 installed: relative submit + output dir"
  else
    note_fail "test 1: output wrong: $(cat "$w/dirA/slurm-$id1.out" 2>/dev/null)"
  fi
else
  note_fail "test 1: never COMPLETED (log: $(tail -3 "$w/vsched.log" 2>/dev/null))"
fi

# 2. the state dir default is honored: same DB seen from anywhere
cd /tmp
if [ "$(sacct -n -j "$id1" -o JobID,State | awk '{print $1, $2}')" = "$id1 COMPLETED(0)" ]; then
  pass=$((pass + 1)); echo "PASS 2 installed: shared XDG-state DB"
else
  note_fail "test 2: sacct could not see job $id1"
fi

# 3. -D with a relative path is resolved against the submit cwd
mkdir -p "$w/dirA/dirB"
cat > "$w/dirA/jobB.sb" <<EOF
#!/bin/sh
echo "chdir=\$(pwd)"
EOF
cd "$w/dirA"
id3=$(sbatch -D dirB -o b-%j.out jobB.sb | awk '{print $4}')
cd /
if wait_state "$id3" CD 20; then
  if grep -q "chdir=$w/dirA/dirB" "$w/dirA/dirB/b-$id3.out"; then
    pass=$((pass + 1)); echo "PASS 3 installed: relative -D resolved at submit"
  else
    note_fail "test 3: wrong chdir: $(cat "$w/dirA/dirB/b-$id3.out" 2>/dev/null)"
  fi
else
  note_fail "test 3: never COMPLETED"
fi

# 4. srun works installed too, and scancel reaches the shared DB
cd /tmp
(srun sleep 300 > /dev/null 2>&1 || true) &
srunpid=$!
sleep 2
id4=$(sacct -n -o JobID -X | tail -1 | tr -d ' ')
scancel "$id4"
if squeue -h -j "$id4" | awk '$3=="CA"{f=1} END{exit f?0:1}'; then
  pass=$((pass + 1)); echo "PASS 4 installed: srun/scancel share the DB"
else
  note_fail "test 4: scancel/squeue did not agree on job $id4"
fi
wait $srunpid 2>/dev/null || true

echo "PASS $pass/4"
if [ $fail -gt 0 ]; then
  exit 1
fi
exit 0
