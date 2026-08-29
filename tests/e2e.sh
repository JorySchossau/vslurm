#!/usr/bin/env bash
# End-to-end acceptance test for vslurm. Run from the repo root.
set -euo pipefail

cd "$(dirname "$0")/.."
repo=$(pwd)

make -s all
w=$(mktemp -d)
export VSLURM_JOBS=$w/jobs.csv
# DAEMON_ONLY exists only in the daemon's env; test 12 asserts jobs don't
# inherit it (they carry the submitter's snapshot instead).
DAEMON_ONLY=should_not_leak ./slurmctld > "$w/slurmctld.log" 2>&1 &
sched_pid=$!
trap 'kill $sched_pid 2>/dev/null || true; wait $sched_pid 2>/dev/null || true; rm -rf "$w"' EXIT

pass=0
fail=0

note_fail() {
  echo "FAIL: $1"
  fail=$((fail + 1))
}

wait_state() {
  local id=$1 state=$2 timeout=${3:-30}
  local waited=0
  while [ $waited -le $((timeout * 2)) ]; do
    if "$repo/squeue" -h | awk -v id="$id" -v st="$state" '$1==id && $3==st{f=1} END{exit f?0:1}'; then
      return 0
    fi
    sleep 0.5
    waited=$((waited + 1))
  done
  return 1
}

# Test 1: basic submit/run/output/exit code; args reach the script;
# ex.sb's own directives name the output (slurmout/%x-%j.out).
cd "$w"
"$repo/sbatch" "$repo/ex.sb" one two > /dev/null
id1=$(tail -1 jobs.csv | cut -d, -f1)
if wait_state "$id1" CD 20; then
  if grep -q "ran okay one two" "$w/slurmout/tpotpipeline-$id1.out" && \
     [ "$(awk -F, -v j="$id1" '$1==j{print $8}' jobs.csv)" = "0" ]; then
    pass=$((pass + 1)); echo "PASS 1 basic"
  else
    note_fail "test 1: output/exitcode wrong"
  fi
else
  note_fail "test 1: never COMPLETED"
fi

# Test 2: custom name + output pattern from directives.
cat > "$w/nm.sb" <<EOF
#SBATCH --job-name=nm
#SBATCH -o out-%x-%j.out
true
EOF
"$repo/sbatch" "$w/nm.sb" > /dev/null
id2=$(tail -1 jobs.csv | cut -d, -f1)
if wait_state "$id2" CD 20; then
  if [ -f "$w/out-nm-$id2.out" ]; then
    pass=$((pass + 1)); echo "PASS 2 name/output"
  else
    note_fail "test 2: out-nm-$id2.out missing"
  fi
else
  note_fail "test 2: never COMPLETED"
fi

# Test 3: dependency chain — B waits for A.
cat > "$w/a.sb" <<EOF
#SBATCH -o a-%j.out
sleep 2
echo adone
EOF
cat > "$w/b.sb" <<EOF
#SBATCH -o b-%j.out
echo "B-ran"
EOF
"$repo/sbatch" "$w/a.sb" > /dev/null
idA=$(tail -1 jobs.csv | cut -d, -f1)
"$repo/sbatch" -d "afterok:$idA" "$w/b.sb" > /dev/null
idB=$(tail -1 jobs.csv | cut -d, -f1)
if wait_state "$idA" R 10; then
  if [ ! -e "$w/b-$idB.out" ] && "$repo/squeue" -h | awk -v id="$idB" '$1==id && $3=="PD"{exit 0}'; then
    :
  else
    note_fail "test 3: B ran before A finished"
  fi
  if wait_state "$idB" CD 20 && grep -q "B-ran" "$w/b-$idB.out"; then
    pass=$((pass + 1)); echo "PASS 3 dependency chain"
  else
    note_fail "test 3: B never completed"
  fi
else
  note_fail "test 3: A never started"
fi

# Test 4: failure semantics + afternotok/afterok gating.
cat > "$w/f.sb" <<EOF
#SBATCH -o f-%j.out
exit 3
EOF
cat > "$w/neg.sb" <<EOF
#SBATCH -o neg-%j.out
echo "neg-ran"
EOF
cat > "$w/pos.sb" <<EOF
#SBATCH -o pos-%j.out
echo "pos-ran"
EOF
"$repo/sbatch" "$w/f.sb" > /dev/null
idF=$(tail -1 jobs.csv | cut -d, -f1)
"$repo/sbatch" -d "afternotok:$idF" "$w/neg.sb" > /dev/null
idNeg=$(tail -1 jobs.csv | cut -d, -f1)
"$repo/sbatch" -d "afterok:$idF" "$w/pos.sb" > /dev/null
idPos=$(tail -1 jobs.csv | cut -d, -f1)
if wait_state "$idF" F 20 && \
   [ "$(awk -F, -v j="$idF" '$1==j{print $8}' jobs.csv)" = "3" ]; then
  if wait_state "$idNeg" CD 20 && grep -q "neg-ran" "$w/neg-$idNeg.out"; then
    if wait_state "$idPos" CA 10 && [ ! -e "$w/pos-$idPos.out" ]; then
      pass=$((pass + 1)); echo "PASS 4 failure semantics"
    else
      note_fail "test 4: afterok job not CANCELLED after FAILED dep"
    fi
  else
    note_fail "test 4: afternotok job never completed"
  fi
else
  note_fail "test 4: F never FAILED with code 3"
fi

# Test 5: timeout kills a stuck job.
cat > "$w/stuck.sb" <<EOF
#SBATCH -o stuck-%j.out
sleep 300
EOF
"$repo/sbatch" -t 1 "$w/stuck.sb" > /dev/null
idT=$(tail -1 jobs.csv | cut -d, -f1)
wait_state "$idT" R 10 || note_fail "test 5: stuck job never RUNNING"
if wait_state "$idT" TO 120; then
  tpid=$(awk -F, -v j="$idT" '$1==j{print $7}' jobs.csv)
  ok=1
  for i in $(seq 1 16); do
    if ! kill -0 "$tpid" 2>/dev/null; then ok=0; break; fi
    sleep 0.5
  done
  if [ $ok -eq 0 ]; then
    pass=$((pass + 1)); echo "PASS 5 timeout"
  else
    note_fail "test 5: timed-out process still alive"
  fi
else
  note_fail "test 5: never TIMEOUT"
fi

# Test 5b: array job — master aggregates, elements execute, %A_%a files,
# SLURM_ARRAY_TASK_ID env, dependency on the master, element cancel.
cat > "$w/arr.sb" <<EOF
#SBATCH -o arr-%A_%a.out
echo "task=\$SLURM_ARRAY_TASK_ID aid=\$SLURM_ARRAY_JOB_ID"
EOF
idArr=$("$repo/sbatch" -a "1-3" "$w/arr.sb" | awk '{print $4}')
wait_state "${idArr}_1" CD 20 && wait_state "${idArr}_2" CD 20 && \
  wait_state "${idArr}_3" CD 20 && wait_state "$idArr" CD 10 || true
if [ -f "$w/arr-${idArr}_1.out" ] && [ -f "$w/arr-${idArr}_2.out" ] && \
   [ -f "$w/arr-${idArr}_3.out" ] && \
   grep -q "task=1 aid=$idArr" "$w/arr-${idArr}_1.out"; then
  pass=$((pass + 1)); echo "PASS 5b array files/env"
else
  note_fail "test 5b: array output files or env wrong"
fi
if "$repo/squeue" -h -j "${idArr}_2" | awk -v t="${idArr}_2" '$1==t && $3=="CD"{f=1} END{exit f?0:1}'; then
  pass=$((pass + 1)); echo "PASS 5b squeue -j master_task"
else
  note_fail "test 5b: squeue -j master_task filter failed"
fi

# dependency on the array master id fires only once every element finished
cat > "$w/deparr.sb" <<EOF
#SBATCH -o deparr-%j.out
echo deparr-ran
EOF
idDepArr=$("$repo/sbatch" -d "afterok:$idArr" "$w/deparr.sb" | awk '{print $4}')
if wait_state "$idDepArr" CD 20; then
  pass=$((pass + 1)); echo "PASS 5b dep on array master"
else
  note_fail "test 5b: afterok on array master never satisfied"
fi

# dependency on a single array element fires when that element finishes
idDepEl=$("$repo/sbatch" --parsable -d "afterok:${idArr}_2" -o depel-%j.out --wrap 'echo depel-ran')
if wait_state "$idDepEl" CD 20 && grep -q "depel-ran" "$w/depel-$idDepEl.out"; then
  pass=$((pass + 1)); echo "PASS 5b dep on array element"
else
  note_fail "test 5b: afterok on element ${idArr}_2 never satisfied"
fi

# cancel one element of a fresh array
cat > "$w/arr2.sb" <<EOF
#SBATCH -o arr2-%A_%a.out
sleep 300
EOF
idArr2=$("$repo/sbatch" -a "1-2" "$w/arr2.sb" | awk '{print $4}')
if wait_state "${idArr2}_1" R 10 && wait_state "${idArr2}_2" R 10; then
  "$repo/scancel" "${idArr2}_1"
  sleep 1.5
  if "$repo/squeue" -h -j "${idArr2}_1" | awk '$3=="CA"{f=1} END{exit f?0:1}' && \
     "$repo/squeue" -h -j "${idArr2}_2" | awk '$3=="R"{f=1} END{exit f?0:1}'; then
    pass=$((pass + 1)); echo "PASS 5b element cancel"
  else
    note_fail "test 5b: element cancel wrong states"
  fi
  "$repo/scancel" "$idArr2" 2>/dev/null || true
else
  note_fail "test 5b: array2 elements never RUNNING"
fi

# array concurrency limit: 4 elements, at most 2 running at once
cat > "$w/lim.sb" <<EOF
#SBATCH -o lim-%A_%a.out
sleep 2
EOF
idLim=$("$repo/sbatch" -a "1-4%2" "$w/lim.sb" | awk '{print $4}')
sleep 3
nrun=$("$repo/squeue" -h -j "$idLim" | awk '$3=="R"{n++} END{print n+0}')
if [ "$nrun" -le 2 ]; then
  pass=$((pass + 1)); echo "PASS 5b array limit (running=$nrun<=2)"
else
  note_fail "test 5b: array limit exceeded ($nrun running)"
fi

# Test 6: cancel a running job.
cat > "$w/c.sb" <<EOF
#SBATCH -o c-%j.out
sleep 300
EOF
"$repo/sbatch" "$w/c.sb" > /dev/null
idC=$(tail -1 jobs.csv | cut -d, -f1)
if wait_state "$idC" R 10; then
  cpid=$(awk -F, -v j="$idC" '$1==j{print $7}' jobs.csv)
  "$repo/scancel" "$idC"
  ok=1
  for i in $(seq 1 16); do
    if ! kill -0 "$cpid" 2>/dev/null; then ok=0; break; fi
    sleep 0.5
  done
  if [ $ok -eq 0 ] && "$repo/squeue" -h | awk -v id="$idC" '$1==id && $3=="CA"{exit 0}'; then
    pass=$((pass + 1)); echo "PASS 6 cancel"
  else
    note_fail "test 6: cancel did not stop process / mark CA"
  fi
else
  note_fail "test 6: never RUNNING"
fi

# Test 7: CPU budget caps concurrency.
ncpu=$(nproc)
total=$((ncpu + 2))
cat > "$w/sl.sb" <<EOF
#SBATCH -o sl-%j.out
sleep 3
EOF
for i in $(seq 1 $total); do
  "$repo/sbatch" "$w/sl.sb" > /dev/null
done
sleep 5
nrun=$("$repo/squeue" -h | awk '$3 == "R"' | wc -l)
if [ "$nrun" -le "$ncpu" ]; then
  pass=$((pass + 1)); echo "PASS 7 concurrency (running=$nrun cap=$ncpu)"
else
  note_fail "test 7: $nrun running > cap $ncpu"
fi
allids=$(tail -n +2 jobs.csv | cut -d, -f1)
allok=1
for j in $allids; do
  if ! "$repo/squeue" -h | awk -v id="$j" '$1==id && ($3=="CD" || $3=="CA" || $3=="TO" || $3=="F"){exit 0}'; then
    allok=0
  fi
done
sleep 8
allok=1
for j in $allids; do
  if ! "$repo/squeue" -h | awk -v id="$j" '$1==id && ($3=="CD" || $3=="CA" || $3=="TO" || $3=="F"){exit 0}'; then
    allok=0
  fi
done
if [ $allok -eq 1 ]; then
  echo "info: all jobs reached terminal state"
else
  note_fail "some jobs never terminated"
fi

# Test 7b: --cpus overrides the machine core count both ways.
kill $sched_pid 2>/dev/null || true
wait $sched_pid 2>/dev/null || true
export VSLURM_JOBS=$w/jobs-cpus.csv
"$repo/slurmctld" --cpus 2 > "$w/slurmctld-cpus.log" 2>&1 &
sched_pid=$!
for i in $(seq 1 5); do
  "$repo/sbatch" -o "$w/sl7b-%j.out" --wrap 'sleep 3' > /dev/null
done
sleep 2
nlow=$("$repo/squeue" -h | awk '$3 == "R"' | wc -l)
kill $sched_pid 2>/dev/null || true
wait $sched_pid 2>/dev/null || true
export VSLURM_JOBS=$w/jobs.csv
DAEMON_ONLY=should_not_leak "$repo/slurmctld" > "$w/slurmctld.log" 2>&1 &
sched_pid=$!
if [ "$nlow" -eq 2 ]; then
  pass=$((pass + 1)); echo "PASS 7b slurmctld --cpus 2 caps at 2"
else
  note_fail "test 7b: --cpus 2 left $nlow running (expected 2)"
fi

# Test 8: srun runs a command, streams output, propagates the exit code.
echo "srun-hello" > "$w/srun-expect"
if "$repo/srun" -J sruntest echo "srun-hello" > "$w/srun.out" 2>"$w/srun.err"; then
  rc1=0
else
  rc1=$?
fi
if [ $rc1 -eq 0 ] && grep -q "srun-hello" "$w/srun.out"; then
  pass=$((pass + 1)); echo "PASS 8 srun basic"
else
  note_fail "test 8: srun did not run/print"
fi

# Test 9: srun propagates a nonzero exit code.
rc9=0
"$repo/srun" sh -c "exit 9" > /dev/null 2>&1 || rc9=$?
if [ $rc9 -eq 9 ]; then
  pass=$((pass + 1)); echo "PASS 9 srun exit code"
else
  note_fail "test 9: srun exit code $rc9 != 9"
fi

# Test 10: srun -o/-e route output; sacct reports fields, -j and -s filter.
"$repo/srun" -o "$w/sr-%j.out" -e "$w/sr-%j.err" sh -c "echo so; echo se 1>&2" > /dev/null 2>&1
id10=$(tail -1 jobs.csv | cut -d, -f1)
if grep -q "so" "$w/sr-$id10.out" && grep -q "se" "$w/sr-$id10.err" && \
   "$repo/sacct" -n -j "$id10" -o JobID,State,ExitCode | grep -q "^$id10 *COMPLETED(0) *0:0" && \
   "$repo/sacct" -n -P -j "$id10" -o JobID,State | grep -q "^$id10|COMPLETED(0)" && \
   "$repo/sacct" -n -s FAILED -o JobID | grep -vq "^$id10"; then
  pass=$((pass + 1)); echo "PASS 10 srun files + sacct filters"
else
  note_fail "test 10: srun files or sacct filters wrong"
fi

# Test 12: jobs inherit the submitter's environment (not the daemon's),
# and ambient SLURM_*/SBATCH_* keys from the submitting shell are replaced
# by the job's own values.
cat > "$w/env.sb" <<EOF
#!/bin/sh
echo "SUBMITVAR=[\$SUBMIT_VAR]"
echo "DAEMONVAR=[\$DAEMON_ONLY]"
echo "STALE=\$SLURM_JOB_ID"
EOF
SUBMIT_VAR=from_submitter SLURM_JOB_ID=999 \
  env -u DAEMON_ONLY "$repo/sbatch" "$w/env.sb" > /dev/null
id12=$(tail -1 jobs.csv | cut -d, -f1)
if wait_state "$id12" CD 20 && \
   grep -q "SUBMITVAR=\[from_submitter\]" "$w/slurm-$id12.out" && \
   grep -q "DAEMONVAR=\[\]" "$w/slurm-$id12.out" && \
   grep -q "STALE=$id12" "$w/slurm-$id12.out"; then
  pass=$((pass + 1)); echo "PASS 12 submitter env snapshot"
else
  note_fail "test 12: job env is not the submitter's snapshot"
fi

# Test 13: submit-side input env vars — sbatch reads SBATCH_* between
# directives and the CLI (env wins over directives, CLI over env), sets
# the array MIN/MAX/STEP/COUNT output vars, and warns on unsupported ones.
cat > "$w/inenv.sb" <<'EOF'
#SBATCH --job-name=fromdirective
#SBATCH -o unused-%j.out
echo "task=$SLURM_ARRAY_TASK_ID min=$SLURM_ARRAY_TASK_MIN max=$SLURM_ARRAY_TASK_MAX step=$SLURM_ARRAY_TASK_STEP count=$SLURM_ARRAY_TASK_COUNT"
echo "alias=$SLURM_JOBID nprocs=$SLURM_NPROCS nnodes=$SLURM_NNODES cpuson=$SLURM_CPUS_ON_NODE tpn=$SLURM_TASKS_PER_NODE proc=$SLURM_PROCID gtids=$SLURM_GTIDS"
EOF
id13=$(SBATCH_JOB_NAME=envname SBATCH_OUTPUT="$w/env-out-%A_%a.out" \
  SBATCH_ARRAY_INX=1-5:2 SBATCH_TIMELIMIT=1 SBATCH_QOS=high \
  "$repo/sbatch" --parsable "$w/inenv.sb" 2> "$w/inenv.err")
ok13=1
name13=$(awk -F, -v j="$id13" '$1==j{print $3}' jobs.csv)
mins13=$(awk -F, -v j="$id13" '$1==j{print $9}' jobs.csv)
out13="$w/env-out-${id13}_1.out"
[ "$name13" = "envname" ] || ok13=0                     # env beats directive
[ "$mins13" = "1" ] || ok13=0
wait_state "${id13}_1" CD 20 || ok13=0
grep -q "task=1 min=1 max=5 step=2 count=3" "$out13" || ok13=0
# SLURM_JOBID inside an element is its own row id (documented divergence
grep -q " nprocs=1 nnodes=1 cpuson=1 tpn=1 proc=0 gtids=0" "$out13" || ok13=0
grep -q "unsupported environment variable 'SBATCH_QOS' ignored" "$w/inenv.err" || ok13=0
# CLI still beats the environment
id13b=$(SBATCH_JOB_NAME=envname "$repo/sbatch" --parsable -J cliname "$w/inenv.sb")
[ "$(awk -F, -v j="$id13b" '$1==j{print $3}' jobs.csv)" = "cliname" ] || ok13=0
# srun input vars: SLURM_JOB_NAME/SLURM_NTASKS apply, -n overrides
SLURM_JOB_NAME=srunenv SLURM_NTASKS=2 "$repo/srun" sh -c 'echo "srunname=$SLURM_JOB_NAME n=$SLURM_NTASKS"' > "$w/srun-env.out" 2>/dev/null
grep -q "srunname=srunenv n=2" "$w/srun-env.out" || ok13=0
SLURM_NTASKS=5 "$repo/srun" -n 3 sh -c 'echo "n=$SLURM_NTASKS"' > "$w/srun-env2.out" 2>/dev/null
grep -q "^n=3" "$w/srun-env2.out" || ok13=0
if [ $ok13 -eq 1 ]; then
  pass=$((pass + 1)); echo "PASS 13 submit-side input env vars"
else
  note_fail "test 13: input env vars not respected"
fi

# Test 11: Ctrl-C on srun cancels the job.
"$repo/srun" sleep 300 > /dev/null 2>&1 &
srunpid=$!
sleep 2
id11=$(tail -1 jobs.csv | cut -d, -f1)
kill -INT $srunpid
rc11=0
wait $srunpid || rc11=$?
if [ $rc11 -ne 0 ] && "$repo/squeue" -h | awk -v id="$id11" '$1==id && $3=="CA"{exit 0}'; then
  pass=$((pass + 1)); echo "PASS 11 srun cancel"
else
  note_fail "test 11: srun cancel failed (rc=$rc11)"
fi

echo "PASS $pass/20"
if [ $fail -gt 0 ]; then
  exit 1
fi
exit 0
