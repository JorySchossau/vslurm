#!/usr/bin/env bash
# Diagnostics (rustc-style error reporting) acceptance test for vslurm.
# Run from the repo root. Verifies that sbatch reports file problems with
# spans, carets, notes and actionable help — and never changes submit
# semantics: warnings still submit, errors still exit 1.
set -euo pipefail

cd "$(dirname "$0")/.."
repo=$(pwd)

make -s all
w=$(mktemp -d)
export VSLURM_JOBS=$w/jobs.csv
export NO_COLOR=1
export TERM=dumb
trap 'rm -rf "$w"' EXIT

pass=0
fail=0

note_fail() {
  echo "FAIL: $1"
  fail=$((fail + 1))
}

expect_out() {
  # expect_out <desc> <grep-pattern> <file>
  if grep -q -- "$2" "$3"; then
    pass=$((pass + 1))
    echo "PASS $1"
  else
    note_fail "$1 (pattern '$2' not in $3)"
  fi
}

# 1: misspelled option in a directive -> span + did-you-mean fix.
cat > "$w/typo.sb" <<'EOF'
#!/bin/bash
#SBATCH --timout=5
true
EOF
"$repo/sbatch" "$w/typo.sb" >/dev/null 2> "$w/typo.err" || true
expect_out "1 typo: summary"        "sbatch: warning: unknown option '--timout' ignored" "$w/typo.err"
expect_out "1 typo: file:line:col"  "typo.sb:2:9" "$w/typo.err"
expect_out "1 typo: source line"    "2 | #SBATCH --timout=5" "$w/typo.err"
expect_out "1 typo: caret"          "\^*\^ unknown option" "$w/typo.err"
expect_out "1 typo: help"           "help: did you mean '--time'?" "$w/typo.err"
expect_out "1 typo: fix line"       "#SBATCH --time=5" "$w/typo.err"

# 2: same typo from the CLI -> bare message, no span, still a warning.
"$repo/sbatch" --timout=5 --wrap true >/dev/null 2> "$w/cli.err" || true
expect_out "2 cli typo: summary" "sbatch: warning: unknown option '--timout' ignored" "$w/cli.err"
if grep -q -- "-->" "$w/cli.err"; then
  note_fail "2 cli typo must not carry a script span"
else
  pass=$((pass + 1)); echo "PASS 2 cli typo: no span"
fi

# 3: directive below the script body -> told why it is ignored.
cat > "$w/late.sb" <<'EOF'
#!/bin/bash
echo hi
#SBATCH --output=late-%j.out
EOF
"$repo/sbatch" "$w/late.sb" >/dev/null 2> "$w/late.err" || true
expect_out "3 late: ignored" "this directive is ignored" "$w/late.err"
expect_out "3 late: reason"  "sbatch reads #SBATCH lines only before the first command" "$w/late.err"

# 4: #sbatch lowercase -> looks like a directive, treated as a comment.
cat > "$w/lc.sb" <<'EOF'
#!/bin/bash
#sbatch --mem=4G
true
EOF
"$repo/sbatch" "$w/lc.sb" >/dev/null 2> "$w/lc.err" || true
expect_out "4 lowercase: detected" "treated as a comment" "$w/lc.err"
expect_out "4 lowercase: fix"     "spell it exactly '#SBATCH'" "$w/lc.err"

# 5: value-taking option with no value in a directive.
cat > "$w/noval.sb" <<'EOF'
#!/bin/bash
#SBATCH --output
true
EOF
"$repo/sbatch" "$w/noval.sb" >/dev/null 2> "$w/noval.err" || true
expect_out "5 noval: missing" "'--output' needs a value" "$w/noval.err"
expect_out "5 noval: fix"     "#SBATCH --output=VALUE" "$w/noval.err"

# 6: mixed dependency separators -> fatal, with the value underlined.
cat > "$w/mix.sb" <<'EOF'
#!/bin/bash
#SBATCH -d afterok:1,afterany:2?afterok:3
true
EOF
if "$repo/sbatch" "$w/mix.sb" >/dev/null 2> "$w/mix.err"; then
  note_fail "6 mixed separators must be fatal"
else
  pass=$((pass + 1)); echo "PASS 6 mixed separators: fatal"
fi
expect_out "6 mixed: message" "cannot be mixed" "$w/mix.err"
expect_out "6 mixed: span"    "mix.sb:2:12" "$w/mix.err"

# 7: invalid --array -> fatal with syntax help.
if "$repo/sbatch" -a "1-x" "$w/noval.sb" >/dev/null 2> "$w/arr.err"; then
  note_fail "7 invalid array must be fatal"
else
  pass=$((pass + 1)); echo "PASS 7 array: fatal"
fi
expect_out "7 array: message" "invalid --array value '1-x'" "$w/arr.err"
expect_out "7 array: help"    "ranges 1-10, lists 1,3,7, optional %N concurrency limit" "$w/arr.err"

# 8: warnings accumulate — several problems reported in one submit.
cat > "$w/many.sb" <<'EOF'
#!/bin/bash
#SBATCH --timout=5
#SBATCH --nodelist=c1
#SBATCH --time=banana
true
EOF
"$repo/sbatch" "$w/many.sb" >/dev/null 2> "$w/many.err" || true
expect_out "8 accumulate: typo"   "--timout" "$w/many.err"
expect_out "8 accumulate: nolist" "--nodelist" "$w/many.err"
expect_out "8 accumulate: time"   "--time=banana" "$w/many.err"

# 9: warnings never block the submit — job id still printed.
out=$("$repo/sbatch" "$w/many.sb" 2>/dev/null)
if [[ "$out" == Submitted\ batch\ job* ]]; then
  pass=$((pass + 1)); echo "PASS 9 warnings do not block submit"
else
  note_fail "9 submit blocked by warnings (got: $out)"
fi

# 10: a good script produces zero diagnostics.
cat > "$w/clean.sb" <<'EOF'
#!/bin/bash
#SBATCH --job-name=clean
#SBATCH --output=out-%j.out
#SBATCH --time=5
true
EOF
"$repo/sbatch" "$w/clean.sb" >/dev/null 2> "$w/clean.err"
if [ ! -s "$w/clean.err" ]; then
  pass=$((pass + 1)); echo "PASS 10 clean script: silent"
else
  note_fail "10 clean script should be silent:"$(cat "$w/clean.err")
fi

# 11: nonexistent script stays a one-line error, like real sbatch.
if "$repo/sbatch" "$w/nope.sb" >/dev/null 2> "$w/nope.err"; then
  note_fail "11 missing script must be fatal"
else
  pass=$((pass + 1)); echo "PASS 11 missing script: fatal"
fi
expect_out "11 missing: message" "sbatch: error: $w/nope.sb: No such file or directory" "$w/nope.err"

echo "PASS $pass/$((pass + fail))"
[ "$fail" -eq 0 ]
