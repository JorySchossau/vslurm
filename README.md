# vslurm — localhost SLURM simulator

vslurm simulates the day-to-day SLURM workflow (`sbatch` / `squeue` /
`scancel` / `srun` / `sacct` plus a scheduler daemon) on a single machine,
so sb files and the programs they run can be verified end-to-end without a
cluster. Scheduling fidelity is deliberately not the goal; interface and
artifact fidelity is: directives are parsed like real sbatch, output/error
files appear exactly where SLURM would put them, exit codes and states
match, and the scripts actually run under `$SHELL` or their shebang
interpreter.

## Quick start

Requires [Nim](https://nim-lang.org) 2.x and a POSIX shell.

```console
$ make                     # builds sbatch squeue scancel srun sacct slurmctld
$ ./slurmctld &            # the scheduler; leave it running in a terminal
$ cat > job.sb <<'EOF'
#!/bin/bash
#SBATCH --job-name=demo
#SBATCH --output=out-%j.out
#SBATCH --error=out-%j.err
#SBATCH --time=5           # minutes
./my_program --input data.dat
EOF
$ ./sbatch job.sb
Submitted batch job 1
$ ./squeue                 # JOBID NAME STATE TIME; CD = completed
$ cat out-1.out            # stdout landed exactly where SLURM would put it
$ ./sacct -n               # accounting: state, elapsed, exit code
```

Nothing runs until `slurmctld` is up — it's the only component that executes
job code, on a 1 s tick. Binaries can be installed on `PATH` with
`make install PREFIX=~/.local` (default `/usr/local`); the DB lives under
`$XDG_STATE_HOME` either way, so all commands work from any directory.
Set `VSLURM_JOBS=/tmp/fresh.csv` to start from an empty DB (in tests, or to
rerun a demo from scratch).

Chained jobs capture the id with `--parsable`, exactly like scripted real
SLURM:

```bash
idA=$(./sbatch --parsable a.sb)          # prints just: 2
./sbatch --depend=afterok:$idA b.sb      # B runs only if A exits 0
```

## Components

- `sbatch` — parses CLI options and `#SBATCH` directives, inserts PENDING
  rows into the jobs DB. Never executes anything.
- `slurmctld` — the daemon. Once per second it locks the DB, reaps finished
  jobs, enforces time limits, evaluates dependencies and the CPU budget,
  launches PENDING jobs, and purges rows of jobs that finished more than 5
  minutes ago. The only component that executes job code. Run it in a
  terminal: `./slurmctld`, optionally with `--cpus N` to override the CPU
  budget (default: the machine's online core count — `N` may exceed the
  real count to test concurrency on a smaller box), or `./slurmctld --once`
  for a single tick. It prints one line per event — startup, submissions,
  job launch/completion/failure/timeout, cancellations (whether it decided
  them or `scancel`/`srun` did), DB purges — so the terminal it runs in
  shows what the scheduler is doing without per-tick noise.
- `squeue` — reads the DB read-only under lock; `JOBID NAME STATE TIME`.
- `scancel` — marks jobs CANCELLED and SIGTERMs the running process.
- `srun` — submits an inline command line as a PENDING job, then polls the
  DB while streaming the job's output/error files to the terminal and
  exits with the job's exit code. SIGINT/SIGTERM cancel the job (same
  transition scancel makes) and exit 1. Never executes anything itself;
  slurmctld does, so `srun` requires a running scheduler just like `sbatch`.
- `sacct` — accounting view over the same DB: `JobID JobName State
  Elapsed ExitCode` by default, with job/state filters (`-j`, `-s`), a
  custom field list (`-o`/`--format`, `%N` min-width), `-X` (hide array
  elements, keep the aggregate master row), `-n` (no header) and
  `-p`/`-P` (pipe-separated, with/without trailing pipe).

All tools share one jobs DB guarded by `lockf`, which is the whole seam
between the CLIs and the daemon. Its path is `$VSLURM_JOBS` if set, else
`$XDG_STATE_HOME/vslurm/jobs.csv` (defaulting to
`~/.local/state/vslurm/jobs.csv`) — a fixed absolute location, so the
tools behave identically whether run from the repo or installed on `PATH`
from any directory. Job ids come from a monotonic sequence file
(`jobs.seq`) next to the DB. The state directory is created on first use;
`make install PREFIX=...` (default `/usr/local`) copies the six binaries
into `$(PREFIX)/bin`.

Submit-time paths are normalized so nothing depends on the caller's cwd:
relative script paths and `--chdir` are stored absolute (resolved against
the submit directory), while output/error patterns stay relative and are
resolved against each job's `chdir` at launch, exactly like SLURM. Unlike
real SLURM, a missing output directory is created rather than rejected.

## Job arrays

`--array=1-4:2` / `-a 1,3,5` / `-a 1-4%2` (also the bracketed `1-4[%2]`
form) submit one master row plus one row per index. Masters never execute;
they aggregate: the master goes COMPLETED when every element completed,
otherwise FAILED once all elements settle (a cancelled or timed-out
element counts as not-completed); cancelling the master id cancels the
whole array, master included. Elements
display as `<masterid>_<task>` (e.g. `101_3`) in squeue and sacct, exactly
like SLURM. Each element gets its own hidden row id used for `%j`; `%A` is
the master id and `%a` the task index. `SLURM_ARRAY_JOB_ID`,
`SLURM_ARRAY_TASK_ID` and `SLURM_ARRAY_TASK_COUNT` are exported to
elements. The `%N` concurrency limit (`1-4%2`) caps how many elements of
one array run at once. Dependencies may reference the master id (meaning
the whole array) or a single element (`afterok:101_3`); scancel and the
squeue/sacct `-j` filter accept both forms too.

## Scheduling model

Single node. Each RUNNING job consumes `ntasks × cpus-per-task` of the
CPU budget; slurmctld walks PENDING jobs in id order each tick and launches
those that fit in the remaining budget. A job that doesn't fit doesn't
block later, smaller ones (no reservations), but a job whose
`ntasks × cpus` exceeds the whole budget can never start and stays
PENDING forever — size `-n`/`-c` to the budget you run with.

The budget defaults to the machine's online CPU count
(`SC_NPROCESSORS_ONLN`, so hyperthreads count) and can be overridden per
server with `slurmctld --cpus N` — a value larger than the physical core
count oversubscribes the machine, which is exactly what you want when
testing concurrent job workflows on a laptop; a small value (e.g.
`--cpus 2`) forces queueing. Jobs run simultaneously for real: the cap is
a scheduling budget, not a process or cgroup limit, so a single job may
use more CPU than it was allocated. The override is not persisted — it
applies for the lifetime of that slurmctld process only, so restarts and
other servers against the same DB use their own setting.

Time limits (`--time`) have whole-minute granularity: `-t 90` means 90
minutes, `MM:SS` and `HH:MM:SS`/`D-HH:MM:SS` round up to the next whole
minute, and limits are enforced on the scheduler's 1 s tick by SIGKILL,
putting the job in TIMEOUT. Cancelling sends SIGTERM first; slurmctld
escalates to SIGKILL if the process lingers ~5 s. Terminal rows are purged
from the DB 5 minutes after they finish, so history in squeue/sacct only
covers that window.

## Supported sbatch options

| Option | Short | Notes |
|---|---|---|
| `--job-name` | `-J` | name; `%x` refers to it |
| `--output` | `-o` | default `slurm-%j.out` (`slurm-%A_%a.out` for arrays) |
| `--error` | `-e` | empty = merge into output |
| `--depend` | `-d` | see dependency types below |
| `--time` | `-t` | minutes; `MM:SS`/`HH:MM:SS`/`D-HH:MM:SS` accepted |
| `--ntasks` | `-n` | default 1; feeds the CPU budget only |
| `--cpus-per-task` | `-c` | default 1; feeds the CPU budget only |
| `--chdir` | `-D` | submit dir by default |
| `--array` | `-a` | `1-4:2`, `1,3,5`, optional `%N` concurrency limit |
| `--wrap` | | inline command instead of a script |
| `--parsable` | | print only the job id (no `Submitted batch job` text) |
| `--export` | | accepted; `ALL` is silent, other values warn |

Directives (`#SBATCH ...`) are read from the top of the script until the
first line that is neither blank nor `#`-prefixed; command-line options
are applied afterwards and win, per real sbatch. Script arguments after
the script path are passed through to the job. `--wrap` plus a script
file is an error; neither is a usage error. A nonexistent script is an
error, exactly like sbatch.

Unsupported-but-known options (`account partition qos nodes nodelist mem
mem-per-cpu gres exclusive share begin cluster mail-user mail-type
constraint reservation`) produce `sbatch: warning: unsupported option ...
ignored` on stderr and are otherwise ignored — never fatal. Truly unknown
options warn the same way and do **not** consume a following value token.

Filename pattern specifiers: `%j` this job's id, `%A` array master id,
`%a` task id, `%x` job name, `%t` task rank, `%N` hostname, `%u` user,
`%J` `jobid.stepid`, `%%` literal percent, with optional zero-pad width
(`%3j`); anything else warns and passes through. `%a`/`%A` in a non-array
job expand to 4294967294 (SLURM's `NO_VAL`) and the job id respectively,
matching real sbatch.

Dependency types (comma groups, ALL must be satisfied): `afterok`,
`afternotok`, `afterany`, `after`. Unknown types and non-integer ids warn
at submit and the group is dropped. A dependency referencing an ID that
was never allocated fails the job rather than pending forever; a
dependency on an ID that finished long ago and was purged counts as
satisfied. Two real-SLURM forms are unsupported and their group is dropped
at submit, so the job runs **immediately** rather than waiting:
`singleton`, and the optional/any-of `?` separator (`afterok:1?afterany:2`,
including a trailing `?` on an id). Port such scripts to explicit id
chains via `--parsable`. Like real SLURM (its default
`kill_invalid_depend` path), a dependency that can never be satisfied —
an `afterok` job whose dependency terminated not-completed (FAILED,
CANCELLED, TIMEOUT), or an `afternotok` job whose dependency COMPLETED —
cancels the dependent job: it leaves the queue as CANCELLED and never
runs.

## The other tools, precisely

- `squeue` — `-h` (no header) and `-j <id>[,<id>...]` (accepts master ids
  and `master_task` element ids, `=` attached or separate). Fixed columns
  `JOBID NAME STATE TIME`; name truncated to 19 chars, id to 14. States
  print as SLURM abbreviations: `PD R CD F CA TO`. Any other option is
  silently ignored — there is no `-o`/`--format`, `-u`, `-p`, `-l`,
  NODELIST column or queue REASON.
- `scancel <jobid...>` — plain ids, array master ids (cancels the whole
  array) or `master_task` element ids. Running jobs are SIGTERMed and
  marked CANCELLED; already-terminal jobs are left alone. Unknown ids
  print `scancel: error: Job N not found` and exit 1. There is no signal
  choice, `--name`, `--state` or `--user`.
- `srun [options] <command> [args...]` — supports `-J/-o/-e/-t/-n/-c/-D/-d`
  with sbatch semantics (`-v` is accepted silently). The command line is
  submitted as one job; srun streams the job's output/error files to the
  terminal and exits with the job's exit code (TIMEOUT/CANCELLED → 1, with
  a message on stderr). SIGINT/SIGTERM cancel the job. `--wait` and the
  allocation/binding options (`-N -m -p -q ...`) warn and are ignored.
- `sacct` — options `-j` (job filter), `-s` (state filter; full names or
  the `PD/R/CD/F/CA/TO` abbreviations), `-o/--format` (comma list with
  `%N` min-width), `-n`, `-p`/`-P`, `-X` (array masters only); `-a -u -S -E`
  are accepted and ignored (single user, single node). Unknown options are
  a hard error, unlike sbatch/srun. Available fields: `JobID JobIDRaw
  JobName State Elapsed ExitCode Start End Submit AllocCPUS NCpus CPUs
  NTasks Timelimit WorkDir ArrayJobID ArrayTaskID`. States print with the
  exit code, e.g. `COMPLETED(0)` / `FAILED(3)`.

## Environment exported to jobs

`SLURM_JOB_ID`, `SLURM_JOB_NAME`, `SLURM_SUBMIT_DIR`, `SLURM_NTASKS`,
`SLURM_CPUS_PER_TASK`, `SLURM_JOB_DEPENDENCY` when set, plus
`SLURM_ARRAY_JOB_ID`/`SLURM_ARRAY_TASK_ID`/`SLURM_ARRAY_TASK_COUNT` for
array elements.

**Caveat:** jobs inherit the *scheduler's* environment, not the
submitter's. `FOO=1 sbatch job.sb` does not put `FOO` in the job —
`--export` has no effect. To get variables into jobs, set them in the sb
script or start slurmctld with the environment you want.

## DB schema

21 columns, header row always present:
`jobid,state,name,submit,start,end,pid,exitcode,minutes,ntasks,cpus,dep,
output,error,chdir,script,args,wrap,arrayid,arraytask,arraylimit`.
`script` and `args` store the submitted script and its argument list
(quoted when needed); `wrap` holds a `--wrap` payload instead. Empty int
cells (pid, exitcode, minutes, arrayid, ...) read back as unset.
Cells containing `,`/`"`/newlines are double-quote escaped by the shared
reader/writer.

## SLURM features that are NOT supported

Everything below is outside vslurm's goal of simulating sb-file workflows;
don't rely on it for these. Options marked *ignored* print a warning (or
are silently ignored by squeue) and have no effect on scheduling or
accounting.

**Clusters and resources**
- Multi-node execution: `--nodes`/`-N` and `--nodelist` ignored; there is
  exactly one implicit node and no `sinfo`, `scontrol`, `sattach` or
  `sbcast`.
- Memory (`--mem`, `--mem-per-cpu`), GRES/GPU (`--gres`), licenses,
  `--constraint`, features: ignored — jobs are never limited or rejected
  by memory or devices.
- Partitions, QOS, accounts, reservations, priorities, preemption,
  fairshare, backfill-with-reservation: none exist. `--partition`,
  `--qos`, `--account`, `--reservation` ignored; jobs launch FIFO by id
  within the single CPU budget.
- Multiple users: no authentication, uid tracking, or per-user limits
  (`-u`/`--user` filters are no-ops).

**Jobs and steps**
- Job steps: none. `--ntasks` only feeds the CPU budget; a job is always
  one process tree. No `--mpi`, `--cpu-bind`, `--mem-bind`, `--hint`
  (srun warns on these).
- `--begin` (deferred start): ignored — the job is eligible immediately.
- Requeue/restart: `--requeue`, `scontrol requeue`, checkpointing: absent.
  After a `slurmctld` restart, jobs that were running under the old process
  are adopted by PID; when they die their exit codes are unknowable and
  recorded as empty (shown as COMPLETED with no exit code).
- Mail (`--mail-user`/`--mail-type`), federation (`--cluster`), burst
  buffers, specialized cores: ignored.
- `--exclusive`/`--share`: ignored; the CPU budget is the only packing
  rule.

**Interactive use**
- No pseudo-terminal (`--pty`), and stdin is not forwarded: jobs run
  detached, so anything that reads the terminal will see EOF/hang. srun
  output is streamed by polling files (~200 ms), so stdout/stderr
  interleaving is not byte-exact.

**Interface divergences from real SLURM**
- Unknown sbatch options do not consume the next argv token (documented
  divergence).
- `--array` ids always get fresh row ids rather than SLURM's contiguous
  block; display and dependency semantics are preserved.
- `SLURM_JOB_ID` inside an array element is the element's own row id, not
  the master id (real SLURM exports the master id); use
  `SLURM_ARRAY_JOB_ID`.
- `SLURM_ARRAY_TASK_COUNT` reflects the number of array elements currently
  in the DB for the master, not the submitted count (purged elements no
  longer count).
- `--parsable` prints the bare job id only (no `;cluster` suffix — there
  is no federation); for arrays it prints the master id. It is
  sbatch-only; `srun` blocks and returns the exit code, so it has no id to
  print.
- History is bounded: terminal jobs are purged 5 minutes after finishing,
  so sacct is a window onto recent activity, not a persistent database.
- Timeouts SIGKILL immediately (no SIGTERM grace period like SLURM's
  `KillWait`).
- A running job killed by something outside vslurm is recorded FAILED
  with exit code 128+signal (e.g. `FAILED(137)`), not as a node failure.

## Tests

`make check` type-checks every tool; `make` builds all six binaries.

`bash tests/e2e.sh` builds everything and runs 18 acceptance checks in a
temp dir: basic submit/run/output/exit-code, directive-driven naming,
dependency chains, failure + `afternotok`/`afterok` gating, timeouts,
arrays (files, env, squeue filters, master deps, element deps, element
cancel, `%N` limit), cancellation, the CPU-budget cap and the
`slurmctld --cpus` override, srun run/exit-code/files/cancel, and sacct
field selection plus `-j`/`-s` filtering.

`bash tests/depend.sh` exercises dependency semantics in isolation: an
`afterok` job that runs after its dependency succeeds, and dependent jobs
that are CANCELLED when their dependency can never be satisfied — both
directions (`afterok` on a FAILED job, `afternotok` on a COMPLETED one),
matching real SLURM's `kill_invalid_depend` behavior.

`bash tests/install.sh` installs to a temp `PREFIX`, puts the binaries on
`PATH` with `VSLURM_JOBS` unset, and verifies installed usage from
unrelated directories: relative script/`-D`/output paths, the shared
XDG-state DB across all tools, and srun/scancel agreement.
