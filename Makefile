NIMFLAGS = --hints:off --warnings:off -d:release

all: squeue sbatch vsched scancel srun sacct

squeue: squeue.nim vslurm_common.nim
	nim c $(NIMFLAGS) -o:$@ $<

sbatch: sbatch.nim vslurm_common.nim
	nim c $(NIMFLAGS) -o:$@ $<

vsched: vsched.nim vslurm_common.nim
	nim c $(NIMFLAGS) -o:$@ $<

scancel: scancel.nim vslurm_common.nim
	nim c $(NIMFLAGS) -o:$@ $<

srun: srun.nim vslurm_common.nim
	nim c $(NIMFLAGS) -o:$@ $<

sacct: sacct.nim vslurm_common.nim
	nim c $(NIMFLAGS) -o:$@ $<

install: all
	mkdir -p $(DESTDIR)$(PREFIX)/bin
	cp squeue sbatch vsched scancel srun sacct $(DESTDIR)$(PREFIX)/bin

check:
	@for f in *.nim; do \
	  nim check --hints:off --warnings:off $$f || exit 1; \
	done

clean:
	rm -f squeue sbatch vsched scancel srun sacct

.PHONY: all install check clean
