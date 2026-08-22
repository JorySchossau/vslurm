import subprocess
import time
from os import path

file = "/home/jory/projects/vslurm/tests/log.txt"
re = subprocess.run(
    [
        "sbatch",
        #f"--wrap=echo 'line 1' > {file}\necho 'line 2' >> {file}\n",
        f"--wrap=#!/usr/bin/bash --login\necho 'Hello World' > {file}\necho 'line 2' >> {file}\n",
    ],
)

time.sleep(2)  # wait for vsched's next tick to launch the job
print("log.txt contents")
print("---")
if not path.isfile(file):
    print("(file does not exist)")
    quit(0)

with open(file, 'rt') as f:
    print(f.read())

