import datetime
import getpass
import subprocess
import sys
import tempfile
import time

from .rundb import processing_count

CPUS = 10
HOURS = 24
TIME = '%d:00:00' % HOURS
MEM = 2000
ENV = 'strax_dev'


def queue_dali():
    f = tempfile.NamedTemporaryFile(delete=False)
    f.write(f"""#!/bin/bash
#SBATCH --job-name=strax
#SBATCH --ntasks=1
#SBATCH --cpus-per-task={CPUS}
#SBATCH --time={TIME}
#SBATCH --partition=dali
#SBATCH --account=pi-lgrandi
#SBATCH --qos=dali
#SBATCH --output=/dali/lgrandi/tunnell/strax_logs/strax_%j_std.log
#SBATCH --error=/dali/lgrandi/tunnell/strax_logs/strax_%j_err.log
#SBATCH --mem-per-cpu={MEM}
source activate {ENV}
python -m snax.straxio dali
        """.encode())
    f.close()
    subprocess.getoutput(f'sbatch {f.name}')


def queue_xenon1t():
    f = tempfile.NamedTemporaryFile(delete=False)
    f.write(f"""#!/bin/bash
#SBATCH --job-name=strax
#SBATCH --ntasks=1
#SBATCH --cpus-per-task={CPUS}
#SBATCH --time={TIME}
#SBATCH --partition=xenon1t
#SBATCH --account=pi-lgrandi
#SBATCH --output=/dali/lgrandi/tunnell/strax_logs/strax_%j_std.log
#SBATCH --error=/dali/lgrandi/tunnell/strax_logs/strax_%j_err.log
#SBATCH --mem-per-cpu={MEM}
source activate {ENV}
python -m snax.straxio dali
    """.encode())
    f.close()
    subprocess.getoutput(f'sbatch {f.name}')


def spawn(partition):
    if partition == 'xenon1t':
        queue_xenon1t()
    else:
        queue_dali()


def queue_state(partition, state='pending'):
    username = getpass.getuser()
    ids = subprocess.getoutput(f'squeue --user {username} --state {state} --partition {partition} --format %%A')
    ids = ids.split()[1:]  # 0 is header
    return len(ids)


def main(spawn_threshold=10, sleep=20, partition='dali', n_running_max=5000):
    while 1:
        n = processing_count()

        n_pending = queue_state(partition=partition)
        n_running = queue_state(partition=partition, state='running')

        print('Running ', str(datetime.datetime.utcnow()))
        print(f'\tQueue Size {n}')
        print(f'\tRunning batch queue {n_running}')
        print(f'\tPending batch queue {n_pending}')

        if n > spawn_threshold and n_pending < 2 and n_running < n_running_max:
            print('\tSpawn')
            spawn(partition)
        else:
            print('\tWait')

        print('\tSleeping')
        time.sleep(sleep)


if __name__ == "__main__":
    partition = 'dali'
    if len(sys.argv) > 1:
        partition = sys.argv[1]
    main(partition=partition)
