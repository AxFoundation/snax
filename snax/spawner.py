import getpass
import subprocess
import time
from datetime import datetime

import boto3

QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/771312461418/snax'

def send(dataset='170505_0309'):
    # Create SQS client
    sqs = boto3.resource('sqs')

    queue = sqs.get_queue_by_name(QueueName='snax')

    print(len(queue))


def queue_dali():
    subprocess.getoutput('sbatch ~/blah.sh')


def queue_xenon1t():
    subprocess.getoutput('sbatch ~/blah_xenon1t.sh')

def spawn(partition):
    if partition == 'xenon1t':
        queue_xenon1t()
    else:
        queue_dali()

def queue_state(partition, state='pending'):
    username = getpass.getuser()
    ids = subprocess.getoutput(f'squeue --user {username} --state {state} --partition {partition} --format %%A')
    ids = ids.split()[1:] # 0 is header                                                                                                                    
    return len(ids)


def main(spawn_threshold=10, sleep=5, partition='dali'):
    sqs = boto3.resource('sqs')

    queue = sqs.get_queue_by_name(QueueName='snax')

    while 1:

        n = int(queue.attributes.get('ApproximateNumberOfMessages'))

        n_pending = queue_state(partition=partition)
        n_running = queue_state(partition=partition, state='running')

        print('Running ', str(datetime.utcnow()))
        print(f'\tSQS Queue Size {n}')
        print(f'\tRunning batch queue {n_running}')
        print(f'\tPending batch queue {n_pending}')

        if n > spawn_threshold and n_pending < 2 and n_running < 50:
            print('\tSpawn')
            spawn(partition)
        else:
            print('\tWait')
        print('\tSleeping')
        time.sleep(sleep)

if __name__ == "__main__":
    import sys
    partition = sys.argv[1]
    main(partition=partition)

