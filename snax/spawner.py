import boto3
import time
import subprocess
import getpass

QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/771312461418/snax'

def send(dataset='170505_0309'):
    # Create SQS client
    sqs = boto3.resource('sqs')

    queue = sqs.get_queue_by_name(QueueName='snax')

    print(len(queue))

def dali():
    subprocess.getoutput('sbatch ~/blah.sh')

def dali():
    subprocess.getoutput('sbatch ~/blah_xenon1t.sh')

def spawn():
    dali()

def main(spawn_threshold=10):
    username = getpass.getuser()

    sqs = boto3.resource('sqs')

    queue = sqs.get_queue_by_name(QueueName='snax')
    print('num', queue.attributes.get('ApproximateNumberOfMessages'))

    while 1:

        n = int(queue.attributes.get('ApproximateNumberOfMessages'))

        ids = subprocess.getoutput(f'squeue --user {username} --state pending --format %%A')
        ids = ids.split()[1:] # 0 is header
        n_ids = len(ids)

        print('Running')
        print('\tSQS Queue Size {n}')
        print('\tPending batch queue {n_ids}')

        if n > spawn_threshold and n_ids < 2:
            print('\tSpawn')
            spawn()
        else:
            print('\tWait')
        print('\tSleeping')
        time.sleep(60)

if __name__ == "__main__":
    main()
