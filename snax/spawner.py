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
        time.sleep(60)
        n = queue.attributes.get('ApproximateNumberOfMessages')
        print('num', n)

        ids = subprocess.getoutput(f'squeue --user {username} --state pending --format %A')
        ids = ids.split()[1:] # 0 is header

        if n > spawn_threshold and len(ids) < 2:
            spawn()

if __name__ == "__main__":
    main()
