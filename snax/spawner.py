import boto3
import time

QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/771312461418/snax'

def send(dataset='170505_0309'):
    # Create SQS client
    sqs = boto3.resource('sqs')

    queue = sqs.get_queue_by_name(QueueName='snax')

    print(len(queue))

def midway():
    pass#processing_script

def spawn():
    midway()

def main(spawn_threshold=10):
    sqs = boto3.resource('sqs')

    queue = sqs.get_queue_by_name(QueueName='snax')
    print('num', queue.attributes.get('ApproximateNumberOfMessages'))

    while 1:
        time.sleep(60)
        n = queue.attributes.get('ApproximateNumberOfMessages')
        print('num', n)
        if n > 10:
            spawn()



    #for message in get_messages_from_queue(QUEUE_URL):
    #    print('message', message['Body'])

if __name__ == "__main__":
    main()
