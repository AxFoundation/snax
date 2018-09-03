import boto3
import pymongo
import os

QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/771312461418/snax'

def send(dataset='170505_0309'):
    # Create SQS client
    sqs = boto3.resource('sqs')

    queue = sqs.get_queue_by_name(QueueName='snax')

    # Send message to SQS queue
    response = queue.send_message(MessageBody=dataset)
    print(response)

def get_messages_from_queue(queue_url=QUEUE_URL):
    """Generates messages from an SQS queue.

    Note: this continues to generate messages until the queue is empty.
    Every message on the queue will be deleted.

    :param queue_url: URL of the SQS queue to drain.

    """
    sqs_client = boto3.client(aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
                               aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
                               service_name='sqs')

    while True:
        resp = sqs_client.receive_message(
            QueueUrl=queue_url,
            AttributeNames=['All'],
            MaxNumberOfMessages=1,
            
        )

        try:
            yield from resp['Messages']
        except KeyError:
            return

        entries = [
            {'Id': msg['MessageId'], 'ReceiptHandle': msg['ReceiptHandle']}
            for msg in resp['Messages']
        ]

        resp = sqs_client.delete_message_batch(
            QueueUrl=queue_url, Entries=entries
        )

        if len(resp['Successful']) != len(entries):
            raise RuntimeError(f'Failed to delete messages: entries={entries} resp={resp}')
            


def main():
    c = pymongo.MongoClient('mongodb://pax:%s@zenigata.uchicago.edu:27017/run' % os.environ.get('MONGO_PASSWORD'))
    collection = c['run']['runs_new']
    for doc in collection.find({'tags.name' : '_sciencerun1', 'detector' : 'tpc',
                                'data.location' : {'$regex' : 'x1t_SR001_.*'},
                                'data.rse' : {'$elemMatch' : {'$eq' : 'UC_OSG_USERDISK'}},
                                #'number' : {'$gt' : 11997}
                                },
                               projection = {'name' : 1, 'number' : 1},
        #                           limit = 1000
    ):
        send(doc['name'])
    #send()
    #for message in get_messages_from_queue(QUEUE_URL):
    #    print('message', message['Body'])



if __name__ == "__main__":
    main()
