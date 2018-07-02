import boto3

QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/771312461418/snax'

def send():
    # Create SQS client
    sqs = boto3.client('sqs')

    # Send message to SQS queue
    response = sqs.send_message(
        QueueUrl=QUEUE_URL,
        DelaySeconds=10,
        MessageAttributes={
            'Title': {
                'DataType': 'String',
                'StringValue': 'The Whistler'
            },
            'Author': {
                'DataType': 'String',
                'StringValue': 'John Grisham'
            },
            'WeeksOn': {
                'DataType': 'Number',
                'StringValue': '6'
            }
        },
        MessageBody=(
            'Information about current NY Times fiction bestseller for '
            'week of 12/11/2016.'
        )
    )

    print(response['MessageId'])


def receive():
    import boto3

    # Create SQS client
    sqs = boto3.client('sqs')

    # Receive message from SQS queue
    response = sqs.receive_message(
        QueueUrl=QUEUE_URL,
        #AttributeNames=[
        #    'SentTimestamp'
        #],
        #MaxNumberOfMessages=1,
        #MessageAttributeNames=[
        #    'All'
        #],
        #VisibilityTimeout=0,
        #WaitTimeSeconds=0
    )

    message = response['Messages'][0]
    receipt_handle = message['ReceiptHandle']

    # Delete received message from queue
    sqs.delete_message(
        QueueUrl=QUEUE_URL,
        ReceiptHandle=receipt_handle
    )
    print('Received and deleted message: %s' % message)

def main():
    #send()
    receive()

main()
