import datetime
import os

import pymongo

CONNECTION = pymongo.MongoClient(
    f'mongodb://queue_inserter:{os.environ.get("MONGO_QUEUE_PASSWORD")}@rundbcluster-shard-00-00-cfaei.gcp.mongodb.net:27017,rundbcluster-shard-00-01-cfaei.gcp.mongodb.net:27017,rundbcluster-shard-00-02-cfaei.gcp.mongodb.net:27017/xenon1t?ssl=true&replicaSet=RunDBCluster-shard-0&authSource=admin&retryWrites=true')
COLLECTION = CONNECTION['xenon1t']['processing_queue']


def send(payload):
    doc = {
        'startTime': None,
        'endTime': None,
        'createdOn': datetime.datetime.utcnow(),
        'priority': 1,
        'error': False,
        'payload': payload
    }

    COLLECTION.insert_one(doc)


def error(doc, msg=""):
    COLLECTION.find_one_and_update(filter={'_id': doc['_id']},
                                   update={'$set': {'error': True,
                                                    'error_msg': msg}},
                                   )


def get_messages_from_queue():
    """Generates messages from an SQS queue.

    Note: this continues to generate messages until the queue is empty.
    Every message on the queue will be deleted.

    :param queue_url: URL of the SQS queue to drain.

    """

    while True:
        doc = COLLECTION.find_one_and_update(filter={'startTime': None},
                                             update={'$set': {'startTime': datetime.datetime.utcnow()}},
                                             sort=[('priority', -1),
                                                   ('createdOn', 1)],
                                             return_document=pymongo.collection.ReturnDocument.AFTER,
                                             )

        if doc is None:
            break
        else:
            yield doc

        doc = COLLECTION.find_one_and_update(filter={'_id': doc['_id']},
                                             update={'$set': {'endTime': datetime.datetime.utcnow()}},
                                             )
        if doc is None:
            raise RuntimeError("Could not find job in queue", doc)


def main():
    index = [('startTime', 1), ('priority', -1), ('createdOn', 1)]

    for index2 in COLLECTION.list_indexes():
        if index2['key'].items() == index:
            break
    else:
        print('create')
        COLLECTION.create_index(index)


    c = pymongo.MongoClient('mongodb://pax:%s@zenigata.uchicago.edu:27017/run' % os.environ.get('MONGO_PASSWORD'))
    collection = c['run']['runs_new']
    for doc in collection.find({'tags.name': '_sciencerun1', 'detector': 'tpc',
                                'data.location': {'$regex': 'x1t_SR001_.*'},
                                'data.rse': {'$elemMatch': {'$eq': 'UC_OSG_USERDISK'}},
                                # 'number' : {'$gt' : 11997}
                                },
                               projection={'name': 1,
                                           'number': 1,
                                           'data.host': 1,
                                           'data.type': 1},
                               skip=400,
                               limit=10,
                               ):
        send(doc)
    # send()
    # for message in get_messages_from_queue(QUEUE_URL):
    #    print('message', message['Body'])


if __name__ == "__main__":
    main()
