import datetime
import os
import socket

import pymongo

PW = os.environ.get("MONGO_JOB_PASSWORD")


def mongo_client(func, *args, **kwargs):
    def inner(*args, **kwargs):
        c = pymongo.MongoClient(
            f'mongodb://worker_queue:{PW}@rundbcluster-shard-00-00-cfaei.gcp.mongodb.net:27017,rundbcluster-shard-00-01-cfaei.gcp.mongodb.net:27017,rundbcluster-shard-00-02-cfaei.gcp.mongodb.net:27017/xenon1t?ssl=true&replicaSet=RunDBCluster-shard-0&authSource=admin&retryWrites=true')
        ret = func(c, *args, **kwargs)
        c.close()
        return ret

    return inner


@mongo_client
def processing_count(client):
    return client['xenon1t']['processing_queue'].count()


@mongo_client
def init_worker(client):
    collection = client['xenon1t']['workers']
    collection.create_index([("heartBeat", 1,), ], expireAfterSeconds=10 * 60)

    result = collection.insert_one({'host': socket.gethostname(),
                                    'startTime': datetime.datetime.utcnow(),
                                    'endTime': None,
                                    'run': None,
                                    'runStart': None,
                                    'heartBeat': datetime.datetime.utcnow(),
                                    })
    return result.inserted_id


@mongo_client
def send_heartbeat(client, inserted_id):
    collection = client['xenon1t']['workers']
    print(inserted_id)
    result = collection.find_one_and_update({'_id': inserted_id},
                                            {'$set': {'heartBeat': datetime.datetime.utcnow()}})
    print(result)


@mongo_client
def update_worker(client, inserted_id, number):
    collection = client['xenon1t']['workers']

    # Otherwise, get different values if call twice.
    now = datetime.datetime.utcnow()

    collection.find_one_and_update({'_id': inserted_id},
                                   {'$set': {'runStart': now,
                                             'run': number,
                                             'heartBeat': now, }})


@mongo_client
def end_worker(client, inserted_id):
    collection = client['xenon1t']['workers']

    # Otherwise, get different values if call twice.
    now = datetime.datetime.utcnow()

    collection.find_one_and_update({'_id': inserted_id},
                                   {'$set': {'endTime': now,
                                             'run': None,
                                             'runStart': None,
                                             'heartBeat': now, }})


@mongo_client
def send(client, payload):
    collection = client['xenon1t']['processing_queue']
    doc = {
        'startTime': None,
        'endTime': None,
        'createdOn': datetime.datetime.utcnow(),
        'priority': 1,
        'error': False,
        'payload': payload
    }

    collection.insert_one(doc)


@mongo_client
def error(client, doc, msg=""):
    collection = client['xenon1t']['processing_queue']
    collection.find_one_and_update(filter={'_id': doc['_id']},
                                   update={'$set': {'error': True,
                                                    'error_msg': msg,
                                                    }},
                                   )


@mongo_client
def finish(client, doc):
    collection = client['xenon1t']['processing_queue']
    return collection.find_one_and_update(filter={'_id': doc['_id']},
                                          update={'$set': {'endTime': datetime.datetime.utcnow()}},
                                          )


@mongo_client
def setup(client, index):
    collection = client['xenon1t']['processing_queue']
    for index2 in collection.list_indexes():
        if index2['key'].items() == index:
            break
    else:
        print('create')
        collection.create_index(index)
    collection.remove({})


@mongo_client
def fetch(client):
    collection = client['xenon1t']['processing_queue']
    return collection.find_one_and_update(filter={'startTime': None},
                                          update={'$set': {'startTime': datetime.datetime.utcnow()}},
                                          sort=[('priority', -1),
                                                ('createdOn', 1)],
                                          return_document=pymongo.collection.ReturnDocument.AFTER,
                                          )


@mongo_client
def runs_data_initialize(client, data_doc, dataset):
    collection = client['xenon1t']['runs']

    return collection.find_one_and_update(filter={'name': dataset},
                                          update={'$push': {'data': data_doc}},
                                          return_document=pymongo.collection.ReturnDocument.AFTER,
                                          )
