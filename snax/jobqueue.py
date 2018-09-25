import os

import pymongo

from .rundb import send, fetch, finish, setup


def get_messages_from_queue():
    """Generates messages from an SQS queue.

    Note: this continues to generate messages until the queue is empty.
    Every message on the queue will be deleted.

    :param queue_url: URL of the SQS queue to drain.

    """

    while True:
        doc = fetch()

        if doc is None:
            break
        else:
            yield doc

        doc = finish(doc)

        if doc is None:
            raise RuntimeError("Could not find job in queue", doc)


def main():
    index = [('startTime', 1), ('priority', -1), ('createdOn', 1)]

    setup(index)

    c = pymongo.MongoClient('mongodb://pax:%s@zenigata.uchicago.edu:27017/run' % os.environ.get('MONGO_PASSWORD'))
    collection = c['run']['runs_new']

    for doc in collection.find({'tags.name': '_sciencerun1', 'detector': 'tpc',
                                'data.location': {'$regex': 'x1t_SR001_.*'},
                                'data.rse': {'$elemMatch': {'$eq': 'UC_OSG_USERDISK'}},
                                # 'number' : {'$gt' : 11997}
                                },
                               projection={'name': 1,
                                           'number': 1,
                                           'data' : {'$elemMatch': {'rse': 'UC_OSG_USERDISK'}},
                                           #'data.host': 1,
                                           #'data.type': 1,
                                           #'data.location' : 1
},
                               ):
        doc['dtype'] = 'records'
        send(doc)


if __name__ == "__main__":
    main()
