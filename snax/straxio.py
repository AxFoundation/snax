# -*- coding: utf-8 -*-

import datetime
import os
import socket
import stat
import subprocess
import tempfile

import pymongo
import strax
from jobqueue import get_messages_from_queue, error

CONNECTION = pymongo.MongoClient(
    f'mongodb://queue_inserter:{os.environ.get("MONGO_JOB_PASSWORD")}@rundbcluster-shard-00-00-cfaei.gcp.mongodb.net:27017,rundbcluster-shard-00-01-cfaei.gcp.mongodb.net:27017,rundbcluster-shard-00-02-cfaei.gcp.mongodb.net:27017/xenon1t?ssl=true&replicaSet=RunDBCluster-shard-0&authSource=admin&retryWrites=true')
COLLECTION = CONNECTION['xenon1t']['workers']


def download(dataset, temporary_directory):
    script = f"""#!/bin/bash
export RUCIO_ACCOUNT=xenon-analysis
export X509_USER_PROXY=/project/lgrandi/xenon1t/grid_proxy/xenon_service_proxy
source /cvmfs/xenon.opensciencegrid.org/software/rucio-py27/setup_rucio_1_8_3.sh
source /cvmfs/oasis.opensciencegrid.org/osg-software/osg-wn-client/3.4/current/el7-x86_64/setup.sh
cd {temporary_directory.name}
rucio download x1t_SR001_{dataset}_tpc:raw --rse UC_OSG_USERDISK --ndownloader 5
"""  # --rse UC_OSG_USERDISK

    file = tempfile.NamedTemporaryFile(suffix='.sh', delete=False)
    name = file.name
    file.write(script.encode())
    file.close()

    os.chmod(name, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
    subprocess.call(name)

    os.unlink(name)

    print(temporary_directory.name)

    raw_dir = os.path.join(temporary_directory.name,
                           'raw')

    if not os.path.isdir(raw_dir):
        raise FileNotFoundError(f'Downloaded data, but could not find {raw_dir}')

    os.unlink(os.path.join(raw_dir, 'trigger_monitor_data.zip'))

    os.rename(os.path.join(temporary_directory.name,
                           'raw'),
              os.path.join(temporary_directory.name,
                           dataset))

    return temporary_directory


BUCKET_NAME = 'snax_s3_v2'


def remove(s3, dataset):
    objects = s3.list_objects(Bucket=BUCKET_NAME,
                              Prefix=dataset)
    if 'Contents' not in objects:
        return

    print('Purge', dataset)

    objects2 = [{'Key': obj['Key']} for obj in objects['Contents']]

    print('deleting %d objects' % len(objects['Contents']))
    s3.delete_objects(Bucket=BUCKET_NAME,
                      Delete={'Objects': objects2})


def convert(dataset):
    temporary_directory = tempfile.TemporaryDirectory(prefix='/dali/lgrandi/tunnell/temp/')
    name = temporary_directory.name

    strax.Mailbox.DEFAULT_TIMEOUT = 600

    st = strax.Context(storage=[
        # strax.SimpleS3Store(),
        strax.DataDirectory(path="/dali/lgrandi/tunnell/strax_data"),
        strax.SimpleS3Store(readonly=True),
    ],
        register_all=strax.xenon.plugins,
        config={'pax_raw_dir': name + '/'})
    st.register(strax.xenon.pax_interface.RecordsFromPax)

    # client = st.storage[0].s3
    # client.create_bucket(Bucket=BUCKET_NAME)

    dtype = 'records'

    meta = {}
    try:
        print('gett meta')
        meta = st.get_meta(dataset, dtype)
        print('got meta')
    except strax.DataNotAvailable:
        temporary_directory = download(dataset, temporary_directory)
        print('running make')
        st.make(dataset, dtype)
        print('made')
    except Exception as e:
        print('STRAXFAIL EXCEPTION', str(e))
        # remove(client, dataset)
        raise
        # temporary_directory = download(dataset, temporary_directory)
        # st.make(dataset, 'dtype')

    temporary_directory.cleanup()


def loop():
    result = COLLECTION.insert_one({'host': socket.gethostname(),
                                    'startTime': datetime.datetime.utcnow(),
                                    'endTime': None,
                                    'run': None,
                                    'runStart': None,
                                    })

    for i, doc in enumerate(get_messages_from_queue()):

        COLLECTION.find_one_and_update({'_id': result.inserted_id},
                                       {'$set': {'runStart': datetime.datetime.utcnow(),
                                                 'run': doc['payload']['number']}})

        dataset = doc['payload']['name']

        print(f"Working on {doc['payload']['number']}")
        try:
            convert(dataset)
        except Exception as ex:
            error(doc, str(ex))
            end_workeer(result)
            raise

    end_workeer(result)


def end_workeer(result):
    COLLECTION.find_one_and_update({'_id': result.inserted_id},
                                   {'$set': {'endTime': datetime.datetime.utcnow(),
                                             'run': None}})


if __name__ == "__main__":
    #    convert("170325_1714")
    loop()  # convert("170428_0804") # #loop()
