# -*- coding: utf-8 -*-

import datetime
import multiprocessing
import os
import stat
import subprocess
import tempfile
import time

import strax

from .jobqueue import get_messages_from_queue
from .rundb import error, send_heartbeat, runs_data_initialize
from .rundb import init_worker, update_worker, end_worker
from .spawner import HOURS


def mongodb_keep_alive(inserted_id):
    while 1:
        print('Heartbeat')
        send_heartbeat(inserted_id)
        time.sleep(60)




def download(dataset, temporary_directory):
    script = f"""#!/bin/bash
export RUCIO_ACCOUNT=xenon-analysis
export X509_USER_PROXY=/project/lgrandi/xenon1t/grid_proxy/xenon_service_proxy
source /cvmfs/xenon.opensciencegrid.org/software/rucio-py27/setup_rucio_1_8_3.sh
source /cvmfs/oasis.opensciencegrid.org/osg-software/osg-wn-client/3.4/current/el7-x86_64/setup.sh
cd {temporary_directory.name}
rucio download x1t_SR001_{dataset}_tpc:raw --rse UC_OSG_USERDISK --ndownloader 2
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


def convert(host, dataset, dtype='records'):
    temporary_directory = tempfile.TemporaryDirectory(prefix='/dali/lgrandi/tunnell/temp/')
    name = temporary_directory.name

    strax.Mailbox.DEFAULT_TIMEOUT = 600

    data_directory = "/dali/lgrandi/tunnell/strax_data"

    st = strax.Context(storage=[
        # strax.SimpleS3Store(),
        strax.DataDirectory(path=data_directory),
        strax.SimpleS3Store(readonly=True),
    ],
        register_all=strax.xenon.plugins,
        config={'pax_raw_dir': name + '/'})
    st.register(strax.xenon.pax_interface.RecordsFromPax)

    # client = st.storage[0].s3
    # client.create_bucket(Bucket=BUCKET_NAME)

    try:
        st.get_meta(dataset, dtype)
    except strax.DataNotAvailable:
        temporary_directory = download(dataset, temporary_directory)

        file_location = os.path.join(st.storage[0].path,
                                     str(st._key_for(dataset, dtype)))
        meta = st.get_meta(dataset, dtype)

        data_doc = {"host": host,
                    "status": "transferred",
                    "type": meta['data_type'],
                    "checksum": None,
                    "creation_time": datetime.datetime.utcnow(),
                    'lineage_hash': strax.deterministic_hash(meta['lineage']),
                    'meta': meta
                    }

        if 'dali' in host:
            data_doc['location'] = file_location
            data_doc['protocol'] = 'FileSytemBackend'

        st.make(dataset, dtype)
        runs_data_initialize(dataset, data_doc)

    temporary_directory.cleanup()


def loop(host):
    start_time = datetime.datetime.utcnow()
    time_limit = datetime.timedelta(hours=(HOURS / 2))

    inserted_id = init_worker()

    # Send mongo heartbeat for worker
    p = multiprocessing.Process(target=mongodb_keep_alive,
                                args=(inserted_id,))
    assert p.is_alive() is False
    p.start()
    assert p.is_alive() is True

    for i, doc in enumerate(get_messages_from_queue()):
        # TODO Hack, remove me 2018
        if 'dtype' not in doc:
            doc['dtype'] = 'records'

        update_worker(inserted_id, doc['payload']['number'])

        print(f"Working on {doc['payload']['number']}")
        try:
            convert(host, doc['payload']['name'], doc['dtype'])
        except BaseException as ex:
            end_worker(inserted_id)
            p.terminate()
            p.join()
            error(doc, str(ex))
            raise

        if datetime.datetime.utcnow() > start_time + time_limit:
            print("Time limit reached")
            break

    # Stop heartbeat
    assert p.is_alive() is True
    p.terminate()
    p.join()
    assert p.is_alive() is False

    end_worker(inserted_id)

if __name__ == "__main__":
    import sys

    host = sys.argv[1]  # e.g. dali
    loop(host)
