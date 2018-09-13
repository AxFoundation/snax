# -*- coding: utf-8 -*-

import os
import stat
import subprocess
import tempfile
from threading import Timer

import strax

from .jobqueue import get_messages_from_queue
from .rundb import error, send_heartbeat
from .rundb import init_worker, update_worker, end_worker


class RepeatingTimer(Timer):
    def run(self):
        while not self.finished.is_set():
            self.function(*self.args, **self.kwargs)
            self.finished.wait(self.interval)

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

    # Send mongo heartbeat for worker
    t = RepeatingTimer(60, send_heartbeat())
    t.start()

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

    temporary_directory.cleanup()

    t.stop()  # Stop heartbeat

def loop():
    inserted_id = init_worker()

    for i, doc in enumerate(get_messages_from_queue()):
        update_worker(inserted_id, doc['payload']['number'])

        print(f"Working on {doc['payload']['number']}")
        try:
            convert(doc['payload']['name'])
        except BaseException as ex:
            end_worker(inserted_id)
            error(doc, str(ex))
            raise

    end_worker(inserted_id)





if __name__ == "__main__":
    #    convert("170325_1714")
    loop()  # convert("170428_0804") # #loop()
