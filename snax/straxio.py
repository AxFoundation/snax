# -*- coding: utf-8 -*-

import tempfile
import subprocess
import os
import stat
from jobqueue import get_messages_from_queue
import strax

def download(dataset, temporary_directory):
    script = f"""#!/bin/bash
export RUCIO_ACCOUNT=xenon-analysis
export X509_USER_PROXY=/project/lgrandi/xenon1t/grid_proxy/xenon_service_proxy
source /cvmfs/xenon.opensciencegrid.org/software/rucio-py27/setup_rucio_1_8_3.sh
source /cvmfs/oasis.opensciencegrid.org/osg-software/osg-wn-client/3.4/current/el7-x86_64/setup.sh
cd {temporary_directory.name}
rucio download x1t_SR001_{dataset}_tpc:raw --rse UC_OSG_USERDISK
""" # --rse UC_OSG_USERDISK

    file = tempfile.NamedTemporaryFile(suffix='.sh', delete=False)
    name = file.name
    file.write(script.encode())
    file.close()

    os.chmod(name, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR )
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
      
    objects2 = [{'Key' : obj['Key']} for obj in objects['Contents']]
            
    print('deleting %d objects' % len(objects['Contents']))
    s3c.delete_objects(Bucket=BUCKET_NAME,
                       Delete={'Objects' : objects2})

def convert(dataset):
    temporary_directory = tempfile.TemporaryDirectory(prefix='/dali/lgrandi/tunnell/temp/')
    name = temporary_directory.name

    strax.Mailbox.DEFAULT_TIMEOUT = 600
    

    st = strax.Context(storage=[ strax.SimpleS3Store(),
                             ],
                       register_all=strax.xenon.plugins,
                       config={'pax_raw_dir' : name + '/'})
    st.register(strax.xenon.pax_interface.RecordsFromPax)

    client = st.storage[0].s3
    client.create_bucket(Bucket=BUCKET_NAME)

    meta = {}
    try:
        meta = st.get_meta(dataset, 'raw_records')
    except strax.DataNotAvailable:
        temporary_directory = download(dataset, temporary_directory)
        st.make(dataset, 'raw_records')
    except Exception as e:
        print('STRAXFAIL EXCEPTION', str(e))
        remove(client, dataset)
        temporary_directory = download(dataset, temporary_directory)
        st.make(dataset, 'raw_records')

    temporary_directory.cleanup()

def loop():
    for i, message in enumerate(get_messages_from_queue()):
        dataset = message['Body']

        print(f'Working on {dataset}')
        try:
            convert(dataset)
        except FileNotFoundError as ex:
            print(f'SNAXFAIL {dataset} {ex}')
        except Exception as ex:
            print(f'NEWSNAXFAIL {dataset} {ex}')
            raise





if __name__ == "__main__":
    loop() #convert("170428_0804") # #loop()
