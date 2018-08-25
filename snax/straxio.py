# -*- coding: utf-8 -*-

import tempfile
import subprocess
import os
import stat
from jobqueue import get_messages_from_queue
import strax

import strax
strax.mailbox.MAILBOX_TIMEOUT = 60*10
strax.mailbox.MAILBOX_MAX_MESSAGES = 3

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

def convert(dataset):
    temporary_directory = tempfile.TemporaryDirectory(prefix='/dali/lgrandi/tunnell/temp/') 
    name = temporary_directory.name

    strax.mailbox.MAILBOX_TIMEOUT = 60 * 60
    strax.mailbox.MAILBOX_MAX_MESSAGES = 3
    
    st = strax.Context(storage=[ strax.SimpleS3Store(),
                                 #strax.DataDirectory('/dali/lgrandi/tunnell/strax', readonly=True)
                             ],
                       register_all=strax.xenon.plugins,
                       config={'pax_raw_dir' : name + '/'})

    #strax.xenon.pax_interface.RecordsFromPax.save_when = strax.SaveWhen.EXPLICIT

    st.register(strax.xenon.pax_interface.RecordsFromPax)

    try:
        st.make(dataset, 'event_info')#, max_workers=2)
    except:
        temporary_directory = download(dataset, temporary_directory)
        st.make(dataset, 'event_info')

    temporary_directory.cleanup()

def loop():
    for i, message in enumerate(get_messages_from_queue()):
        if i == 1:
            break
        dataset = message['Body']

        print(f'Working on {dataset}')
        try:
            convert(dataset)
        except FileNotFoundError as ex:
            print(f'Fail {dataset} {ex}')

        




if __name__ == "__main__":
    loop() #convert("170428_0804") # #loop()
