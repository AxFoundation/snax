# -*- coding: utf-8 -*-

import tempfile
import subprocess
import os
import stat
from jobqueue import get_messages_from_queue
import strax

def download(dataset='170505_0309'):
    temporary_directory = tempfile.TemporaryDirectory(suffix='/project2/lgrandi/tunnell/temp')

    script = f"""#!/bin/bash
source /cvmfs/xenon.opensciencegrid.org/software/rucio-py26/setup_rucio_1_8_3.sh
export RUCIO_ACCOUNT=xenon-analysis
export X509_USER_PROXY=/project/lgrandi/xenon1t/grid_proxy/xenon_service_proxy
source /cvmfs/xenon.opensciencegrid.org/software/rucio-py27/setup_rucio_1_8_3.sh
source /cvmfs/oasis.opensciencegrid.org/osg-software/osg-wn-client/3.3/current/el7-x86_64/setup.sh
cd {temporary_directory.name}
rucio download x1t_SR001_{dataset}_tpc:raw
    """

#    script = f"""#!/bin/bash
#echo {dataset}
#             """

    file = tempfile.NamedTemporaryFile(suffix='.sh', delete=False)
    name = file.name
    file.write(script.encode())
    file.close()

    os.chmod(name, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR )
    subprocess.call(name)

    os.unlink(name)

    print(temporary_directory.name)

    if not os.path.isdir(os.path.join(temporary_directory.name,
                        'raw')):
        raise FileNotFoundError()

    os.rename(os.path.join(temporary_directory.name,
                           'raw'),
              os.path.join(temporary_directory.name,
                           dataset))

    return temporary_directory

def convert(dataset):
    temporary_directory = download(dataset)
    name = temporary_directory.name
    #name = 'tmp'

    st = strax.Context('/project2/lgrandi/tunnell/strax',
                       register_all=strax.xenon.plugins,
                       config={'pax_raw_dir' : name + '/'})

    strax.xenon.pax_interface.RecordsFromPax.save_when = strax.SaveWhen.EXPLICIT

    st.register(strax.xenon.pax_interface.RecordsFromPax)
    st.make(dataset, 'event_info', max_workers=8)

    temporary_directory.cleanup()

def loop():
    for message in get_messages_from_queue():
        dataset = message['Body']

        print(f'Working on {dataset}')
        convert(dataset)

if __name__ == "__main__":
    loop()
