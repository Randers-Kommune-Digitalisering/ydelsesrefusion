import os
import base64
import pysftp
import fnmatch
import warnings
import io
import pandas as pd

from utils.config import SFTP_HOST, SFTP_USER, SSH_KEY_BASE64, SSH_KEY_PASS, REMOTE_DIR
from utils.logging import get_logger
from utils.custom_data_api import post_to_custom_data_connector


logger = get_logger(__name__)
warnings.filterwarnings('ignore', '.*Failed to load HostKeys.*')


def write_key_file(path='.'):
    key_data = base64.b64decode(SSH_KEY_BASE64).decode('utf-8')

    with open(os.path.join(path, 'key'), 'w') as key_file:
        key_file.write(key_data)

    return os.path.abspath(os.path.join(path, 'key'))


def list_all_files():
    # Trust all host keys - bad practice!
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None

    key_path = write_key_file()

    # Get connection
    sftp = pysftp.Connection(host=SFTP_HOST, username=SFTP_USER, private_key=key_path, private_key_pass=SSH_KEY_PASS, cnopts=cnopts)

    # filter away directorires and files without file extensions
    filelist = [f for f in sftp.listdir(REMOTE_DIR) if fnmatch.fnmatch(f, 'yr-ydelsesrefusion-beregning*.csv')]
    
    # Beholder kun seneste fil 
    filelist = [filelist[-1]]
    return filelist, sftp


def handle_files(files, connection):
    for filename in files:
        with connection.open(os.path.join(REMOTE_DIR, filename).replace("\\", "/")) as f:
            df = pd.read_csv(f,sep=";",header=0)  
            df2=df.groupby(['Uge','Ydelse'])['CPR nummer'].count().to_frame().reset_index()
            
            # Skriver til Custom Data (kun en fil for nu)
            data=io.BytesIO(df2.to_csv(index=False, sep=';').encode('utf-8'))
            post_to_custom_data_connector("ØKTestdata", data.getbuffer())

            logger.info(f'Updated {"ØKTestdata"}')