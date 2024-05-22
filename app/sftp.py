import os
import base64
import pysftp
import fnmatch
import warnings
import io
import pandas as pd
import datetime
from datetime import timedelta

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
    
    logger.info('-ALLE FILER-----------------------------------------')
    logger.info(filelist)
    
    # Beholder kun seneste fil 
    #filelist = [filelist[-1]]
    return filelist, sftp


def handle_files(files, connection):

    # Finder relevante filer ud fra dato på seneste fil - går pt 2 år tilbage
    senesteFil=files[-1]
    dato=datetime.date(int(senesteFil[-14:-10]), int(senesteFil[-9:-7]), 1)
    maxDato=dato-timedelta(days=1)
    minDato=datetime.date(dato.year-2,(dato.month % 12) + 1, dato.day)

    logger.info('-PERIODEAFGRÆNSNING---------------------------------')
    logger.info(f'Første dag i måneden i seneste data: {dato}')
    logger.info(f'Dataperiode: {minDato} - {maxDato}')
    logger.info('-LOOP START-----------------------------------------')

    # Definerer tom dataframe
    df_appended=pd.DataFrame()

    for filename in files:
        with connection.open(os.path.join(REMOTE_DIR, filename).replace("\\", "/")) as f:

            # Indlæser filen hvis datoen på filen er sammenfaldende eller ligger før minDato
            datoCurrentFile=datetime.date(int(filename[-14:-10]), int(filename[-9:-7]), 1)
            if datoCurrentFile>=minDato:
                logger.info(filename)
                logger.info('----------------------------------------------------')
                
                # Indlæser csv
                df = pd.read_csv(f,sep=";",header=0,decimal=",")  

                # Fjerner overflødige kolonner
                df = df[['Uge','CPR nummer', 'Ydelse','Beregnet udbetalingsbeløb','Refusionssats','Refusionsbeløb','Medfinansieringssats','Medfinansieringsbeløb']]
                #logger.info(df.dtypes)

                # Dropper Fleksbidrag fra staten - korrekt?
                før=df.shape[0]
                df = df[df['Ydelse']!='Fleksbidrag fra staten']
                efter=df.shape[0]
                logger.info(f'Dropper Fleksbidrag fra staten: {før} => {efter} ({efter-før})')

                # Dropper dubletter
                før=df.shape[0]
                df = df.drop_duplicates()
                efter=df.shape[0]
                logger.info(f'Dropper eventuelle dubletter: {før} => {efter} ({efter-før})')

                # Konverterer ugenr til datetime 
                df['Uge'] = pd.to_datetime(df['Uge'], format="%Y-%m-%d").dt.date

                #  Konverterer relevante kolonner til numerisk - REDUNDANT
                #kolonner=['Indberetning udbetalingsbeløb','Beregnet udbetalingsbeløb','Refusionssats','Refusionsbeløb','Medfinansieringssats','Medfinansieringsbeløb']
                #df[kolonner] = df[kolonner].apply(pd.to_numeric, errors='coerce', axis=1)
                #logger.info(df.dtypes)

                # Beholder data ældre end minDato
                før=df.shape[0]
                df = df[df['Uge']>=minDato]
                efter=df.shape[0]
                logger.info(f'Dropper data før {minDato}: {før} => {efter} ({efter-før})')
                
                # Sætter data sammen til een DataFrame
                df_appended=pd.concat([df_appended, df], ignore_index=True)

                # df = df.groupby(['Uge','Ydelse'])['CPR nummer'].count().to_frame().reset_index()

    logger.info('-LOOP SLUT------------------------------------------')
    # Summerer 'Refusionsbeløb' og 'Medfinansieringsbeløb' for hver Uge x CPR Nummer x Ydelse.
    før=df_appended.shape[0]
    df1 = df_appended.groupby(['Uge','CPR nummer','Ydelse'])[['Beregnet udbetalingsbeløb','Refusionsbeløb','Medfinansieringsbeløb']].sum().reset_index()
    efter=df1.shape[0]
    logger.info(f'Grupperer data efter uge, cpr og ydelse {før} => {efter} ({efter-før})')

    # Dropper observationer med et summeret udbetalingsbeløb på 0
    før=df1.shape[0]
    df1 = df1[df1['Beregnet udbetalingsbeløb']!=0]
    efter=df1.shape[0]
    logger.info(f'Dropper data med summeret udbetalingsbeløb på 0: {før} => {efter} ({efter-før})')

    før=df1.shape[0]
    df1 = df1.groupby(['Uge','Ydelse']).agg(Antal=('CPR nummer', 'count'), Total=('Beregnet udbetalingsbeløb', 'sum'),Refusion=('Refusionsbeløb','sum'),Medfinansiering=('Medfinansieringsbeløb','sum')).reset_index()
    efter=df1.shape[0]
    logger.info(f'Aggregerer data på uge og ydelsesniveau: {før} => {efter} ({efter-før})')

    # Begrænser alle numeriske værdier til 2 decimaler 
    kolonner=['Total','Refusion','Medfinansiering']
    df1[kolonner] = df1[kolonner].round(2)

    # Skriver til Custom Data 
    data=io.BytesIO(df1.to_csv(index=False, sep=';').encode('utf-8'))    
    post_to_custom_data_connector("SAYdelsesrefusion", data.getbuffer())
    logger.info(f'Opdateret via CDC: {"SAYdelsesrefusion"}')

    #df1.to_csv("test.csv", index=False)
