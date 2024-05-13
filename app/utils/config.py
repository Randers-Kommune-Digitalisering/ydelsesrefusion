import os
from dotenv import load_dotenv

load_dotenv()

SFTP_HOST = os.environ['SFTP_HOST']
SFTP_USER = os.environ['SFTP_USER']
SSH_KEY_BASE64 = os.environ['SSH_KEY_BASE64']
SSH_KEY_PASS = os.environ['SSH_KEY_PASS']
REMOTE_DIR = 'IN'
DEBUG = os.getenv('DEBUG', False)
POD_NAME = os.getenv('POD_NAME', 'Pod name not set')

CUSTOM_DATA_CONNECTOR_HOST = os.environ['CUSTOM_DATA_CONNECTOR_HOST']