import os
from dotenv import load_dotenv

load_dotenv()
cwd=os.getcwd()
BASE_URL = os.getenv("BASE_URL", "https://emea5.ebmpapstneo.io")
if not BASE_URL:
    raise ValueError("BASE_URL environment variable is not set")

DATAFRAME_OUTPUT_PATH = f"{cwd}/DataFrame/"
DATAFRAME_ALL_DEVICES = f"{cwd}/AllDevices/"
DATAFRAME_RANDOM_DEVICES = f"{cwd}/RandomDevices/"




