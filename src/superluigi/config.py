from pathlib import Path
import os


SUPERLUIGI_LOCAL_DATA_DIR = Path(os.getenv('SUPERLUIGI_LOCAL_DATA_DIR', Path(Path.home(), '.superluigi/data')))
SUPERLUIGI_DEFAULT_TASK_CONFIG = Path(os.getenv('SUPERLUIGI_DEFAULT_TASK_CONFIG', Path('./superluigi.cfg')))
