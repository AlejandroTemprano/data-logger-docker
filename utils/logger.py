import logging
import os
import sys


def setup_logger(debug_level='DEBUG', name='root', save_on_file=True, clear_files_on_start=True, folder_path='.//', file_name ='logs.log'):

    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, debug_level))

    if save_on_file:
        os.makedirs(folder_path, exist_ok=True)

        file_path = f'.//{folder_path}//{file_name}'
        if not os.path.exists(file_name) or clear_files_on_start:
            with open(file_path, 'w') as logfile:
                logfile.truncate()

        handler = logging.FileHandler(file_path)
        handler.setFormatter(logging.Formatter('%(asctime)s - [%(levelname)s] - %(threadName)s - %(funcName)s >> %(message)s'))

        logger.addHandler(handler)

    handler = logging.StreamHandler(sys.stdout) 
    handler.setFormatter(logging.Formatter('%(asctime)s - [%(levelname)s] >> %(message)s', "%H:%M:%S"))

    logger.addHandler(handler)

    return logger