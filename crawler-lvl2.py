import logging
import os
import signal
import time
from threading import Thread, Lock

import pandas as pd
import requests
import yaml

from TSEData import TSEData, helper

with open('./config.yaml') as f:
    conf = yaml.safe_load(f)
helper.folder_check(conf['general']['log_path'])
logging.basicConfig(filename=os.path.join(conf['general']['log_path'], 'crawler_lvl2.log'),
                    format=conf['general']['log_format'],
                    level=logging.getLevelName(conf['crawler_lvl2']['log_level'].upper()),
                    filemode='w')
logger = logging.getLogger('crawler-lvl2.py')
index_list_file = os.path.join(conf['update_index']['path'], conf['update_index']['file_name'])
lvl2_data_path = conf['crawler_lvl2']['path']
helper.folder_check(lvl2_data_path)
assert os.path.isfile(index_list_file), f'{index_list_file} not found. first create one using update_index.py'
index_list = pd.read_csv(index_list_file)
assert index_list['crawl'].any(), 'no index to crawl in index_list'
index_list['uid'] = index_list['uid'].str.replace('i', '')
index_list = index_list.loc[index_list['crawl'], ['uid', 'symbol']].to_numpy()
logger.info(f'{len(index_list)} indexes are found to update')
i2get = 0
general_counter = 0
tloc = Lock()
TERM = False


def termsig(*args):
    print(f'termination signal received {args}')
    print(f'exiting. please wait ...')
    global TERM
    TERM = True


# noinspection PyBroadException
def twork():
    global i2get, general_counter, TERM
    sess = requests.session()
    while True:
        with tloc:
            if TERM:
                sess.close()
                break
            if i2get == len(index_list):
                i2get = 0
            uid, symbol = index_list[i2get]
            i2get += 1
            general_counter += 1
        try:
            data = TSEData.last_data_lvl2(uid, symbol, sess)
        except Exception:
            logger.exception(f'error crawling (uid:{uid})')
        else:
            data.to_json(os.path.join(lvl2_data_path, f'{uid}.json'),force_ascii=False)
            if conf['crawler_lvl2']['print_on_stdout']:
                print(data)
            logger.info(f'fetched and saved (uid:{uid})')


n_thread = min(conf['crawler_lvl1']['max_workers'], len(index_list))
threads = []
for i_ in range(n_thread):
    t = Thread(target=twork)
    t.daemon = True
    t.start()
    threads.append(t)
logger.info(f'all threads are created ({n_thread})')
# noinspection PyTypeChecker
signal.signal(signal.SIGTERM, termsig)
print(f'send a SIGTERM to my pid ({os.getpid()}) to exit')
while True:
    if TERM:
        break
    crawl_rate = general_counter / 10
    general_counter = 0
    if conf['crawler_lvl2']['verbose']:
        print('crawl rate:', crawl_rate)
    time.sleep(10)

for t in threads:
    t.join()
