import logging
import os
import time
from concurrent import futures

import pandas as pd
import requests
import yaml

from TSEData import TSEData, helper

with open('./config.yaml') as f:
    conf = yaml.safe_load(f)
helper.folder_check(conf['general']['log_path'])
logging.basicConfig(filename=os.path.join(conf['general']['log_path'], 'crawler_lvl1.log'),
                    format=conf['general']['log_format'],
                    level=logging.getLevelName(conf['crawler_lvl1']['log_level'].upper()),
                    filemode='w')
logger = logging.getLogger('crawler-lvl1.py')
index_list_file = os.path.join(conf['update_index']['path'], conf['update_index']['file_name'])
lvl1_data_path = conf['crawler_lvl1']['path']
helper.folder_check(lvl1_data_path)
assert os.path.isfile(index_list_file), f'{index_list_file} not found. first create one using update_index.py'
index_list = pd.read_csv(index_list_file)
assert index_list['crawl'].any(), 'no index to crawl in index_list'
index_list['uid'] = index_list['uid'].str.replace('i', '')
index_list = index_list.loc[index_list['crawl'], ['uid', 'symbol']].to_numpy()
logger.info(f'{len(index_list)} indexes are found to update')
sess = requests.session()
failed = []


def twork(t_):
    uid, sym = t_
    # noinspection PyBroadException
    try:
        data = TSEData.last_data_lvl1(uid, sym, sess=sess)
    except Exception:
        logger.exception(f'error crawling (uid:{uid})')
        failed.append(uid)
    else:
        data.to_json(os.path.join(lvl1_data_path, f'{uid}.json'), force_ascii=False)
        if conf['crawler_lvl1']['print_on_stdout']:
            print(data)
        logger.info(f'fetched and saved (uid:{uid})')


n_thread = conf['crawler_lvl1']['max_workers']
with futures.ThreadPoolExecutor(n_thread) as executor:
    run = []
    t1 = time.time()
    for i_ in index_list:
        run.append(executor.submit(twork, i_))
    logger.info(f'all requests are submitted to {n_thread} threads')
    futures.as_completed(run)
logger.info(f'all requests complete in {time.time() - t1} seconds')
sess.close()
if len(failed) != 0:
    print(f'error fetching {len(failed)} indexes:{failed}')
    logger.error(f'crawling error in {len(failed)} indexes:{failed}')
