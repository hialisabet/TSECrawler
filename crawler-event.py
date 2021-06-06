import datetime
import logging
import numpy as np
import os
import signal
import time
import hashlib

import pandas as pd
import requests
import yaml

from TSEData import TSEData, helper

with open('./config.yaml') as f:
    conf = yaml.safe_load(f)
helper.folder_check(conf['general']['log_path'])
logging.basicConfig(filename=os.path.join(conf['general']['log_path'], 'crawler_event.log'),
                    format=conf['general']['log_format'],
                    level=logging.getLevelName(conf['crawler_event']['log_level'].upper()),
                    filemode='w')
logger = logging.getLogger('crawler-event.py')
event_data_path = conf['crawler_event']['path']
helper.folder_check(event_data_path)
update_interval = conf['crawler_event']['interval']
sess = requests.session()
TERM = False


def termsig(*args):
    print(f'termination signal received {args}')
    print(f'exiting. please wait ...')
    global TERM
    TERM = True


# noinspection PyTypeChecker
signal.signal(signal.SIGTERM, termsig)
print(f'send a SIGTERM to my pid ({os.getpid()}) to exit')
data_total = pd.DataFrame(columns=['Title', 'DateTime', 'Text', 'MD5'])
md5_gen = lambda x: hashlib.md5((x['Title'] + x['DateTime']).encode()).hexdigest()
while not TERM:
    # noinspection PyBroadException
    try:
        fetch_data = TSEData.last_event(sess)
        fetch_data['MD5'] = fetch_data.apply(md5_gen, axis=1)
        new_data = fetch_data.loc[~np.isin(fetch_data['MD5'], data_total['MD5'])]
    except Exception:
        logger.exception(f'error crawling')
    else:
        if len(new_data) > 0:
            data_total = pd.concat((data_total, new_data))
            for _, d_ in new_data.iterrows():
                d_.DateTime = d_.DateTime.replace('/', '-')
                with open(os.path.join(event_data_path, f'{d_["MD5"]}.json'), 'w', encoding='utf8') as jfile:
                    d_[['Title', 'DateTime', 'Text']].to_json(jfile, force_ascii=False, compression=False)
            if conf['crawler_event']['print_on_stdout']:
                print(new_data)
            if conf['crawler_event']['verbose']:
                print(f'fetched new {len(new_data)} market event data')
            logger.info(f'fetched and saved')
        else:
            logger.info(f'no new event')
    time.sleep(update_interval)

sess.close()
