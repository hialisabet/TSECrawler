import logging
import os

import numpy as np
import pandas as pd
import yaml

from TSEData import TSEData, helper

with open('./config.yaml') as f:
    conf = yaml.safe_load(f)
helper.folder_check(conf['general']['log_path'])
logging.basicConfig(filename=os.path.join(conf['general']['log_path'], 'update_index.log'),
                    format=conf['general']['log_format'],
                    level=logging.getLevelName(conf['update_index']['log_level'].upper()),
                    filemode='w')
logger = logging.getLogger('update_index')
helper.folder_check(conf['update_index']['path'])
file_ = os.path.join(conf['update_index']['path'], conf['update_index']['file_name'])
new_index = TSEData.market_watch()
logger.info(f'fetched {len(new_index)} indexes')
new_index.index = 'i' + new_index.index
new_index['crawl'] = False
if os.path.isfile(file_):
    logger.info('found an existing index file. updating it ...')
    old_index = pd.read_csv(file_, index_col=0)
    i_s = new_index.loc[np.isin(new_index.index, old_index.index)].index
    i_n = new_index.loc[~np.isin(new_index.index, old_index.index)].index
    i_o = old_index.loc[~np.isin(old_index.index, new_index.index)].index
    if len(i_n) > 0:
        logger.warning(f'{len(i_n)} symbols are missed in old index data')
        print('following data are missed in old index data')
        print(new_index.loc[i_n])
    if len(i_o) > 0:
        logger.warning(f'{len(i_o)} symbols are missed in old index data')
        print('following data are missed in new index data')
        print(old_index.loc[i_o])
    if len(i_n) == 0 and len(i_o) == 0:
        logger.info(f'no change in existing index file')
        exit()
    new_index = [old_index.loc[i_s], old_index.loc[i_o], new_index.loc[i_n]]
    # noinspection PyTypeChecker
    new_index = pd.concat(new_index)
new_index = new_index.sort_values('symbol')
new_index.to_csv(file_)
logger.info(f'index file saved to {file_}')
