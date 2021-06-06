import inspect
import itertools
import logging
import os
import re
import time
from threading import Lock

import requests
import yaml

with open('./config.yaml') as f:
    conf = yaml.safe_load(f)
REQ_RETRY = conf['request']['req_retry']
REQ_RETRY_DELAY = conf['request']['req_retry_delay']
REQ_TIMEOUT = conf['request']['req_timeout']
CDN_LIST = conf['request']['cdn_list']
if len(CDN_LIST) == 0:
    CDN_ = ''
else:
    CDN_ = CDN_LIST[0]
CDN_LIST = itertools.cycle(CDN_LIST)
tlock = Lock()


def default_socket(url, par, cdn=False, sess=None):
    logger = get_logger(caller=True)
    global CDN_
    create_flag = False
    if sess is None:
        logger.info('creating new session')
        sess = requests.session()
        create_flag = True
    t_ = 1
    current_cdn = None
    while True:
        if cdn:
            current_cdn = CDN_
            url_ = url.replace('tsetmc.com', f'{current_cdn}tsetmc.com')
        else:
            url_ = url
        # noinspection PyBroadException
        try:
            with sess.get(url_, params=par, timeout=REQ_TIMEOUT) as req:
                pass
            req.raise_for_status()
            logger.info(f'request complete {req.url}')
            break
        except Exception as e:
            logger.warning(f'request error (try={t_}) {url_} {par} : {e}')
            if t_ == REQ_RETRY:
                logger.error(f'request error (max_try={t_}) {url_} {par} : {e}')
                raise e
            if cdn:
                with tlock:
                    if current_cdn == CDN_:
                        tmp = CDN_
                        CDN_ = next(CDN_LIST)
                        logger.warning(f'cdn changed from {tmp} to {CDN_}')
            t_ += 1
            time.sleep(REQ_RETRY_DELAY)
    if create_flag:
        sess.close()
    return req


def js_var(data_, var_, start, end):
    pat_ = f'{var_}{start}(.*?){end}'
    return re.findall(pat_, data_)


def get_logger(currect=True, caller=False):
    name = ''
    if caller:
        name += inspect.stack()[2][3]
        name += ' : '
    if currect:
        name += inspect.stack()[1][3]
        name += ' : '
    return logging.getLogger(name[:-2])


def folder_check(check_path):
    logger = get_logger(caller=True)
    if not os.path.isdir(check_path):
        logger.warning(f'{check_path} directory not found. creating...')
        os.makedirs(check_path)

# def get_request_session(retry=5, backoff=0.1):
#     retry_strategy = Retry(
#         total=retry,
#         status_forcelist=[104, 429, 500, 502, 503, 504],
#         method_whitelist=["GET"],
#         backoff_factor=backoff
#     )
#     adapter = HTTPAdapter(max_retries=retry_strategy)
#     sess = requests.Session()
#     sess.mount("https://", adapter)
#     sess.mount("http://", adapter)
#     return sess
