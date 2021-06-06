import time
from typing import Union

import bs4
import hazm
import numpy as np
import pandas as pd
import requests
import yaml

from . import helper

# ======================== constants
MARKETWATCH_COLS = ['uid', 'symbol']
LASTDAYLVL1_DATA_NAMES = ['DEven', 'BaseVolume', 'TotalShares', 'FloatingShares', 'HighLimit', 'LowLimit', 'EPS',
                          'SectorPE']
LASTDAYLVL1_DATA = pd.Series(index=LASTDAYLVL1_DATA_NAMES)
LASTDAYLVL2_DATA_NAMES = ['Date', 'Status', 'LastTradeTime', 'Open', 'First', 'High', 'Low',
                          'Last', 'Close', 'Trade', 'Volume', 'Value']
LASTDAYLVL2_DATA_NAMES += [x + y + z for z in ['No', 'Vol'] for y in ['Buy', 'Sell'] for x in ['Real', 'Legal']]
LASTDAYLVL2_DATA_NAMES += [x + y + z for x in ['Buy', 'Sell'] for y in ['1', '2', '3'] for z in ['No', 'Vol', 'Price']]
LASTDAYLVL2_DATA = pd.Series(index=LASTDAYLVL2_DATA_NAMES)
LASTDAYEVENTS_DATA_NAMES = ['Title', 'DateTime', 'Text']
# ======================== config
with open('./config.yaml') as f:
    conf = yaml.safe_load(f)
LVL1_CDN = conf['crawler_lvl1']['use_cdn']
LVL2_CDN = conf['crawler_lvl2']['use_cdn']
EVENT_CDN = conf['crawler_event']['use_cdn']


# ======================== private functions
def _getparse_market_watch():
    url = 'http://tsetmc.com/tsev2/data/marketwatchdata.aspx'
    par = {'HEven': '0', 'RefId': '0'}
    raw = helper.default_socket(url, par, cdn=False)
    raw = raw.content.decode('utf8')
    raw = raw.split(';')
    raw = [x.split(',') for x in raw[3:-1]]
    data = pd.DataFrame.from_records(raw)[[3, 0]]
    data.columns = MARKETWATCH_COLS
    return data


def _getparse_lastday_lvl1(uid: str, sess=None):
    data = LASTDAYLVL1_DATA.copy()
    url = 'http://tsetmc.com/Loader.aspx'
    par = {'ParTree': '151311', 'i': uid}
    raw = helper.default_socket(url, par, LVL1_CDN, sess)
    raw = raw.content.decode('utf8')
    data['DEven'] = helper.js_var(raw, 'DEven', "='", "',")[0]
    data['BaseVolume'] = helper.js_var(raw, 'BaseVol', "=", ",")[0]
    data['TotalShares'] = helper.js_var(raw, 'ZTitad', "=", ",")[0]
    data['FloatingShares'] = helper.js_var(raw, 'KAjCapValCpsIdx', "='", "',")[0]
    data['HighLimit'] = helper.js_var(raw, 'PSGelStaMax', "='", "',")[0]
    data['LowLimit'] = helper.js_var(raw, 'PSGelStaMin', "='", "',")[0]
    data['EPS'] = helper.js_var(raw, 'EstimatedEPS', "='", "',")[0]
    data['SectorPE'] = helper.js_var(raw, 'SectorPE', "='", "',")[0]
    return data


def _getparse_lastday_lvl2(uid: str, max_try=5, sess=None):
    logger = helper.get_logger()
    data = LASTDAYLVL2_DATA.copy()
    url = 'http://tsetmc.com/tsev2/data/instinfofast.aspx'
    par = {'i': uid, 'c': '10'}
    price, book, client = None, None, None
    for _ in range(max_try):
        flag1, flag2, flag3 = False, False, False
        raw = helper.default_socket(url, par, LVL2_CDN, sess)
        raw = raw.content.decode('utf8')
        raw = raw.split(';')
        price_, book_, client_ = raw[0], raw[2], raw[4]
        if len(price_) != 0:  # available price raw?
            price = price_.split(',')
            flag1 = True
        if len(book_) != 0:  # available book raw?
            book = book_.split(',')
            book = [x.split('@') for x in book]
            flag2 = True
        if len(client_) != 0:  # available client_type raw?
            flag3 = True
            client = client_.split(',')
        if flag1 and flag2 and flag3:
            break

    if price is not None:
        data['Date'] = price[12]
        data['Status'] = price[1].strip()
        data['LastTradeTime'] = price[0]
        data['Open'] = price[5]
        data['First'] = price[4]
        data['High'] = price[6]
        data['Low'] = price[7]
        data['Last'] = price[2]
        data['Close'] = price[3]
        data['Trade'] = price[8]
        data['Volume'] = price[9]
        data['Value'] = price[10]
    else:
        logger.warning(f'no price data (uid:{uid})')
    if book is not None:
        if len(book) >= 1 and len(book[0]) > 1:
            data['Buy1No'] = book[0][0]
            data['Buy1Vol'] = book[0][1]
            data['Buy1Price'] = book[0][2]
            data['Sell1No'] = book[0][5]
            data['Sell1Vol'] = book[0][4]
            data['Sell1Price'] = book[0][3]
        if len(book) >= 2 and len(book[1]) > 1:
            data['Buy2No'] = book[1][0]
            data['Buy2Vol'] = book[1][1]
            data['Buy2Price'] = book[1][2]
            data['Sell2No'] = book[1][5]
            data['Sell2Vol'] = book[1][4]
            data['Sell2Price'] = book[1][3]
        if len(book) >= 3 and len(book[2]) > 1:
            data['Buy3No'] = book[2][0]
            data['Buy3Vol'] = book[2][1]
            data['Buy3Price'] = book[2][2]
            data['Sell3No'] = book[2][5]
            data['Sell3Vol'] = book[2][4]
            data['Sell3Price'] = book[2][3]
    else:
        logger.warning(f'no book data (uid:{uid})')
    if client is not None:
        data['RealBuyVol'] = client[0]
        data['LegalBuyVol'] = client[1]
        data['RealSellVol'] = client[3]
        data['LegalSellVol'] = client[4]
        data['RealBuyNo'] = client[5]
        data['LegalBuyNo'] = client[6]
        data['RealSellNo'] = client[8]
        data['LegalSellNo'] = client[9]
    else:
        logger.warning(f'no client data (uid:{uid})')
    return data


def _getparse_lastday_events(sess=None):
    url = 'http://tsetmc.com/Loader.aspx'
    par = {'ParTree': '151313', 'Flow': '0'}
    raw = helper.default_socket(url, par, EVENT_CDN, sess)
    raw = raw.content.decode('utf8')
    raw = bs4.BeautifulSoup(raw, "html.parser")
    trs = raw.find_all('tr')
    data = [None] * int(len(trs) / 2)
    for c_ in range(0, len(trs), 2):
        tmp = trs[c_].find_all('th')
        title = tmp[0].text
        datetime = tmp[1].text
        text = trs[c_ + 1].find('td').text
        # noinspection PyTypeChecker
        data[c_ // 2] = [title, datetime, text]
    data = pd.DataFrame.from_records(data, columns=LASTDAYEVENTS_DATA_NAMES)
    return data


def _translate_market_watch(data: pd.DataFrame):
    data = data.astype(str)
    data = data.set_index('uid')
    return data


def _translate_lastday_lvl1(data: pd.Series):
    logger = helper.get_logger()
    data = data.copy()
    data = data.replace('', np.nan)
    # noinspection PyBroadException
    try:
        data['DEven'] = pd.to_datetime(data['DEven'], format='%Y%m%d').date()
    except Exception:
        logger.error(f'error translating DEven={data["DEven"]} (uid:{data["uid"]})')
    return data


def _translate_lastday_lvl2(data: Union[pd.DataFrame, pd.Series]):
    logger = helper.get_logger()
    data = data.copy()
    data = data.replace('', np.nan)
    data['Date'] = pd.to_datetime(data['Date'], format='%Y%m%d').date().strftime('%Y-%m-%d')
    # noinspection PyBroadException
    try:
        data['LastTradeTime'] = pd.to_datetime(data['LastTradeTime'], format='%H:%M:%S').time().strftime('%H:%M:%S')
    except Exception:
        logger.error(f'error translating LastTradeTime={data["LastTradeTime"]} (uid:{data["uid"]})')
    return data


def _translate_lastday_events(data: pd.DataFrame):
    normalizer = hazm.Normalizer(persian_style=True, remove_extra_spaces=True, persian_numbers=False,
                                 remove_diacritics=True, affix_spacing=True, token_based=False,
                                 punctuation_spacing=False)
    data['Title'] = data['Title'].apply(normalizer.normalize)
    data['Text'] = data['Text'].apply(normalizer.normalize)
    return data


# ======================== main functions
def market_watch():
    """
    last symbol/uid data from market watch

    Returns
    -------
    pd.DataFrame

    """
    data = _getparse_market_watch()
    data = _translate_market_watch(data)
    return data


def last_data_lvl1(uid, symbol=None, sess=None):
    """
    fetch level-1 information of symbol/instrument
    'Date', 'BaseVolume', 'TotalShares', 'FloatingShares', 'HighLimit', 'LowLimit', 'EPS', 'SectorPE'

    Parameters
    ----------
    uid: str
        InsCode of symbol/instrument
    symbol: str or None
    sess: requests.sessions.Session

    Returns
    -------
    pd.Series

    """
    data = _getparse_lastday_lvl1(uid, sess=sess)
    data['uid'] = uid
    data['symbol'] = symbol or ''
    data['LastUpdate'] = time.time()
    data = _translate_lastday_lvl1(data)
    return data


def last_data_lvl2(uid, symbol=None, sess=None):
    """
    fetch level-2 information of symbol/instrument
    'Date','Status','LastTradeTime','Open','First','High','Low','Last','Close','Trade','Volume','Value','RealBuyNo',
    'LegalBuyNo','RealSellNo','LegalSellNo','RealBuyVol','LegalBuyVol','RealSellVol','LegalSellVol'


    Parameters
    ----------
    uid: str
        InsCode of symbol/instrument
    symbol: str or None
    sess: requests.sessions.Session

    Returns
    -------
    pd.Series

    """
    data = _getparse_lastday_lvl2(uid, sess=sess)
    data['uid'] = uid
    data['symbol'] = symbol or ''
    data['LastUpdate'] = time.time()
    data = _translate_lastday_lvl2(data)
    return data


def last_event(sess=None):
    """
    fetch market events data
    including event title, datetime and text

    Parameters
    ----------
    sess: requests.sessions.Session

    Returns
    -------
    pd.DataFrame

    """
    data = _getparse_lastday_events(sess)
    data = _translate_lastday_events(data)
    return data
