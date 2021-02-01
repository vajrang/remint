import glob
import json
import os
import pickle
import re
from datetime import datetime

from accountsdataprocessor import AccountsDataProcessor
from budgetgroups import BudgetGroups
from categoriesdataprocessor import CategoriesDataProcessor
from mintwrapper import MintWrapper
from transactionsdataprocessor import TransactionsDataProcessor

CACHE_TTL = 6  # hours
CACHE_PATH = './cache'
if not os.path.isdir(CACHE_PATH):
    os.makedirs(CACHE_PATH, exist_ok=False)


def _get_cache_timestamp() -> datetime:
    """gets the timestamp when the cache was last updated
    by checking the latest timestamp of each file"""
    pattern = re.compile(r'.+_(\d+)_(\d+)_(\d+)_(\d+)_(\d+)_(\d+)\..+')
    timestamps = []
    for file in glob.glob(f'{CACHE_PATH}/*'):
        m = pattern.fullmatch(file)
        if m:
            g = m.groups()
            ts = datetime(int(g[0]), int(g[1]), int(g[2]), int(g[3]), int(g[4]), int(g[5]))
            timestamps.append(ts)

    return max(timestamps) if timestamps else datetime.min


def _delete_cache():
    """removes everything in the cache"""
    for file in glob.glob(f'{CACHE_PATH}/*'):
        os.remove(file)


def _fetch_data_from_cache(dataprefix: str):
    """fetches data from cache for given prefix; reads latest one if there are more than one
    """

    files = sorted(glob.glob(f'{CACHE_PATH}/{dataprefix}_*.p'))

    raw_data = None
    try:
        with open(files[-1], 'rb') as f:
            raw_data = pickle.load(f)
    except Exception as ex:
        pass

    return raw_data


def _save_data_to_cache(data, dataprefix: str) -> None:
    time_format_str = f'{dataprefix}_%Y_%m_%d_%H_%M_%S.p'
    with open(datetime.now().strftime(f'{CACHE_PATH}/{time_format_str}'), 'wb') as f:
        pickle.dump(data, f)


# ---------------------------------------------------------------------------------------------------------------------


def _fetch_raw_data_to_cache(mint: MintWrapper, dataname: str):
    """fetches raw data from mintapi, stores in cache with a timestamp in the name and returns it
    """

    # time_format_str = f'{dataname}_raw_%Y_%m_%d_%H_%M_%S.p'
    raw_data = getattr(mint, f'get_{dataname}')()
    _save_data_to_cache(raw_data, f'{dataname}_raw')
    return raw_data


def _fetch_raw_data(mint: MintWrapper, dataname: str):
    raw_data = _fetch_data_from_cache(dataname + '_raw')
    if raw_data is None:
        raw_data = _fetch_raw_data_to_cache(mint, dataname)
    return raw_data


def _fetch_data_processors():
    adp = _fetch_data_from_cache('adp')
    cdp = _fetch_data_from_cache('cdp')
    tdp = _fetch_data_from_cache('tdp')

    if not all((adp, cdp, tdp)):  # recreate all the dataprocessors
        a, c, t = fetch_raw_data()

        adp = AccountsDataProcessor(a)
        _save_data_to_cache(adp, 'adp')
        cdp = CategoriesDataProcessor(c)
        _save_data_to_cache(cdp, 'cdp')
        tdp = TransactionsDataProcessor(
            t,
            cdp.get_parents(),
            get_budgets(cdp.get_parents()).get_budget_parents(),
        )
        _save_data_to_cache(tdp, 'tdp')

    return (adp, cdp, tdp)


# ---------------------------------------------------------------------------------------------------------------------


def get_budgets(parents: dict) -> BudgetGroups:
    rawdata = None
    with open('config/budgets.json', 'r') as f:
        rawdata = json.load(f)
    bg = BudgetGroups(
        rawdata,
        parents,
    )
    return bg


def fetch_raw_data():
    # if cache is older than TTL, blow it away
    if (datetime.now() - _get_cache_timestamp()).total_seconds() > CACHE_TTL * 60 * 60:
        _delete_cache()

    with MintWrapper() as mint:
        a = _fetch_raw_data(mint, 'accounts')
        c = _fetch_raw_data(mint, 'categories')
        t = _fetch_raw_data(mint, 'transactions_csv')
        return (a, c, t)


def fetch_data_processors():
    # if cache is older than TTL, blow it away
    if (datetime.now() - _get_cache_timestamp()).total_seconds() > CACHE_TTL * 60 * 60:
        _delete_cache()

    adp, cdp, tdp = _fetch_data_processors()
    return (adp, cdp, tdp)
