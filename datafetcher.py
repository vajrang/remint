import glob
import json
import os
import pickle
import re
from datetime import datetime
from enum import Enum, Flag, auto

from accountsdataprocessor import AccountsDataProcessor
from budgetgroups import BudgetGroups
from categoriesdataprocessor import CategoriesDataProcessor
from mintwrapper import MintWrapper
from transactionsdataprocessor import TransactionsDataProcessor

CACHE_TTL = 6  # hours
CACHE_PATH = './cache'
if not os.path.isdir(CACHE_PATH):
    os.makedirs(CACHE_PATH, exist_ok=False)


def _purge_cache() -> None:
    """checks each cache file and removes it if older than CACHE_TTL hours """
    pattern = re.compile(r'.+_(\d+)_(\d+)_(\d+)_(\d+)_(\d+)_(\d+)\..+')
    for file in glob.glob(f'{CACHE_PATH}/*'):
        m = pattern.fullmatch(file)
        if m:
            g = m.groups()
            ts = datetime(int(g[0]), int(g[1]), int(g[2]), int(g[3]), int(g[4]), int(g[5]))
            if (datetime.now() - ts).total_seconds() > CACHE_TTL * 60 * 60:
                os.remove(file)


def _read_data_from_cache(dataprefix: str):
    """fetches data from cache for given prefix; reads latest one if there are more than one"""

    files = sorted(glob.glob(f'{CACHE_PATH}/{dataprefix}_*.p'))

    raw_data = None
    try:
        with open(files[-1], 'rb') as f:
            raw_data = pickle.load(f)
    except Exception as ex:
        pass

    return raw_data


def _save_data_to_cache(data, prefix: str) -> None:
    time_format_str = f'{prefix}_%Y_%m_%d_%H_%M_%S.p'
    with open(datetime.now().strftime(f'{CACHE_PATH}/{time_format_str}'), 'wb') as f:
        pickle.dump(data, f)


# ---------------------------------------------------------------------------------------------------------------------


def _fetch_raw_data(mint: MintWrapper, dataname: str):
    raw_data = _read_data_from_cache(dataname + '_raw')
    if raw_data is None:
        raw_data = getattr(mint, f'get_{dataname}')()
        _save_data_to_cache(raw_data, f'{dataname}_raw')
    return raw_data


# ---------------------------------------------------------------------------------------------------------------------


def _fetch_dataprocessor_adp(mint: MintWrapper, dataname: str):
    dp = _read_data_from_cache(dataname + '_dp')
    if dp is None:
        dp = AccountsDataProcessor(_fetch_raw_data(mint, dataname))
        _save_data_to_cache(dp, f'{dataname}_dp')
    return dp


def _fetch_dataprocessor_cdp(mint: MintWrapper, dataname: str):
    dp = _read_data_from_cache(dataname + '_dp')
    if dp is None:
        dp = CategoriesDataProcessor(_fetch_raw_data(mint, dataname))
        _save_data_to_cache(dp, f'{dataname}_dp')
    return dp


def _fetch_dataprocessor_tdp(mint: MintWrapper, dataname: str):
    dp = _read_data_from_cache(dataname + '_dp')
    if dp is None:
        cdp = _fetch_dataprocessor_cdp(mint, 'categories')
        assert cdp
        dp = TransactionsDataProcessor(
            _fetch_raw_data(mint, 'transactions_csv'),
            cdp.get_parents(),
            get_budgets(cdp.get_parents()).get_budget_parents(),
        )
        _save_data_to_cache(dp, f'{dataname}_dp')
    return dp


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


class ProcessorType(Enum):
    ACCOUNTS = auto()
    CATEGORIES = auto()
    TRANSACTIONS = auto()


def fetch_data_processor(dptype: ProcessorType) -> dict:
    _purge_cache()

    with MintWrapper() as mint:
        if dptype == ProcessorType.ACCOUNTS:
            return _fetch_dataprocessor_adp(mint, 'accounts')

        elif dptype == ProcessorType.CATEGORIES:
            return _fetch_dataprocessor_cdp(mint, 'categories')

        elif dptype == ProcessorType.TRANSACTIONS:
            return _fetch_dataprocessor_tdp(mint, 'transactions')

        else:
            return None
