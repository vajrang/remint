from io import BytesIO

import pandas as pd


class TransactionsDataProcessor():
    """Takes in the raw dict as received from mintapi.get_transactions()
    Only the processed dataframe is cached. The summary is recomputed for every call
    """
    def __init__(self, rawdata: dict, parents: dict, budget_parents: dict):
        self.data = TransactionsDataProcessor._process(rawdata, parents, budget_parents)

    @staticmethod
    def _process(rawdata: dict, parents: dict, budget_parents: dict) -> pd.DataFrame:
        df = pd.read_csv(BytesIO(rawdata))

        df['Date'] = pd.to_datetime(df['Date'])
        df = df[df['Date'] >= '2011-01-01'].copy()

        #add the generated columns
        df['Year'] = df['Date'].array.year
        df['Month'] = df['Date'].array.month
        df['DOY'] = df['Date'].array.dayofyear
        if parents:
            df['Parent'] = df.apply(lambda x: parents.get(x['Category']), axis=1)
        if budget_parents:
            df['BudgetGroup'] = df.apply(lambda x: budget_parents.get(x['Category']), axis=1)

        for c in ['Transaction Type', 'Category', 'Account Name', 'Parent', 'BudgetGroup']:
            df[c] = df[c].astype('category')

        #negate debit amounts
        df.loc[df["Transaction Type"] == "debit", "Amount"] = abs(df.Amount) * +1.0
        df.loc[df["Transaction Type"] == "credit", "Amount"] = abs(df.Amount) * -1.0

        return df
