from io import BytesIO

import pandas as pd


def filter_column_containing(input: pd.DataFrame, column_name: str, matches: list[str]) -> pd.DataFrame:
    return input[input[column_name].str.contains('|'.join(matches), regex=True) != True]


class TransactionsDataProcessor():
    """Takes in the raw dict as received from mintapi.get_transactions()
    Only the processed dataframe is cached. The summary is recomputed for every call
    """
    def __init__(self, rawdata: dict, parents: dict, budget_parents: dict):
        self.data = TransactionsDataProcessor._process(rawdata, parents, budget_parents)

    @staticmethod
    def _process(rawdata: dict, parents: dict, budget_parents: dict) -> pd.DataFrame:
        df = pd.read_df(BytesIO(rawdata))

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
        df.loc[df["Transaction Type"] == "debit", "Amount"] = abs(df.Amount) * -1.0
        df.loc[df["Transaction Type"] == "credit", "Amount"] = abs(df.Amount) * +1.0

        return df

    @property
    def expenses_data(self) -> pd.DataFrame:
        """Removes: 1. Labels not needed
        2. Categories not counted in expenses
        3. Accounts not counted
        4. Certain one-off transactions, viz. basement costs & Honda Odyssey purchase
        """
        df = self.data.copy()

        # labels
        df = filter_column_containing(df, 'Labels', ['Excluded', 'Reimbursable'])

        df = filter_column_containing(
            df, 'Category', [
                'Transfer',
                'Credit Card Payment',
                'Investments',
                'Dividend',
                'Buy',
                'Sell',
                'Paycheck',
                'Bonus',
                'Gift Income',
                'Income',
                'Cash Back Rewards',
                'Returned Purchase',
                'Rudra Income',
                'Tax Refund',
                'Federal Tax',
                'State Tax',
            ]
        )

        # exclude Rudra's accounts
        df = filter_column_containing(df, 'Account Name', ['Amex Blue - Rudra'])

        # exclude the basements & minivan (to get a YoY apples-to-apples comparison)
        df = filter_column_containing(df, 'Description', [
            'David Goldbaum',
            'Bill Lampros',
            'The Honda Store',
        ])

        # for c in df.dtypes[df.dtypes == 'category'].index:
        #     df[c] = df[c].astype('str').astype('category')

        return df
