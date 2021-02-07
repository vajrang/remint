import pandas as pd


class AccountsDataProcessor():
    """Takes in the raw dict as received from mintapi.get_accounts()
    Only the processed dataframe is cached. The summary is recomputed for every call
    """
    def __init__(self, rawdata: dict):
        self.data = AccountsDataProcessor._process(rawdata)

    @staticmethod
    def _process(rawdata: dict) -> pd.DataFrame:
        df = pd.DataFrame(rawdata)
        # keep only relevant columns
        df = df[[
            'id', 'accountType', 'fiName', 'fiLoginDisplayName', 'accountName', 'yodleeAccountNumberLast4', 'isActive',
            'lastUpdated', 'isError', 'value'
        ]]
        # keep only active accounts
        df = df[df['isActive']]
        # change all HealthEquity accounts to count as investment
        df.loc[df['fiName'] == 'HealthEquity', 'accountType'] = 'investment'
        # set the value of the Robinhood account to $21,000, if it's in sync error state
        # TODO put this into the config file
        if df.loc[df['id'] == 9212055, 'isError'].bool():
            df.loc[df['id'] == 9212055, 'value'] = 21_000.00
        return df

    def get_summary(self) -> dict:
        df = self.data
        savings = df.loc[df['accountType'].isin(['bank', 'credit']), 'value'].sum()
        investments = df.loc[df['fiName'].isin([
            'Betterment',
            'M1 Finance',
            'Robinhood',
            'E*TRADE Securities',
        ]), 'value'].sum()
        inv401k = df.loc[df['accountName'].isin([
            'Vajrang-Rollover IRA',
            'iRobot',
            'Kranti-IRA',
            'ETade-401k',
        ]), 'value'].sum()
        inv529 = df.loc[df['accountName'].isin(['Rudra 529', 'Arin 529']), 'value'].sum()
        invHSA = df.loc[df['accountName'].isin([
            'Vajrang-HSA',
            'Vajrang-HSA-Investment',
            'Kranti-HSA',
        ]), 'value'].sum()
        assets = df.loc[df['accountType'].isin(['real estate', 'vehicle']), 'value'].sum()
        unaccounted = df['value'].sum() - savings - investments - inv401k - inv529 - invHSA - assets
        total_wo_assets = df['value'].sum() - assets
        total = df['value'].sum()

        retval = (
            [
                ('Savings', savings),
                ('Investments', investments),
                ('401k', inv401k),
                ('529', inv529),
                ('HSA', invHSA),
                ('Assets', assets),
                ('Unaccounted', unaccounted),
                ('Total (w/o assets)', total_wo_assets),
                ('Grand Total', total),
            ]
        )
        return retval
