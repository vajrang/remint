import pandas as pd


class CategoriesDataProcessor():
    """Takes in the raw dict as received from mintapi.get_categories()
    Only the processed dataframe is cached. The summary is recomputed for every call
    """
    def __init__(self, rawdata: dict):
        self.data = CategoriesDataProcessor._process(rawdata)

    @staticmethod
    def _process(rawdata: dict) -> pd.DataFrame:
        df = pd.DataFrame(rawdata).transpose()
        assert df['depth'].max() == 2  # more than two levels deep not handled
        df['parentCategory'] = df['parent'].apply(pd.Series)['name']  # normalize the json in the parent column
        df.drop(['parent', 'notificationName'], axis=1, inplace=True)  # drop unneeded columns

        # set the parent to self if it's a root category (makes it easier to groupby later)
        df.loc[df['parentCategory'] == 'Root', 'parentCategory'] = df['name']
        return df

    def get_parents(self) -> dict:
        """Returns a dict of category:parentCategory for all categories"""
        df_dict = self.data[['name', 'parentCategory']].transpose().to_dict()
        retval = {v['name']: v['parentCategory'] for v in df_dict.values()}
        return retval

    def __str__(self) -> str:
        info = self.get_parents()
        return str(info)
