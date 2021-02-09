from datetime import datetime

import pandas as pd
import seaborn as sns
import streamlit as st

from datafetcher import ProcessorType, fetch_data_processor

# def mfa():
#     input('Enter the MFA code:')
# pd.set_option('display.max_columns', 50, 'display.width', 500, 'display.max_rows', 50)

if __name__ == '__main__':
    st.set_page_config(layout='wide')

    t0 = datetime.now()
    adp = fetch_data_processor(ProcessorType.ACCOUNTS)
    for cat, val in adp.get_summary():
        print(f'{cat:.<19}:{val:>13,.2f}')

    t1 = datetime.now()
    tdp = fetch_data_processor(ProcessorType.TRANSACTIONS)
    assert isinstance(tdp.data, pd.DataFrame)
    tdp.data.to_csv('~/Downloads/transactions.csv', index=False)

    t2 = datetime.now()

    expenses = tdp.expenses_data.groupby(['Parent', 'Year'], observed=True)['Amount'].sum().unstack(fill_value=0)
    sns.heatmap(expenses, robust=True, annot=True, fmt=',.0f', cbar=False)

    print(f't1 = {(t1-t0).total_seconds():.2f}')
    print(f't2 = {(t2-t1).total_seconds():.2f}')

    # st.table(adp.get_summary())
