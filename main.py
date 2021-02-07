import streamlit as st

from datafetcher import ProcessorType, fetch_data_processor

# def mfa():
#     input('Enter the MFA code:')
# pd.set_option('display.max_columns', 50, 'display.width', 500, 'display.max_rows', 50)

if __name__ == '__main__':
    st.set_page_config(layout='wide')

    adp = fetch_data_processor(ProcessorType.ACCOUNTS)
    print(adp.get_summary())
    st.table(adp.get_summary())
    # print(adp)
    # print(cdp)
    # print(tdp)
