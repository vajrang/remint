import json

from datafetcher import fetch_data_processors

# def mfa():
#     input('Enter the MFA code:')
# pd.set_option('display.max_columns', 50, 'display.width', 500, 'display.max_rows', 50)

if __name__ == '__main__':
    adp, cdp, tdp = fetch_data_processors()
    print(adp)
    print(cdp)
    print(tdp)
