import json
import pickle

import mintapi
from bitarray import bitarray


class MintWrapper():
    def __init__(self):
        self.__mint = None

    def _init_mint(self) -> mintapi.Mint:
        config = {}
        with open('config/mint.json') as f:
            config = json.load(f)

        self.__mint = mintapi.Mint(
            bitarray(config['u']).tobytes().decode('utf8'),
            bitarray(config['p']).tobytes().decode('utf8'),
            # Optional parameters
            mfa_method='sms',  # Can be 'sms' (default), 'email', or 'soft-token'.
            # if mintapi detects an MFA request, it will trigger the requested method
            # and prompt on the command line.
            headless=True,  # Whether the chromedriver should work without opening a
            # visible window (useful for server-side deployments)
            mfa_input_callback=None,  # A callback accepting a single argument (the prompt)
            # which returns the user-inputted 2FA code. By default
            # the default Python `input` function is used.
            session_path='./session',  # Directory that the Chrome persistent session will be written/read from.
            # To avoid the 2FA code being asked for multiple times, you can either set
            # this parameter or log in by hand in Chrome under the same user this runs
            # as.
            imap_account=None,  # account name used to log in to your IMAP server
            imap_password=None,  # account password used to log in to your IMAP server
            imap_server=None,  # IMAP server host name
            imap_folder='INBOX',  # IMAP folder that receives MFA email
            wait_for_sync=False,  # do not wait for accounts to sync
            wait_for_sync_timeout=300,  # number of seconds to wait for sync
            use_chromedriver_on_path=False,  # True will use a system provided chromedriver binary that
            # is on the PATH (instead of downloading the latest version)
        )
        return self.__mint

    def __enter__(self):
        return self

    def __exit__(self, *args):
        # don't use the mint property here in case it's never been initialized
        if self.__mint:
            self.__mint.close()

    @property
    def mint(self):
        if self.__mint:
            return self.__mint
        return self._init_mint()

    # the names of these wrapping methods are required to remain as is
    # there are external callers that rely on this
    def get_accounts(self) -> dict:
        return self.mint.get_accounts()

    def get_categories(self) -> dict:
        return self.mint.get_categories()

    def get_transactions_csv(self) -> bytes:
        return self.mint.get_transactions_csv()
