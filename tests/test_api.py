from typing import NewType

from api import get_networth


def test_get_networth():
    networth = get_networth()
    assert len(networth) == 10
