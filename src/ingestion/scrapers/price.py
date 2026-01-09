import numpy as np


def fetch_price(coin: str, current_price: float | None) -> dict:
    """
    For now random coin prices generator

    :param coin: Coin name eg. bitcoin, ethereum, dogecoin, axie infinity, litecoin
    :type coin: str
    """
    coin = coin.lower()

    means: dict[str, float] = {"bitcoin": 90_000.0, "ethereum": 2500.0, "dogecoin": 0.2}
    stds: dict[str, float] = {"bitcoin": 5000.0, "ethereum": 100.0, "dogecoin": 0.005}

    if current_price is None:
        current_price = means.get(coin, 0.0)

    new_price = np.random.normal(loc=current_price, scale=stds.get(coin, 1.0))

    return {
        "currency": coin,
        "price": new_price,
    }


def fetch_other_info(coin: str) -> dict[str, float | str]:

    coin = coin.lower()

    cex_volume = np.random.normal(loc=55_000_000_000, scale=1_000_000_000)
    dex_volume = np.random.normal(loc=100_000_000, scale=1_000_000)

    return {"currency": coin, "cex_volume_24h": cex_volume, "dex_volume_24h": dex_volume}
