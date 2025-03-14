from dataclasses import dataclass


@dataclass
class TickerMapping:
    ticker: str
    name: str


TICKERS = [
    TickerMapping(ticker="BZ=F", name="brent"),
    TickerMapping(ticker="CL=F", name="wti"),
    TickerMapping(ticker="^OVX", name="ovx"),  # oil volatility index
    TickerMapping(ticker="DX-Y.NYB", name="dxy"),  # US Dollar Index
]
