from dataclasses import dataclass


@dataclass
class TickerMapping:
    ticker: str
    name: str


TICKERS = [
    TickerMapping(ticker="BZ=F", name="Brent"),
    TickerMapping(ticker="CL=F", name="WTI"),
    TickerMapping(ticker="^OVX", name="OVX"),
]
