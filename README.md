# dsignals

Utilities and information for the signals.numer.ai tournament

## using eodhistoricaldata.com

eodhistoricaldata.com provides excellent historical price coverage for the signals universe. There are two main challenges with it:

1. Ticker mapping from bloomberg to eod tickers
2. Lack of coverage for Japan, Czech Republic and New Zealand

### Building the ticker map

To build the mapping from bloomberg_ticker to eodhd, use:

    python build_eodhd_map.py

This will retrieve:

- live_universe (a small 40 KB file just listing the ~5,340 tickers in current round)
- historical_targets (a large 150 MB file, and extract ~13,370 unique historical tickers)
- the bloomberg to yahoo map courtesy of Liam @ numerai

And follow the conversion logic in the python code and manual overrides in `db/eod-overrides.csv` to build `eodhd-map.csv` in the following format:

| bloomberg_ticker | yahoo | data_provider | signals_ticker |
|---|---|---|---|
| MONY LN | MONY.L | eodhd | MONY.LSE |
| ANIM3 BZ | ANIM3.SA | eodhd | ANIM3.SA |
| CAO US |   | eodhd | CAO.US |
| 7013 JP | 7013.T | yahoo | 7013.T |

### Download quotes from the correct data_provider

First find `EODHD_TOKEN = "put_your_token_here"` in the `download_quotes.py` file and insert your eodhd api token. Then running:

    python download_quotes.py

will download each quote from the appropriate source (eodhd or yahoo) saving each ticker to a separate pickle file under ./data/ticker_bin. As of October 2021, this results in 10,900+ ticker histories.

### How you can help

- Some amount of experimentation is needed with Korean tickers (KO vs KQ extension) to get better fills for ~50 tickers.
- Bloomberg Singapore ticker prefixes are very different than the yahoo or eodhd tickers. We are extracting the live universe prefixes from numerai yahoo map, but historical Singapore tickers would need to be manually mapped if anyone is up for the challenge.
- The rest of the tickers seem to work well -- all feedback and advice is appreciated.
