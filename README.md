# dsignals

Utilities and information for the signals.numer.ai tournament.

## Working with eodhistoricaldata.com

eodhistoricaldata.com provides excellent ticker coverage (including delisted Signals tickers) for historical prices. While a free tier is available, I recommend the "EOD Historical Data - All World" package at $19.99/mo to make the most of dsignals.

dsignals solves the two main challenges with using eodhistoricaldata in Signals:

- `build_eodhd_map.py` generates the ticker mappings from bloomberg_ticker.
- `download_quotes.py` provides comprehensive global coverage by downloading from eodhistoricaldata on its supported stock exchanges, and from yahoo for Japan, Czech Republic and New Zealand.

## Generate the ticker map

To generate the up-to-date ticker mappings for the entire Signals universe (live and historical), run:

    python build_eodhd_map.py

Step 1, this will download and merge tickers from three sources:

- live_universe (a small 40 KB file with the ~5,340 tickers for the current round)
- historical_targets (a large 150 MB file, and extract ~13,370 unique historical tickers)
- the bloomberg to yahoo map courtesy of Liam @ numerai

Step 2, tickers are mapped, and overrides in `db/eodhd-overrides.csv` are applied.

Step 3, `db/eodhd-map.csv` is generated in the following format:

| bloomberg_ticker | yahoo | data_provider | signals_ticker |
|---|---|---|---|
| MONY LN | MONY.L | eodhd | MONY.LSE |
| ANIM3 BZ | ANIM3.SA | eodhd | ANIM3.SA |
| CAO US |   | eodhd | CAO.US |
| 7013 JP | 7013.T | yahoo | 7013.T |

## Download historical data

There are two ways to specify your eodhistoricaldata API token for use by `download_quotes.py`:

- set "NUMERAI_EODHD_TOKEN" environment variable:

    linux: `export NUMERAI_EODHD_TOKEN="your_eodhd_api_key"`

    windows: `set NUMERAI_EODHD_TOKEN="your_eodhd_api_key"`

- or, edit `download_quotes.py` and replace `"your_eodhd_api_key"` with your API token.

To start the download, run:

    python download_quotes.py

This will download price data from either eodhistoricaldata or yahoo, and save each ticker to a separate pickle file in the `data/ticker_bin` folder. As of February 2022, this yields price data for 11,200+ tickers.

 You can modify the download behavior with two arguments:

| argument | description |
| --- | --- |
| --live | Download quotes for only the live ticker universe. The complete historical ticker universe will be downloaded if not specified. |
| --startdate YYYY-MM-DD | Start date for the quote history. The default value of 2000-01-01 will be used if not specified. |

## Read the downloaded quotes

Example code to receive a pandas DataFrame for a given bloomberg_ticker:

    from download_quotes import read_quotes
    quotes = read_quotes("MSFT US")

Alternatively, copy the `make_filename_safe()` function to your codefile to generate the name of the pickle file for a given ticker, and read the pickle file directly with `pd.read_pickle(filename)`.
