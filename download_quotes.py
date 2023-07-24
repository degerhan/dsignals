from __future__ import annotations

import logging
import os
import re
import time
from concurrent import futures
from pathlib import Path

import pandas as pd
import requests
from tqdm import tqdm

from build_eodhd_map import EODHD, YAHOO

_logger = logging.getLogger(__name__)

# Read eodhistoricaldata.com token fron environment -- or insert into code
EODHD_TOKEN = os.getenv("NUMERAI_EODHD_TOKEN", "your_eodhd_api_key")

MAP_FILE = Path("./db/eodhd-map.csv")
QUOTE_FOLDER = Path("./data/ticker_bin")
STATUS_FILE = QUOTE_FOLDER / "download_status.csv"

_RETRY_COUNT = 3
_RETRY_WAIT = 25
_MAX_WORKERS = 10


def write_status(bbg_ticker, yahoo_ticker, provider, signals_ticker, count, status):
    with open(STATUS_FILE, "a") as file:
        file.write(
            f"{bbg_ticker},{yahoo_ticker},{provider},{signals_ticker},{count},{status}\n"
        )


def yahoo_download_one(signals_ticker: str) -> tuple[pd.DataFrame, int]:
    start_epoch = int(946684800)  # 2000-01-01
    end_epoch = int(time.time())
    quotes = None

    quotes = (
        pd.read_csv(
            f"https://query1.finance.yahoo.com/v7/finance/download/{signals_ticker}?period1={start_epoch}&period2={end_epoch}&interval=1d&events=history&includeAdjustedClose=true"
        )
        .dropna()
        .set_index("Date")
    )

    if quotes is not None and len(quotes) > 1:
        quotes["date64"] = pd.to_datetime(quotes.index, format="%Y-%m-%d")
        quotes = quotes.reset_index(drop=True).set_index("date64").sort_index()
        quotes.index.name = "date"
        quotes.columns = [
            "open",
            "high",
            "low",
            "close",
            "adjusted_close",
            "volume",
        ]

    return quotes, 0


def eodhd_download_one(signals_ticker: str) -> tuple[pd.DataFrame, int]:
    start_date = "2000-01-01"
    quotes = None

    r = requests.get(
        f"https://eodhistoricaldata.com/api/eod/{signals_ticker}?from={start_date}&fmt=json&api_token={EODHD_TOKEN}"
    )

    if r.status_code == requests.codes.ok:
        if len(r.json()) > 0:
            quotes = pd.DataFrame(r.json()).set_index("date")
            quotes["date64"] = pd.to_datetime(quotes.index, format="%Y-%m-%d")
            quotes = quotes.reset_index(drop=True).set_index("date64").sort_index()
            quotes.index.name = "date"
            quotes.columns = [
                "open",
                "high",
                "low",
                "close",
                "adjusted_close",
                "volume",
            ]

    return quotes, r.status_code


def download_one(bloomberg_ticker: str, map: pd.DataFrame):
    yahoo_ticker = map.loc[bloomberg_ticker, "yahoo"]
    signals_ticker = map.loc[bloomberg_ticker, "signals_ticker"]
    data_provider = map.loc[bloomberg_ticker, "data_provider"]

    if pd.isnull(signals_ticker):
        write_status(
            bloomberg_ticker,
            yahoo_ticker,
            data_provider,
            signals_ticker,
            -1,
            "signals_ticker cannot be null",
        )
        return bloomberg_ticker, None

    quotes = None
    for _ in range(_RETRY_COUNT):
        try:
            if data_provider == EODHD:
                quotes, status_code = eodhd_download_one(signals_ticker)
            elif data_provider == YAHOO:
                quotes, status_code = yahoo_download_one(signals_ticker)

            count = len(quotes) if quotes is not None else -1
            write_status(
                bloomberg_ticker,
                yahoo_ticker,
                data_provider,
                signals_ticker,
                count,
                status_code,
            )

            break

        except Exception as ex:
            _logger.warning(f"download_one, ticker:{bloomberg_ticker}, exception:{ex}")
            write_status(
                bloomberg_ticker,
                yahoo_ticker,
                data_provider,
                signals_ticker,
                -3,
                ex,
            )
            time.sleep(_RETRY_WAIT)

    return bloomberg_ticker, quotes


def make_filename_safe(bloomberg_ticker):
    return re.sub(r"[^\w-]", "_", bloomberg_ticker.lower()) + ".pkl"


def download_save_all(ticker_map):
    # Shuffle the download order to balance load and wait times with data providers
    tickers = pd.Series(ticker_map.index).sample(frac=1).unique().tolist()

    with futures.ThreadPoolExecutor(_MAX_WORKERS) as executor:
        _futures = []
        for ticker in tickers:
            _futures.append(
                executor.submit(
                    download_one,
                    bloomberg_ticker=ticker,
                    map=ticker_map,
                )
            )

        for future in tqdm(futures.as_completed(_futures), total=len(tickers)):
            bloomberg_ticker, quotes = future.result()
            if quotes is not None:
                quotes.to_pickle(QUOTE_FOLDER / make_filename_safe(bloomberg_ticker))


def read_quotes(ticker) -> pd.DataFrame:
    filename = Path(QUOTE_FOLDER / make_filename_safe(ticker))
    if filename.exists():
        quotes = pd.read_pickle(filename)
        return quotes
    else:
        return None


def main():
    map = pd.read_csv(MAP_FILE, index_col=0)

    QUOTE_FOLDER.mkdir(exist_ok=True, parents=True)
    with open(STATUS_FILE, "w") as file:
        file.write(
            f"bloomberg_ticker,yahoo_ticker,data_provider,signals_ticker,count,status\n"
        )

    download_save_all(map)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
