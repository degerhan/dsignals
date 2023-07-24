from __future__ import annotations

import collections
import logging
from pathlib import Path

import pandas as pd

_logger = logging.getLogger(__name__)

IGNORE = "ignore"
YAHOO = "yahoo"
EODHD = "eodhd"
BBG = "bbg"
FIXUPHK = "fixuphk"

AWS_BASE_URL = "https://numerai-signals-public-data.s3-us-west-2.amazonaws.com"
SIGNALS_UNIVERSE = f"{AWS_BASE_URL}/latest_universe.csv"
SIGNALS_TICKER_MAP = f"{AWS_BASE_URL}/signals_ticker_map_w_bbg.csv"
SIGNALS_TARGETS = f"{AWS_BASE_URL}/signals_train_val_bbg.csv"

DB_FOLDER = Path("./db/")
OVERRIDE_DB = DB_FOLDER / "eodhd-overrides.csv"
MAP_EXPORT = DB_FOLDER / "eodhd-map.csv"

ConverterItem = collections.namedtuple(
    "ConverterItem",
    [
        "data_provider",  # {eodhd, yahoo, ignore}
        "ticker_source",  # {bbg, yahoo, fixuphk}
        "suffix_source",  # {eodhd, yahoo}
        "eodhd_suffix",
    ],
    defaults=(EODHD, BBG, EODHD, ""),
)

converters = {
    "AU": ConverterItem(EODHD, BBG, EODHD, ".AU"),
    "AV": ConverterItem(EODHD, BBG, EODHD, ".VI"),
    "BB": ConverterItem(EODHD, BBG, EODHD, ".BR"),
    "BZ": ConverterItem(EODHD, BBG, EODHD, ".SA"),
    "CA": ConverterItem(IGNORE),
    "CH": ConverterItem(IGNORE),
    "CN": ConverterItem(EODHD, BBG, EODHD, ".TO"),
    "CP": ConverterItem(YAHOO, YAHOO, YAHOO),
    "DC": ConverterItem(EODHD, BBG, EODHD, ".CO"),
    "FH": ConverterItem(EODHD, BBG, EODHD, ".HE"),
    "FP": ConverterItem(EODHD, BBG, EODHD, ".PA"),
    "GA": ConverterItem(EODHD, BBG, EODHD, ".AT"),
    "GR": ConverterItem(EODHD, BBG, EODHD, ".XETRA"),
    "GY": ConverterItem(IGNORE),
    "HB": ConverterItem(EODHD, BBG, EODHD, ".BUD"),
    "HK": ConverterItem(EODHD, FIXUPHK, EODHD, ".HK"),
    "ID": ConverterItem(EODHD, YAHOO, EODHD, ".IR"),
    "IJ": ConverterItem(EODHD, BBG, EODHD, ".JK"),
    "IM": ConverterItem(EODHD, BBG, EODHD, ".MI"),
    "IT": ConverterItem(EODHD, BBG, EODHD, ".TA"),
    "JP": ConverterItem(YAHOO, YAHOO, YAHOO),
    "JX": ConverterItem(IGNORE),
    "KS": ConverterItem(EODHD, BBG, EODHD, ".KQ"),
    "LN": ConverterItem(EODHD, BBG, EODHD, ".LSE"),
    "MF": ConverterItem(EODHD, BBG, EODHD, ".MX"),
    "MK": ConverterItem(EODHD, YAHOO, EODHD, ".KLSE"),
    "NA": ConverterItem(EODHD, BBG, EODHD, ".AS"),
    "NO": ConverterItem(EODHD, BBG, EODHD, ".OL"),
    "NZ": ConverterItem(YAHOO, YAHOO, YAHOO),
    "PL": ConverterItem(EODHD, BBG, EODHD, ".LS"),
    "PM": ConverterItem(EODHD, BBG, EODHD, ".PSE"),
    "PW": ConverterItem(EODHD, BBG, EODHD, ".WAR"),
    "SJ": ConverterItem(EODHD, BBG, EODHD, ".JSE"),
    "SM": ConverterItem(EODHD, BBG, EODHD, ".MC"),
    "SP": ConverterItem(EODHD, YAHOO, EODHD, ".SG"),
    "SS": ConverterItem(EODHD, YAHOO, EODHD, ".ST"),
    "SW": ConverterItem(EODHD, BBG, EODHD, ".SW"),
    "TB": ConverterItem(EODHD, BBG, EODHD, ".BK"),
    "TI": ConverterItem(EODHD, BBG, EODHD, ".IS"),
    "TT": ConverterItem(EODHD, BBG, YAHOO, ".TW"),
    "TW": ConverterItem(IGNORE),
    "UQ": ConverterItem(IGNORE),
    "US": ConverterItem(EODHD, BBG, EODHD, ".US"),
}

replacements = [
    ("-U.TO", "-UN.TO"),
    ("/P.MC", "-P.MC"),
    ("/2.US", ".US"),
    ("/B.", "-B."),
    ("/A.", "-A."),
    ("/X.", "-X."),
    ("//.", "."),
    ("/.", "."),
    ("*.MX", ".MX"),
]


def get_historical_universe_bbg() -> set:
    _logger.info(f"get_historical_universe_bbg, reading from network")

    historical_targets = pd.read_csv(SIGNALS_TARGETS)
    universe = set(historical_targets.bloomberg_ticker)

    _logger.info(f"get_historical_universe_bbg, unique symbols:{len(universe)}")

    return universe


def get_live_universe_bbg() -> set:
    _logger.info(f"get_live_universe_bbg, reading from network")

    universe = set(pd.read_csv(SIGNALS_UNIVERSE).squeeze("columns"))

    _logger.info(f"get_live_universe_bbg, unique symbols:{len(universe)}")

    return universe


def get_yahoo_map() -> tuple[dict, dict]:
    _logger.info(f"get_yahoo_map, reading from network")

    ticker_map = (
        pd.read_csv(SIGNALS_TICKER_MAP)
        .dropna()
        .apply(lambda x: x.astype(str).str.upper())
        .drop_duplicates(subset="yahoo")
    )

    # Dictionaries for bbg to yahoo and back conversions
    map_bbg_to_yahoo = dict(zip(ticker_map["bloomberg_ticker"], ticker_map["yahoo"]))
    map_yahoo_to_bbg = dict(zip(ticker_map["yahoo"], ticker_map["bloomberg_ticker"]))

    _logger.info(f"get_yahoo_map, unique symbols:{len(map_bbg_to_yahoo)}")

    return map_bbg_to_yahoo, map_yahoo_to_bbg


def build_eodhd_map(universe_bbg: set) -> pd.DataFrame:
    map_bbg_to_yahoo, _ = get_yahoo_map()

    overrides_df = pd.read_csv(OVERRIDE_DB)
    overrides_dict = dict(zip(overrides_df["old"], overrides_df["new"]))
    _logger.info(f"overrides_dict, count:{len(overrides_dict)}")

    eodhd_map_list = []
    for ticker in universe_bbg:
        # tickers without a recognized converter or exchange code will be marked IGNORE
        signals_ticker = ""
        data_provider = IGNORE

        # bloomberg_ticker suffix is the exchange code
        bbg_suffix = ticker.rpartition(" ")[2]

        converter = converters.get(bbg_suffix)
        if converter is not None:
            data_provider = converter.data_provider

            # First, build the signals_ticker from bloomberg_ticker using converters
            if converter.data_provider == IGNORE:
                pass
            elif converter.data_provider == YAHOO and ticker in map_bbg_to_yahoo:
                signals_ticker = map_bbg_to_yahoo[ticker]
            elif converter.data_provider == EODHD:
                if converter.ticker_source == BBG:
                    prefix = ticker.rpartition(" ")[0]
                elif converter.ticker_source == YAHOO and ticker in map_bbg_to_yahoo:
                    prefix = map_bbg_to_yahoo[ticker].rpartition(".")[0]
                elif converter.ticker_source == FIXUPHK:
                    prefix = ticker.rpartition(" ")[0].zfill(4)
                else:
                    prefix = ticker.rpartition(" ")[0]

                if converter.suffix_source == EODHD:
                    suffix = converter.eodhd_suffix
                elif converter.suffix_source == YAHOO and ticker in map_bbg_to_yahoo:
                    suffix = "." + map_bbg_to_yahoo[ticker].rpartition(".")[2]
                else:
                    suffix = converter.eodhd_suffix

                signals_ticker = prefix + suffix

            # Second, clean the resulting ticker with replacement rules
            for a_tuple in replacements:
                signals_ticker = signals_ticker.replace(a_tuple[0], a_tuple[1])

            # Third, apply manual overrides
            if signals_ticker in overrides_dict:
                signals_ticker = overrides_dict[signals_ticker]

        # add liam's yahoo ticker to the final map for comparison and debugging
        yahoo_ticker = map_bbg_to_yahoo.get(ticker, "")

        # Add entry to the map
        eodhd_map_list.append(
            {
                "bloomberg_ticker": ticker,
                "yahoo": yahoo_ticker,
                "data_provider": data_provider,
                "signals_ticker": signals_ticker,
            }
        )

    map = pd.DataFrame(eodhd_map_list).set_index("bloomberg_ticker")

    return map


def main():
    historical_bbg = get_historical_universe_bbg()
    live_bbg = get_live_universe_bbg()
    universe_bbg = historical_bbg.union(live_bbg)
    _logger.info(f"complete universe_bbg, unique symbols:{len(universe_bbg)}")

    map = build_eodhd_map(universe_bbg)
    map.to_csv(MAP_EXPORT, index=True, header=True)

    _logger.info(f"saved: {MAP_EXPORT}, rows:{len(map)}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()
