import logging
import collections
import pandas as pd

from pathlib import Path

_logger = logging.getLogger(__name__)

MAP_IGNORE = "ignore"
MAP_YAHOO = "yahoo"
MAP_EODHD = "eodhd"

_BBG = "bbg"
_FIXUPHK = "fixuphk"

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
        "data_provider",  # {eodhd | yahoo | clean}
        "ticker_source",  # [bbg | yahoo | fixuphk]
        "suffix_source",  # [eodhd | yahoo]
        "eodhd_suffix",
    ],
    defaults=(MAP_EODHD, _BBG, MAP_EODHD, ""),
)

converters = {
    "AU": ConverterItem(MAP_EODHD, _BBG, MAP_EODHD, ".AU"),
    "AV": ConverterItem(MAP_EODHD, _BBG, MAP_EODHD, ".VI"),
    "BB": ConverterItem(MAP_EODHD, _BBG, MAP_EODHD, ".BR"),
    "BZ": ConverterItem(MAP_EODHD, _BBG, MAP_EODHD, ".SA"),
    "CA": ConverterItem(MAP_IGNORE),
    "CH": ConverterItem(MAP_IGNORE),
    "CN": ConverterItem(MAP_EODHD, _BBG, MAP_EODHD, ".TO"),
    "CP": ConverterItem(MAP_YAHOO, MAP_YAHOO, MAP_YAHOO),
    "DC": ConverterItem(MAP_EODHD, _BBG, MAP_EODHD, ".CO"),
    "FH": ConverterItem(MAP_EODHD, _BBG, MAP_EODHD, ".HE"),
    "FP": ConverterItem(MAP_EODHD, _BBG, MAP_EODHD, ".PA"),
    "GA": ConverterItem(MAP_EODHD, _BBG, MAP_EODHD, ".AT"),
    "GR": ConverterItem(MAP_EODHD, _BBG, MAP_EODHD, ".XETRA"),
    "GY": ConverterItem(MAP_IGNORE),
    "HB": ConverterItem(MAP_EODHD, _BBG, MAP_EODHD, ".BUD"),
    "HK": ConverterItem(MAP_EODHD, _FIXUPHK, MAP_EODHD, ".HK"),
    "ID": ConverterItem(MAP_EODHD, MAP_YAHOO, MAP_EODHD, ".IR"),
    "IJ": ConverterItem(MAP_EODHD, _BBG, MAP_EODHD, ".JK"),
    "IM": ConverterItem(MAP_EODHD, _BBG, MAP_EODHD, ".MI"),
    "IT": ConverterItem(MAP_EODHD, _BBG, MAP_EODHD, ".TA"),
    "JP": ConverterItem(MAP_YAHOO, MAP_YAHOO, MAP_YAHOO),
    "JX": ConverterItem(MAP_IGNORE),
    "KS": ConverterItem(MAP_EODHD, _BBG, MAP_EODHD, ".KQ"),
    "LN": ConverterItem(MAP_EODHD, _BBG, MAP_EODHD, ".LSE"),
    "MF": ConverterItem(MAP_EODHD, _BBG, MAP_EODHD, ".MX"),
    "MK": ConverterItem(MAP_EODHD, MAP_YAHOO, MAP_EODHD, ".KLSE"),
    "NA": ConverterItem(MAP_EODHD, _BBG, MAP_EODHD, ".AS"),
    "NO": ConverterItem(MAP_EODHD, _BBG, MAP_EODHD, ".OL"),
    "NZ": ConverterItem(MAP_YAHOO, MAP_YAHOO, MAP_YAHOO),
    "PL": ConverterItem(MAP_EODHD, _BBG, MAP_EODHD, ".LS"),
    "PM": ConverterItem(MAP_EODHD, _BBG, MAP_EODHD, ".PSE"),
    "PW": ConverterItem(MAP_EODHD, _BBG, MAP_EODHD, ".WAR"),
    "SJ": ConverterItem(MAP_EODHD, _BBG, MAP_EODHD, ".JSE"),
    "SM": ConverterItem(MAP_EODHD, _BBG, MAP_EODHD, ".MC"),
    "SP": ConverterItem(MAP_EODHD, MAP_YAHOO, MAP_EODHD, ".SG"),
    "SS": ConverterItem(MAP_EODHD, MAP_YAHOO, MAP_EODHD, ".ST"),
    "SW": ConverterItem(MAP_EODHD, _BBG, MAP_EODHD, ".SW"),
    "TB": ConverterItem(MAP_EODHD, _BBG, MAP_EODHD, ".BK"),
    "TI": ConverterItem(MAP_EODHD, _BBG, MAP_EODHD, ".IS"),
    "TT": ConverterItem(MAP_EODHD, _BBG, MAP_YAHOO, ".TW"),
    "TW": ConverterItem(MAP_IGNORE),
    "UQ": ConverterItem(MAP_IGNORE),
    "US": ConverterItem(MAP_EODHD, _BBG, MAP_EODHD, ".US"),
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


def _get_historical_universe_bbg():
    _logger.info(f"_get_historical_universe_bbg, read historical targets from network")

    historical_targets = pd.read_csv(SIGNALS_TARGETS)
    universe = historical_targets.bloomberg_ticker.unique()
    return universe


def _get_live_universe_bbg():
    _logger.info(f"_get_live_universe_bbg, read from network")

    universe = pd.read_csv(SIGNALS_UNIVERSE, squeeze=True).drop_duplicates()
    return universe


def _get_delisted_universe_bbg(historical_universe_bbg, live_universe_bbg):
    delisted_bbg_set = set(historical_universe_bbg) - set(live_universe_bbg)
    return delisted_bbg_set


def _get_complete_universe_bbg(historical_universe_bbg, live_universe_bbg):
    complete_universe_bbg = set(historical_universe_bbg).union(set(live_universe_bbg))
    return complete_universe_bbg


def _get_yahoo_map():
    _logger.info(f"_get_yahoo_map, read from network")
    ticker_map = (
        pd.read_csv(SIGNALS_TICKER_MAP)
        .dropna()
        .apply(lambda x: x.astype(str).str.upper())
        .drop_duplicates(subset="yahoo")
    )

    # Dictionaries for bbg to yahoo and back conversions
    map_bbg_to_yahoo = dict(zip(ticker_map["bloomberg_ticker"], ticker_map["yahoo"]))
    map_yahoo_to_bbg = dict(zip(ticker_map["yahoo"], ticker_map["bloomberg_ticker"]))

    return map_bbg_to_yahoo, map_yahoo_to_bbg


def build_eodhd_map(bbg_universe: set) -> pd.DataFrame:
    map_bbg_to_yahoo, _ = _get_yahoo_map()
    _logger.info(f"map_bbg_to_yahoo, unique symbols:{len(map_bbg_to_yahoo)}")

    overrides_df = pd.read_csv(OVERRIDE_DB)
    overrides_dict = dict(zip(overrides_df["old"], overrides_df["new"]))
    _logger.info(f"overrides_dict, count:{len(overrides_dict)}")

    eodhd_map_list = []
    for bbg_ticker in bbg_universe:
        # tickers without a recognized converter or exchange code will be marked IGNORE
        signals_ticker = ""
        data_provider = MAP_IGNORE

        # bloomberg_ticker suffix is the exchange code
        bbg_suffix = bbg_ticker.rpartition(" ")[2]

        converter = converters.get(bbg_suffix)
        if converter is not None:
            data_provider = converter.data_provider

            # First, build the signals_ticker from bloomberg_ticker using converters
            if converter.data_provider == MAP_IGNORE:
                pass
            elif (
                converter.data_provider == MAP_YAHOO and bbg_ticker in map_bbg_to_yahoo
            ):
                signals_ticker = map_bbg_to_yahoo[bbg_ticker]
            elif converter.data_provider == MAP_EODHD:
                if converter.ticker_source == _BBG:
                    prefix = bbg_ticker.rpartition(" ")[0]
                elif (
                    converter.ticker_source == MAP_YAHOO
                    and bbg_ticker in map_bbg_to_yahoo
                ):
                    prefix = map_bbg_to_yahoo[bbg_ticker].rpartition(".")[0]
                elif converter.ticker_source == _FIXUPHK:
                    prefix = bbg_ticker.rpartition(" ")[0].zfill(4)
                else:
                    prefix = bbg_ticker.rpartition(" ")[0]

                if converter.suffix_source == MAP_EODHD:
                    suffix = converter.eodhd_suffix
                elif (
                    converter.suffix_source == MAP_YAHOO
                    and bbg_ticker in map_bbg_to_yahoo
                ):
                    suffix = "." + map_bbg_to_yahoo[bbg_ticker].rpartition(".")[2]
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
        yahoo_ticker = (
            map_bbg_to_yahoo[bbg_ticker] if bbg_ticker in map_bbg_to_yahoo else ""
        )

        # Add entry to the map
        eodhd_map_list.append(
            {
                "bloomberg_ticker": bbg_ticker,
                "yahoo": yahoo_ticker,
                "data_provider": data_provider,
                "signals_ticker": signals_ticker,
            }
        )

    map = pd.DataFrame(eodhd_map_list).set_index("bloomberg_ticker")

    return map


def main():
    historical_bbg = _get_historical_universe_bbg()
    _logger.info(f"historical_universe_bbg, unique symbols:{len(historical_bbg)}")

    live_bbg = _get_live_universe_bbg()
    _logger.info(f"live_universe_bbg, unique symbols:{len(live_bbg)}")

    bbg_universe = _get_complete_universe_bbg(historical_bbg, live_bbg)
    _logger.info(f"complete_universe_bbg, unique symbols:{len(bbg_universe)}")

    map = build_eodhd_map(bbg_universe)
    map.to_csv(MAP_EXPORT, index=True, header=True)
    _logger.info(f"saved: {MAP_EXPORT}, rows:{len(map)}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()
