import glob
import json
import os
from pathlib import Path
from typing import Optional

import pandas as pd
from pandas import DataFrame

from src.constants import COLUMN_NAMES


def parse_heart_rate_json_all(path_src: str) -> DataFrame:
    list_paths_heart_rate_json = sorted(
        glob.glob(os.path.join(path_src, "heart_rate-*.json"))
    )

    if len(list_paths_heart_rate_json) == 0:
        raise FileNotFoundError("No matching files found")

    df = pd.DataFrame(columns=["dateTime", "bpm", "confidence"])

    df_single_list = []

    for path in list_paths_heart_rate_json:
        with open(path, "r") as f:
            df_single = _parse_heart_rate_json_single(json.load(f))
        df_single_list.append(df_single)

    df = df.append(df_single_list).reset_index(drop=True)
    df["dateTime"] = pd.to_datetime(df["dateTime"], format="%m/%d/%y %H:%M:%S")
    df["dateTime"] = df["dateTime"].dt.tz_localize(None)
    df = df.astype({"bpm": "int32", "confidence": "int32"})

    return df


def _parse_heart_rate_json_single(json_object: dict) -> DataFrame:
    if len(json_object) == 0:
        raise IndexError("JSON of length 0")

    json_object_transformed = []
    for elem in json_object:
        new_elem = {
            "dateTime": elem["dateTime"],
            "bpm": elem["value"]["bpm"],
            "confidence": elem["value"]["confidence"],
        }
        json_object_transformed.append(new_elem)

    return pd.DataFrame(json_object_transformed)


def parse_sleep_score_csv(path_src: str) -> DataFrame:
    """
    Parse sleep_score.csv
    :param path_src: path to sleep_score.csv
    :return: a Pandas DataFrame
    """
    file = Path(path_src)
    if not file.is_file():
        raise FileNotFoundError(
            "sleep_score.csv not found in path_src: {}".format(path_src)
        )

    df = pd.read_csv(path_src)

    # Convert to datetime and to naive local time
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["timestamp"] = df["timestamp"].dt.tz_localize(None)

    return df


def _parse_sleep_json(path_src: str) -> (DataFrame, DataFrame):
    """
    Parse a single sleep JSON file
    :param path_src: path to the sleep JSON file
    :return: a Pandas DataFrame
    """
    file = Path(path_src)
    if not file.is_file():
        raise FileNotFoundError(
            "sleep data JSON file not found in path_src: {}".format(path_src)
        )

    with open(path_src, "r") as f:
        json_file = json.load(f)

    if len(json_file) == 0:
        raise IndexError("JSON of length 0")

    df_all = pd.DataFrame(columns=COLUMN_NAMES)
    df_list_main = []
    dict_df_ts = {}

    for json_elem in json_file:
        df_main, df_ts = _parse_sleep_json_single(json_elem)
        if df_main is None and df_ts is None:
            continue
        df_ts = df_ts.astype({"level": "string", "seconds": "int32"})
        df_list_main.append(df_main)
        dict_df_ts[df_main["dateOfSleep"][0]] = df_ts

    df_all = df_all.append(df_list_main).reset_index(drop=True)

    return df_all, dict_df_ts


def _parse_sleep_json_single(
    json_object: dict,
) -> (Optional[DataFrame], Optional[DataFrame]):
    """
    Parse a single json object containing sleep data
    :param json_object: dict containing the sleep data
    :return: Pandas DataFrame
    """
    # Skip if type: classic
    if json_object["type"] == "classic":
        return None, None

    # Overall
    dateOfSleep = json_object["dateOfSleep"]
    minutesAsleep = json_object["minutesAsleep"]
    minutesAwake = json_object["minutesAwake"]
    timeInBed = json_object["timeInBed"]
    efficiency = json_object["efficiency"]
    summary = json_object["levels"]["summary"]

    # Deep sleep
    deepCount = summary["deep"]["count"]
    deepMinutes = summary["deep"]["minutes"]
    deepThirtyDayAvgMinutes = summary["deep"]["thirtyDayAvgMinutes"]

    # Light sleep
    lightCount = summary["light"]["count"]
    lightMinutes = summary["light"]["minutes"]
    lightThirtyDayAvgMinutes = summary["light"]["thirtyDayAvgMinutes"]

    # REM sleep
    remCount = summary["rem"]["count"]
    remMinutes = summary["rem"]["minutes"]
    remThirtyDayAvgMinutes = summary["rem"]["thirtyDayAvgMinutes"]

    # Awake
    wakeCount = summary["wake"]["count"]
    wakeMinutes = summary["wake"]["minutes"]
    wakeThirtyDayAvgMinutes = summary["wake"]["thirtyDayAvgMinutes"]

    # Detailed data - create a small individual dataframe
    df_ts = pd.read_json(json.dumps(json_object["levels"]["data"]))

    # Create the "main" dataframe
    df_main = pd.DataFrame(
        {
            "dateOfSleep": dateOfSleep,
            "minutesAsleep": minutesAsleep,
            "minutesAwake": minutesAwake,
            "timeInBed": timeInBed,
            "efficiency": efficiency,
            "deepCount": deepCount,
            "deepMinutes": deepMinutes,
            "deepThirtyDayAvgMinutes": deepThirtyDayAvgMinutes,
            "lightCount": lightCount,
            "lightMinutes": lightMinutes,
            "lightThirtyDayAvgMinutes": lightThirtyDayAvgMinutes,
            "remCount": remCount,
            "remMinutes": remMinutes,
            "remThirtyDayAvgMinutes": remThirtyDayAvgMinutes,
            "wakeCount": wakeCount,
            "wakeMinutes": wakeMinutes,
            "wakeThirtyDayAvgMinutes": wakeThirtyDayAvgMinutes,
        },
        index=[0],
    )

    return df_main, df_ts


def parse_sleep_json_all(path_src: str) -> (DataFrame, dict):
    # Parse all sleep JSON files
    df_all = pd.DataFrame(columns=COLUMN_NAMES)
    df_list = []
    dict_ts_all = {}

    list_paths_sleep_json = glob.glob(os.path.join(path_src, "sleep-*.json"))

    if len(list_paths_sleep_json) == 0:
        raise FileNotFoundError("No matching files found")

    for json_path in list_paths_sleep_json:
        df_main, dict_ts = _parse_sleep_json(json_path)
        df_list.append(df_main)
        dict_ts_all.update(dict_ts)

    df_all = df_all.append(df_list)

    # Deduplicate rows, as some identical days are counted in two different JSONs.
    # Keep last row, as df is sorted by ascending avg minutes
    df_all = df_all.sort_values(
        ["dateOfSleep", "deepThirtyDayAvgMinutes"]
    ).drop_duplicates("dateOfSleep", keep="last")

    df_all["dateOfSleep"] = pd.to_datetime(df_all["dateOfSleep"], format="%Y-%m-%d")
    df_all["dateOfSleep"] = df_all["dateOfSleep"].dt.tz_localize(None)

    df_all = df_all.astype(
        {
            "minutesAsleep": "int32",
            "minutesAwake": "int32",
            "timeInBed": "int32",
            "efficiency": "int32",
            "deepCount": "int32",
            "deepMinutes": "int32",
            "deepThirtyDayAvgMinutes": "int32",
            "lightCount": "int32",
            "lightMinutes": "int32",
            "lightThirtyDayAvgMinutes": "int32",
            "remCount": "int32",
            "remMinutes": "int32",
            "remThirtyDayAvgMinutes": "int32",
            "wakeCount": "int32",
            "wakeMinutes": "int32",
            "wakeThirtyDayAvgMinutes": "int32",
        }
    )

    return df_all, dict_ts_all
