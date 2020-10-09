import argparse
import logging

logging.basicConfig(level=logging.INFO)

from urllib.parse import urlparse

import pandas as pd


logger = logging.getLogger(__name__)


def main(filename):
    logger.info("Starting cleaning process")

    df = _read_data(filename)
    newspaper_uid = _extract_newspaper_uid(filename)
    df = _add_newspaper_uid_column(df, newspaper_uid)
    df = _extract_host(df)
    df = _fill_missing_titles(df)

    return df


def _extract_host(df):
    logger.info("Extracting host from urls")
    df["host"] = df["url"].apply(lambda url: urlparse(url).netloc)

    return df


def _add_newspaper_uid_column(df, newspaper_uid):
    logger.info("Filling newspaper uid column with {}".format(newspaper_uid))
    df["newspaper_uid"] = newspaper_uid

    return df


def _extract_newspaper_uid(filename):
    logger.info("Extracting Newspaper UID")
    newspaper_uid = filename.split("_")[0]

    logger.info("Newspaper iud detected {}".format(newspaper_uid))
    return newspaper_uid


def _read_data(filename):
    logger.info("Reading file{}".format(filename))

    return pd.read_csv(filename)


def _fill_missing_titles(df):
    logger.info("Filling missing titles")
    missing_title_mask = df["title"].isna()

    missing_titles = (
        df[missing_title_mask]["url"]
        .str.extract(r"(?P<missing_titles>[^/]+)$")
        .applymap(lambda title: title.split("-"))
        .applymap(lambda title_word_list: " ".join(title_word_list))
    )
    df.loc[missing_title_mask, "title"] = missing_titles.loc[:, "missing_titles"]

    return df


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("filename", help="The path to the dirty data", type=str)

    args = parser.parse_args()

    df = main(args.filename)
    print(df)