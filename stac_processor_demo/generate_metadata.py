from os import path
import csv
import random


def _to_snake(s: str):
    return "".join(["_" if c in [" ", "-"] else c for c in s]).strip("_").lower()


def generate_metadata():
    metadata_file = path.join(path.dirname(__file__), "metadata.csv")

    with open(metadata_file) as csvfile:
        metadata = list(csv.DictReader(csvfile))

    return {
        _to_snake(key): value
        for (key, value)
        in random.choice(metadata).items()
    }
