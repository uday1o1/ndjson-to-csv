# NDJSON to CSV Converter

This is a Python script to convert very large NDJSON (.json or .json.gz) files into CSV.
It can flatten nested objects and also explode list columns into multiple rows.

## Features

* Works with .json and .json.gz
* Streams line by line (safe for multi-GB files)
* Flatten nested fields into dotted columns (example: song.artist, song.track)
* Optionally explode one list column into multiple rows
* Progress logs while running

## Usage

Run the script with python. Required arguments are input and output paths.

Examples:

1. Plain conversion
   python convertJsonToCSV.py -i full_dataset.json -o full_dataset.csv

2. Flatten nested objects
   python convertJsonToCSV.py -i full_dataset.json -o full_dataset.csv --flatten

3. Flatten and explode the tags column
   python convertJsonToCSV.py -i full_dataset.json -o full_dataset_exploded.csv --flatten --explode-column tags

4. Faster header discovery (scan only first 200k lines)
   python convertJsonToCSV.py -i full_dataset.json -o full_dataset.csv --flatten --discover-limit 200000

## How to test

A small smoke test is included to check that the script works in plain, flatten, and explode modes.
Run it from the project root with:

python -m unittest tests/smoke_test.py

If everything is correct, you will see the tests run and finish with OK.

---

## Examples folder

* run_plain.sh shows plain conversion
* run_flatten.sh shows flattening
* run_explode.sh shows flatten plus exploding a list column

## Sample data

A small sample file is included in sample_data/sample.ndjson. Running the example scripts will create CSV outputs there.

Got it 👍 Here’s a simple **How to test** section you can add to the end of your README:

---
