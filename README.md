# NDJSON to CSV Converter

Python script to convert large NDJSON (.json or .json.gz) files into CSV.  
Supports flattening nested fields and exploding list columns. Works as a general tool for any NDJSON dataset.

## Features
- Streams line by line for multi-GB files
- Works with .json and .json.gz
- Flatten nested fields into dotted columns
- Explode a single list column or explode all list columns (cartesian product)
- Progress logs while running

## Usage
Plain conversion  
python convertJsonToCSV.py -i input.json -o output.csv

Flatten nested objects  
python convertJsonToCSV.py -i input.json -o output.csv --flatten

Explode a single list column (example: tags)  
python convertJsonToCSV.py -i input.json -o output_exploded.csv --flatten --explode-column tags

Explode all list columns (cartesian product)  
python convertJsonToCSV.py -i input.json -o output_all_exploded.csv --flatten --explode-all

Faster header discovery (scan first N lines)  
python convertJsonToCSV.py -i input.json -o output.csv --flatten --discover-limit 200000

Compressed output (write .csv.gz by using .gz extension)  
python convertJsonToCSV.py -i input.json -o output.csv.gz --flatten --explode-all

## Examples folder
run_plain.sh shows plain conversion  
run_flatten.sh shows flattening  
run_explode.sh shows single column explode  
run_explode_all.sh shows explode all list columns

## Sample data
A small sample file exists at sample_data/sample.ndjson and sample_data/sample_multi.ndjson.

## How to test
Run the smoke test from project root  
python -m unittest tests/smoke_test.py

## Notes
Exploding all list columns multiplies rows by the size of each list. Use with care on very large records.
