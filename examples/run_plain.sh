set -euo pipefail
python3 ./convertJsonToCSV.py -i ./sample_data/sample.ndjson -o ./sample_data/out_plain.csv
echo "wrote: sample_data/out_plain.csv"
