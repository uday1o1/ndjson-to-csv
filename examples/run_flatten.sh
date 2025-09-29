set -euo pipefail
python3 ./convertJsonToCSV.py -i ./sample_data/sample.ndjson -o ./sample_data/out_flat.csv --flatten
echo "wrote: sample_data/out_flat.csv"
