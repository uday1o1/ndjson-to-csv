set -euo pipefail
python3 ./convertJsonToCSV.py -i ./sample_data/sample_multi.ndjson -o ./sample_data/out_exploded_all.csv --flatten --explode-all
echo "wrote: sample_data/out_exploded_all.csv"
