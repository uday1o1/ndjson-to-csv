import argparse
import csv
import gzip
import io
import json
import os
import sys

# flatten a nested dict into one level using dotted keys
def flatten_dict(obj, parent_key='', sep='.', explode_key=None):
    # if the root is not a dict, make it a simple dict
    if not isinstance(obj, dict):
        return {parent_key or 'value': obj}
    # hold flattened items
    items = []
    # walk through each key/value
    for k, v in obj.items():
        # build the dotted key
        new_key = f'{parent_key}{sep}{k}' if parent_key else k
        # if value is dict, flatten it
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep, explode_key=explode_key).items())
        # if value is list and this is the explode target, keep list as-is
        elif isinstance(v, list) and explode_key and new_key == explode_key:
            items.append((new_key, v))
        # if value is list and not explode target, keep it as a json string so csv stays rectangular
        elif isinstance(v, list):
            items.append((new_key, json.dumps(v, ensure_ascii=False)))
        # otherwise keep the value
        else:
            items.append((new_key, v))
    # return flattened dict
    return dict(items)

# open a text file for reading, works with .gz or plain
def open_text_read(path):
    # if ends with .gz use gzip text mode
    if path.lower().endswith('.gz'):
        return gzip.open(path, 'rt', encoding='utf-8', newline='')
    # otherwise open regular text
    return open(path, 'r', encoding='utf-8', newline='')

# open a text file for writing, can gzip if path ends with .gz
def open_text_write(path):
    # if ends with .gz, open gzip in binary then wrap to text
    if path.lower().endswith('.gz'):
        gz = gzip.open(path, 'wb')
        return io.TextIOWrapper(gz, encoding='utf-8', newline='')
    # otherwise open regular text
    return open(path, 'w', encoding='utf-8', newline='')

# yield non-empty lines from an ndjson file
def iter_ndjson_lines(path):
    # open the file for text reading
    with open_text_read(path) as f:
        # read line by line
        for line in f:
            # strip whitespace
            s = line.strip()
            # skip empty lines
            if not s:
                continue
            # yield the clean line
            yield s

# discover all columns by scanning the file once
def discover_columns(src_path, flatten, explode_key=None, limit=None, progress_every=200000):
    # keep a set of column names
    keys = set()
    # count scanned lines
    count = 0
    # go over each line
    for count, line in enumerate(iter_ndjson_lines(src_path), start=1):
        # parse json for this line
        try:
            rec = json.loads(line)
        except json.JSONDecodeError as e:
            raise RuntimeError(f'json decode error on line {count}: {e}') from e
        # force record to dict if not dict
        if not isinstance(rec, dict):
            rec = {'value': rec}
        # flatten if asked
        if flatten:
            rec = flatten_dict(rec, explode_key=explode_key)
        # add keys to set
        keys.update(rec.keys())
        # show progress if needed
        if progress_every and count % progress_every == 0:
            print(f'[pass1] scanned {count:,} lines, found {len(keys):,} columns', file=sys.stderr)
        # stop early if limit set
        if limit and count >= limit:
            print(f'[pass1] stopped at discovery limit {limit:,} lines', file=sys.stderr)
            break
    # if no lines found, raise
    if count == 0:
        raise RuntimeError('no lines found. is this file empty or not ndjson?')
    # sort columns for stable order
    cols = sorted(keys)
    # print summary
    print(f'[pass1] done. lines: {count:,}, columns: {len(cols):,}', file=sys.stderr)
    # return columns list
    return cols

# write the csv by streaming the file again
def write_csv(src_path, dst_path, columns, flatten, explode_key=None, progress_every=200000):
    # make sure the output folder exists
    os.makedirs(os.path.dirname(os.path.abspath(dst_path)) or '.', exist_ok=True)
    # open output csv (gz if .gz extension used)
    with open_text_write(dst_path) as out_f:
        # create csv writer
        writer = csv.writer(out_f)
        # write header
        writer.writerow(columns)
        # count written rows
        written = 0
        # read ndjson again
        for i, line in enumerate(iter_ndjson_lines(src_path), start=1):
            # parse json for this line
            try:
                rec = json.loads(line)
            except json.JSONDecodeError as e:
                raise RuntimeError(f'json decode error on line {i}: {e}') from e
            # force dict
            if not isinstance(rec, dict):
                rec = {'value': rec}
            # flatten if asked
            if flatten:
                rec = flatten_dict(rec, explode_key=explode_key)
            # if explode is enabled, duplicate rows for each list item
            if explode_key:
                # get the explode value
                ex_val = rec.get(explode_key, None)
                # if it's a string that looks like json, try to load it
                if isinstance(ex_val, str) and ex_val.startswith('[') and ex_val.endswith(']'):
                    try:
                        ex_val = json.loads(ex_val)
                    except Exception:
                        ex_val = [ex_val]
                # if it's None, write a single row with empty value
                if ex_val is None:
                    # build row with empty explode col
                    row = [rec.get(col, '') if col != explode_key else '' for col in columns]
                    writer.writerow(row)
                    written += 1
                # if it's a list, write one row per item
                elif isinstance(ex_val, list):
                    # write rows for each item
                    for item in ex_val:
                        # convert non-string items to string for csv
                        val = item if isinstance(item, str) else json.dumps(item, ensure_ascii=False) if isinstance(item, (dict, list)) else item
                        # build row with this item
                        row = [rec.get(col, '') if col != explode_key else val for col in columns]
                        writer.writerow(row)
                        written += 1
                # otherwise write a single row with scalar value
                else:
                    # build row with scalar
                    row = [rec.get(col, '') if col != explode_key else ex_val for col in columns]
                    writer.writerow(row)
                    written += 1
            # if not exploding, write a single row
            else:
                # map to row with missing keys as empty string
                row = [rec.get(col, '') for col in columns]
                writer.writerow(row)
                written += 1
            # progress print
            if progress_every and written % progress_every == 0:
                print(f'[pass2] wrote {written:,} rows', file=sys.stderr)
    # final summary
    print(f'[pass2] finished. total rows: {written:,}. output: {dst_path}', file=sys.stderr)

# main function for command line
def main():
    # set up arguments
    parser = argparse.ArgumentParser(
        description='convert large ndjson (.json or .json.gz) to csv, streaming and memory safe'
    )
    # input path
    parser.add_argument('-i', '--input', required=True, help='path to ndjson input (.json or .json.gz)')
    # output path
    parser.add_argument('-o', '--output', required=True, help='path to csv output (.csv or .csv.gz)')
    # flatten flag
    parser.add_argument('--flatten', action='store_true', help='flatten nested objects to dotted columns')
    # explode column name
    parser.add_argument('--explode-column', default=None, help='column to explode into multiple rows (e.g. "tags")')
    # discovery limit
    parser.add_argument('--discover-limit', type=int, default=None, help='scan only first N lines to build header')
    # progress interval
    parser.add_argument('--progress-every', type=int, default=200000, help='print progress every N rows (0 to disable)')
    # parse args
    args = parser.parse_args()

    # check file exists
    if not os.path.exists(args.input):
        print(f'input not found: {args.input}', file=sys.stderr)
        sys.exit(1)

    # set explode key or None
    explode_key = args.explode_column

    # discover columns
    columns = discover_columns(
        src_path=args.input,
        flatten=args.flatten,
        explode_key=explode_key,
        limit=args.discover_limit,
        progress_every=args.progress_every
    )

    # write csv
    write_csv(
        src_path=args.input,
        dst_path=args.output,
        columns=columns,
        flatten=args.flatten,
        explode_key=explode_key,
        progress_every=args.progress_every
    )

if __name__ == '__main__':
    main()