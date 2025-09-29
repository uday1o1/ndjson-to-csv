# import standard libraries
import argparse
import csv
import gzip
import io
import json
import os
import sys
from itertools import product

# flatten a nested dict into one level using dotted keys
def flatten_dict(obj, parent_key='', sep='.', keep_lists_for=None, explode_all=False):
    # if the root is not a dict, make it a simple dict
    if not isinstance(obj, dict):
        return {parent_key or 'value': obj}
    # hold flattened items
    items = []
    # define which keys should stay as lists
    keep_lists_for = set(keep_lists_for or [])
    # walk through each key/value
    for k, v in obj.items():
        # build the dotted key
        new_key = f'{parent_key}{sep}{k}' if parent_key else k
        # if value is dict, flatten it
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep, keep_lists_for=keep_lists_for, explode_all=explode_all).items())
        # if value is list
        elif isinstance(v, list):
            # if explode_all, keep lists as lists
            if explode_all:
                items.append((new_key, v))
            # if single-column explode requested for this exact key, keep as list
            elif new_key in keep_lists_for:
                items.append((new_key, v))
            # otherwise keep as json string so csv stays rectangular
            else:
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

# normalize a value for csv cell
def to_csv_cell(val):
    # return empty string for None
    if val is None:
        return ''
    # if list or dict, serialize
    if isinstance(val, (list, dict)):
        return json.dumps(val, ensure_ascii=False)
    # otherwise return as-is
    return val

# expand one flattened record by exploding lists
def expand_record(rec, explode_keys):
    # if no explode keys, just yield single row
    if not explode_keys:
        yield rec
        return
    # build base dict of non-exploded keys
    base = {k: v for k, v in rec.items() if k not in explode_keys}
    # build a list of value lists for each explode key
    value_lists = []
    keys_order = []
    for k in explode_keys:
        val = rec.get(k, None)
        # if missing -> treat as one empty value
        if val is None:
            value_lists.append([''])
        # if a string that looks like json array -> try to parse
        elif isinstance(val, str) and val.startswith('[') and val.endswith(']'):
            try:
                parsed = json.loads(val)
                value_lists.append(parsed if isinstance(parsed, list) else [parsed])
            except Exception:
                value_lists.append([val])
        # if already a list -> use it
        elif isinstance(val, list):
            # handle empty list by emitting one empty row to avoid dropping the record
            value_lists.append(val if len(val) > 0 else [''])
        # otherwise scalar
        else:
            value_lists.append([val])
        keys_order.append(k)
    # produce cartesian product
    for combo in product(*value_lists):
        row = dict(base)
        for k, v in zip(keys_order, combo):
            row[k] = v
        yield row

# discover all columns by scanning the file once
def discover_columns(src_path, flatten, explode_key=None, explode_all=False, limit=None, progress_every=200000):
    # keep a set of column names
    keys = set()
    # count scanned lines
    count = 0
    # set list-preserving behavior
    keep_lists_for = [explode_key] if explode_key else []
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
            rec = flatten_dict(rec, keep_lists_for=keep_lists_for, explode_all=explode_all)
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
def write_csv(src_path, dst_path, columns, flatten, explode_key=None, explode_all=False, progress_every=200000):
    # make sure the output folder exists
    os.makedirs(os.path.dirname(os.path.abspath(dst_path)) or '.', exist_ok=True)
    # set list-preserving behavior
    keep_lists_for = [explode_key] if explode_key else []
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
                rec = flatten_dict(rec, keep_lists_for=keep_lists_for, explode_all=explode_all)
            # decide which keys to explode
            if explode_all:
                explode_keys = [k for k, v in rec.items() if isinstance(v, list)]
            elif explode_key:
                explode_keys = [explode_key]
            else:
                explode_keys = []
            # expand and write rows
            for expanded in expand_record(rec, explode_keys):
                row = [to_csv_cell(expanded.get(col, '')) for col in columns]
                writer.writerow(row)
                written += 1
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
    # explode one column
    parser.add_argument('--explode-column', default=None, help='explode a single list column (e.g. "tags")')
    # explode all list columns
    parser.add_argument('--explode-all', action='store_true', help='explode all list columns (cartesian product)')
    # discovery limit
    parser.add_argument('--discover-limit', type=int, default=None, help='scan only first N lines to build header')
    # progress interval
    parser.add_argument('--progress-every', type=int, default=200000, help='print progress every N rows (0 to disable)')
    # parse args
    args = parser.parse_args()

    # forbid conflicting explode settings
    if args.explode_column and args.explode_all:
        print('choose either --explode-column or --explode-all, not both', file=sys.stderr)
        sys.exit(1)

    # check file exists
    if not os.path.exists(args.input):
        print(f'input not found: {args.input}', file=sys.stderr)
        sys.exit(1)

    # discover columns
    columns = discover_columns(
        src_path=args.input,
        flatten=args.flatten,
        explode_key=args.explode_column,
        explode_all=args.explode_all,
        limit=args.discover_limit,
        progress_every=args.progress_every
    )

    # write csv
    write_csv(
        src_path=args.input,
        dst_path=args.output,
        columns=columns,
        flatten=args.flatten,
        explode_key=args.explode_column,
        explode_all=args.explode_all,
        progress_every=args.progress_every
    )

if __name__ == '__main__':
    main()