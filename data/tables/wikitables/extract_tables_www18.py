#!/usr/bin/env python
# -*- coding: utf-8 -*-


import re
import json
import gzip
import pathlib
import argparse
import os

import numpy as np


from tqdm import tqdm


def dir_path(path):
    target = pathlib.Path(path).resolve()
    try:
        if not target.exists():
            print(f"Creating directory {target}")
            target.mkdir(parents=True, exist_ok=True)
        return target
    except Exception as e:
        raise argparse.ArgumentTypeError(
            f"{target} is not a valid directory path: {e}")


def existing_file_path(path):
    target = pathlib.Path(path).resolve()
    if target.exists() and target.is_file():
        return target
    else:
        raise argparse.ArgumentTypeError(
            f"'{target}' is not a valid file path")


def generate_json_file_per_table(input_dir_raw, output_dir):
    '''
    Given the input directory with the raw json files as in www18_wikitables, generate a single json file for each table and save them in the output_dir
    '''

    print("Generating a json file for each table from the raw www18_wikitable json files...")
    files = sorted(os.listdir(input_dir_raw))
    for file in tqdm(files):
        with open(input_dir_raw + file) as f:
            raw_json = json.load(f)

        # Each 'raw_json' contains multiple tables. Create a separate json file for each table 
        for table in raw_json:
            # Save each table as a json file in the `output_dir`
            with open(output_dir + table + ".json", 'w') as f:
                json.dump(raw_json[table], f, indent=4)

    print("Finished generating a json file for each table from the raw www18_wikitable json files.")


def export(input_dir, output_folder, min_rows, max_rows, min_cols, keep_numeric=True, verbose=False):

    print("Generating a parsed json file for each table...")
    files = sorted(os.listdir(input_dir))

    with_numeric = 0
    with_1header = 0
    num_columns = []
    num_lines = []
    num_numeric_columns = []
    num_to_keep = 0

    p = re.compile(r'href=[\'"]?([^\'" >]+)')

    for file in tqdm(files):
        with open(input_dir + file) as f:
            table = json.load(f)

        _has_numeric = False
        _has_min_cols = False
        _has_1header = False

        if table['numCols'] >= min_cols:
            _has_min_cols = True

        if len(table['numericColumns']) >= 1:
            _has_numeric = True
            with_numeric += 1

        if table['numHeaderRows'] <= 1:
            _has_1header = True
            with_1header += 1

        # Check _has_numeric logic
        if (keep_numeric and not _has_numeric) or (not _has_1header) or (not _has_min_cols):
            continue

        _keep = False

        bigger_than = table['numDataRows'] >= min_rows

        if min_rows < max_rows:
            lower_than = table['numDataRows'] <= max_rows
            _keep = bigger_than and lower_than
        else:
            _keep = bigger_than

        if not _keep:
            continue

        num_to_keep += 1
        num_lines.append(table['numDataRows'])
        num_columns.append(table['numCols'])
        num_numeric_columns.append(len(table['numericColumns']))

        # The new parsed json file to be exported and saved in the `output_folder`
        _to_export = {}
        _to_export['_id'] = file.replace('table-', '').replace('.json', '')
        _to_export['pgId'] = _to_export['_id'].split('-', 1)[0]
        for attr in ['numCols', 'numDataRows', 'pgTitle', 'tableCaption']:
            _to_export[attr] = table.get(attr, '')
        _to_export['numNumericCols'] = num_numeric_columns[-1]
        _to_export['headers'] = []

        # Extract the header (in the www18_wikitables the header names are under the "title" key,
        # there is no info about links or isNumeric)
        for _header in table['title']:
            _to_export['headers'].append({
                'text': _header,
                'isNumeric': False,
                'links': []
            })

        # Extract the row data
        _to_export['rows'] = []
        for _row_data in table['data']:
            _row = []
            for _cell in _row_data:
                # Check if cell is associated with a wikilink (in the www18_wikitables a cell value
                # has an associated wikilink if it is surround by square brackets)
                if (len(_cell) >= 1 and _cell[0] == "[" and _cell[-1] == "]" and "|" in _cell):
                    # Split the cell value to extract its text and wikipedia url
                    parsed_cell = _cell.split(sep="|")
                    text = parsed_cell[1][:-1]
                    
                    # Remove the [ brackets from parsed_cell[0] and add the wikipedia link
                    links = ["http://www.wikipedia.org/wiki/"+parsed_cell[0][1:]]
                else:
                    text = _cell
                    links = []
                
                _row.append({
                    'text': text,
                    'isNumeric': False,
                    'links': links
                })
            _to_export['rows'].append(_row)

        # Save the _to_export dictionary as a json file
        _table_file = output_folder / 'table-{}.json'.format(_to_export['_id'])
        with open(_table_file, 'w') as outfile:
            if verbose:
                print("Saving to {}".format(_table_file))

            json.dump(_to_export, outfile)
        

    # Statistics Summary

    # AVG/STD/Median/Max lines
    # Num Tables >1 NumericColum
    # Num Table == 1 Header
    # AVG/STD/Median/Max num columns

    print("[Lines] MAX: {} ".format(np.max(num_lines)),
          "AVG: {:.3f} ".format(np.average(num_lines)),
          "STD: {:.3f} ".format(np.std(num_lines)),
          "MED: {:.3f} ".format(np.median(num_lines)))

    print("[Cols] MAX: {} ".format(np.max(num_columns)),
          "AVG: {:.3f} ".format(np.average(num_columns)),
          "STD: {:.3f} ".format(np.std(num_columns)),
          "MED: {:.3f} ".format(np.median(num_columns)))

    print("[Numeric] MAX: {} ".format(np.max(num_numeric_columns)),
          "AVG: {:.3f} ".format(np.average(num_numeric_columns)),
          "STD: {:.3f} ".format(np.std(num_numeric_columns)),
          "MED: {:.3f} ".format(np.median(num_numeric_columns)))

    print("Total With Numeric: {} ".format(with_numeric),
          "Total with 1 Header: {} ".format(with_1header),
          "To export: {} ".format(num_to_keep))

    return num_to_keep


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('-idr', '--input_dir_raw', help='path to the input directory with the raw json files')

    parser.add_argument('-odc', '--output_dir_clean', 
        help='path to the output directory where cleaned json files are placed. In that directory each file corresponds to one table')


    parser.add_argument('-id', '--input_dir', help='path to the input directory where each json file corresponds to one table')


    parser.add_argument(
        '--min-rows', help='min number of rows', metavar='MIN_R', default=10, type=int)

    parser.add_argument(
        '--max-rows', help='max number of rows', metavar='MAX_R', default=100, type=int)

    parser.add_argument(
        '--min-cols', help='min number of columns', metavar='MIN_C', default=2, type=int)

    parser.add_argument('-o', '--output', help='output directory',
                        metavar='DIR', type=dir_path, default='./files')

    parser.add_argument('-v', '--verbose', action="store_true", default=False)

    args = parser.parse_args()


    min_rows = args.min_rows
    max_rows = args.max_rows
    min_cols = args.min_cols

    if max_rows > min_rows:
        output_folder = args.output / 'tables_{}_{}'.format(min_rows, max_rows)
    else:
        output_folder = args.output / 'tables_{}_MAX'.format(min_rows)

    if not output_folder.exists():
        print(f"Creating directory {output_folder}")
        output_folder.mkdir(parents=True, exist_ok=True)

    if args.output_dir_clean:
        pathlib.Path(args.output_dir_clean).mkdir(parents=True, exist_ok=True)

    if args.input_dir_raw:
        generate_json_file_per_table(args.input_dir_raw, args.output_dir_clean)

    num_tables = export(args.input_dir, output_folder, min_rows, max_rows, min_cols, args.verbose)
    print("Exported {} tables".format(num_tables))


if __name__ == "__main__":
    """
    usage: extract_tables_www18.py [-h] [-idr INPUT_DIR_RAW]
                                [-odc OUTPUT_DIR_CLEAN] [-id INPUT_DIR]
                                [--min-rows MIN_R] [--max-rows MAX_R]
                                [--min-cols MIN_C] [-o DIR] [-v]

    optional arguments:
    -h, --help            show this help message and exit
    -idr INPUT_DIR_RAW, --input_dir_raw INPUT_DIR_RAW
                            path to the input directory with the raw json files
    -odc OUTPUT_DIR_CLEAN, --output_dir_clean OUTPUT_DIR_CLEAN
                            path to the output directory where cleaned json files
                            are placed. In that directory each file corresponds to
                            one table
    -id INPUT_DIR, --input_dir INPUT_DIR
                            path to the input directory where each json file
                            corresponds to one table
    --min-rows MIN_R      min number of rows
    --max-rows MAX_R      max number of rows
    --min-cols MIN_C      min number of columns
    -o DIR, --output DIR  output directory
    -v, --verbose
                         
    When min_rows > max_rows, we select tables *only* bigger than min_rows
    """

    main()