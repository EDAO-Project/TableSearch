#!/usr/bin/env python
# -*- coding: utf-8 -*-


import re
import json
import gzip
import pathlib
import argparse

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


def export(filename, output_folder, min_rows, max_rows, min_cols, keep_numeric=True, verbose=False):
    reader = gzip.open(filename, 'rt')

    print("Read file")

    with_numeric = 0
    with_1header = 0
    num_columns = []
    num_lines = []
    num_numeric_columns = []
    num_to_keep = 0

    p = re.compile(r'href=[\'"]?([^\'" >]+)')

    for line in tqdm(reader):
        table = json.loads(line)
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

        _to_export = {k: table.get(k, '') for k in ['_id', 'numCols', 'numDataRows', 'pgTitle', 'pgId', 'tableCaption']}
        _to_export['numNumericCols'] = num_numeric_columns[-1]
        _to_export['headers'] = []

        # This is a list of list in principle there are multiple headers
        # But we filter and stick with only one header
        for _header_cell in table['tableHeaders'][0]:
            _to_export['headers'].append({
                'text': _header_cell['text'],
                'isNumeric': _header_cell['isNumeric'],  # Numeric refers only to the content of the header cell
                # that is only if you have actually a number insider the header cell
                'links': p.findall(_header_cell['tdHtmlString']) if len(_header_cell['surfaceLinks']) > 0 else []
            })

        _to_export['rows'] = []
        for _row_data in table['tableData']:
            _row = []
            for _cell in _row_data:
                _row.append({
                    'text': _cell['text'],
                    'isNumeric': _cell['isNumeric'],
                    'links': p.findall(_cell['tdHtmlString']) if len(_cell['surfaceLinks']) > 0 else []
                })
            _to_export['rows'].append(_row)
        _table_file = output_folder / 'table-{}.json'.format(table['_id'])
        with open(_table_file, 'w') as outfile:
            if verbose:
                print( "Saving to {}".format(_table_file))

            json.dump(_to_export, outfile)

    # AVG/STD/Median/Max lines
    # Num Tables >1 NumericColum
    # Num Table == 1 Header
    # AVG/STD/Median/Max num columns
    #

    print("[Lines] MAX: ", np.max(num_lines),
          "AVG: ", np.average(num_lines),
          "STD: ", np.std(num_lines),
          "MED: ", np.median(num_lines))

    print("[Cols] MAX: ", np.max(num_columns),
          "AVG: ", np.average(num_columns),
          "STD: ", np.std(num_columns),
          "MED: ", np.median(num_columns))

    print("[Numeric] MAX: ", np.max(num_numeric_columns),
          "AVG: ", np.average(num_numeric_columns),
          "STD: ", np.std(num_numeric_columns),
          "MED: ", np.median(num_numeric_columns))

    print("Total With Numeric: ", with_numeric,
          "Total with 1 Header: ", with_1header,
          "To export: ", num_to_keep)

    return num_to_keep


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--tables', help='tables file, .json .gz compressed',
                        metavar='FILE', type=existing_file_path, default='tables.json.gz', required=True)

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

    if max_rows > min_rows:
        output_folder = args.output / 'tables_{}_{}'.format(min_rows, max_rows)
    else:
        output_folder = args.output / 'tables_{}_MAX'.format(min_rows)

    if not output_folder.exists():
        print(f"Creating directory {output_folder}")
        output_folder.mkdir(parents=True, exist_ok=True)

    num_tables = export(args.tables, output_folder, 50, 100, 3, args.verbose)
    print("Exported {} tables".format(num_tables))


if __name__ == "__main__":
    """
    command line: python read_wiki_tables.py <filename tables> <min_rows> <max_rows> <outputfile>
    When min_rows > max_rows, we select tables *only* bigger than min_rows
    """

    main()
