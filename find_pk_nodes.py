#!/usr/bin/env python

"""
Given a slow query primary key CSV, find nodes that own each primary key.

CSV should be `keyspace, column_family, primary_key`. Use the `slow_primary_keys.csv` output from
`analyze_slow_queries.py`.
"""
import csv
import logging
import subprocess
from argparse import ArgumentParser


def run(csv_file):
    """
    Run.

    :param str csv_file: Slow primary key CSV file.
    """
    keys = read_csv(csv_file)
    keys = gather_endpoints(keys)
    print_endpoints(keys)


def read_csv(csv_file):
    """
    Get a list of keys from slow primary key CSV file.

    :param str csv_file: Slow primary key CSV file.

    :rtype: list[dict]
    :return: List of keys.
    """
    keys = []
    with open(csv_file, 'r') as fh:
        reader = csv.reader(fh)
        # Skip headers
        next(reader, None)
        for row in reader:
            # Ignore any rows without full data
            if len(row) >= 3 and 'truncated output' not in row[2]:
                keys.append({
                    'keyspace': row[0],
                    'column_family': row[1],
                    'primary_key': row[2],
                })
            else:
                logging.debug('Ignoring row %s', ','.join(row))
    return keys


def gather_endpoints(keys):
    """
    Get endpoints for each key and add to key dict.

    :param list[dict] keys: Keys.

    :rtype: list[dict]
    :return: Keys with `endpoints` added.
    """
    for key in keys:
        key['endpoints'] = get_endpoints(key['keyspace'], key['column_family'], key['primary_key'])
    return keys


def get_endpoints(keyspace, column_family, primary_key):
    """
    Get endpoints for a primary key.

    :param str keyspace: Keyspace.
    :param str column_family: Column family.
    :param str primary_key: Primary key.

    :rtype: list[str]|None
    :return: List of endpoints that have primary key.
    """
    logging.info('Getting endpoints for %s.%s %s', keyspace, column_family, primary_key)
    cmd = ['nodetool', 'getendpoints', '--', keyspace, column_family, primary_key]
    logging.debug(cmd)
    try:
        output = subprocess.check_output(cmd)
        logging.debug(output)
        return output.split('\n')
    except subprocess.CalledProcessError as e:
        logging.warn('Unable to get endpoints for %s %s %s, Error: %s', keyspace, column_family, primary_key, repr(e))
    return None


def print_endpoints(keys):
    """
    Print keys and endpoints.

    :param list[dict] keys: Keys.
    """
    headers = [
        'Keyspace',
        'Column Family',
        'Primary Key',
        'Endpoint',
        'Endpoint',
        'Endpoint',
        'Endpoint',
        'Endpoint',
        'Endpoint',
    ]
    print(','.join(headers))
    for key in keys:
        if key['endpoints']:
            row = [
                key['keyspace'],
                key['column_family'],
                key['primary_key'],
            ]
            for endpoint in key['endpoints']:
                row.append(endpoint)
            print(','.join(row))


if __name__ == '__main__':
    parser = ArgumentParser(description='Find nodes that own slow primary keys')
    parser.add_argument('csv', help='Slow primary key CSV')
    parser.add_argument('-v', help='Verbose output', action='store_true', default=False)
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.v else logging.INFO)

    run(args.csv)
