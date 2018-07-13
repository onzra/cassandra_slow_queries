#!/usr/bin/env python
"""
Download slow query logs from ElasticSearch.

This script require the elastic search URL and an authenticated session cookie. The easiest way to get this is to log
into Kibana and inspect a network request with Chrome. Look for the biggest request to `_search` and Copy as cURL. Pull
out the URL and the cookie header value. Save cookie into a file like `cookie.txt`.

Example usage:

    ./download_slow_queries.py https://es.example.com/elasticsearch/whatever/_search cookie.txt

Options:

- `--start`: The start of the time interval. Default 5am of current day.
- `--end`: Then end of the time interval. Default 12pm of current day.
- `--size`: Page size to request records in. Default 1,000,000.
- `-v`: Verbose output.

TODO: Currently if a single page download fails we have to retry the whole thing. Add some error checking and log a
curl command that can be used to retry the single failed request.
TODO: Proper escaping of search query parameter.
"""
import math
from argparse import ArgumentParser
import logging
import arrow
import json
import subprocess

DEFAULT_SIZE = 1000000


def run(url, cookie_file, size, start=None, end=None):
    """
    Run.

    :param str url: Kibana URL.
    :param str cookie_file: Cookie file.
    :param int size: Request size.
    :param str|None start: Start time.
    :param str|None end: End time.
    """
    # Get start and end times
    start, end = get_time_interval(start, end)
    logging.info('Requesting logs from {} to {}'.format(start, end))
    # Get cookie data
    with open(cookie_file, 'r') as fh:
        cookie = fh.read().strip(' \n')
    # Download first page
    filename = _get_filename(start, 0)
    _download_page(url, cookie, "@message:(\"Query too slow\")", start, end, size, 0, filename)
    # Determine total page count
    total_pages = _determine_pages(size, filename)
    # Download remaining pages
    for i in range(1, total_pages):
        filename = _get_filename(start, i)
        _download_page(url, cookie, "@message:(\"Query too slow\")", start, end, size, i, filename)
    logging.info('Download complete')


def _get_filename(start, page):
    """
    Get download filename.

    :param arrow.Arrow start: Start time.
    :param int page: Page number.

    :rtype: str
    :return: Filename.
    """
    return '{}_{}.json'.format(start.format('MM_DD'), page)


def _determine_pages(size, filename):
    """
    Determine number of pages to download the entire time range.

    Requires first download one page so we can check total number of hits.

    :param int size: Request size.
    :param str filename: Filename of initial download.

    :rtype: int
    :return: Number of pages.
    """
    with open(filename, 'r') as fh:
        # Hits should be within the first 500 characters
        sample = fh.read(500)
    found = sample.find('"hits":{"total":')
    if found == -1:
        raise Exception('Unable to determine total hits from {}'.format(filename))
    end = sample.find(',', found)
    total = sample[found + len('"hits":{"total":'):end]
    logging.info('Found total hits: {}'.format(total))
    if int(total) >= 10000000:
        raise Exception('Total hits {} too high, likely error'.format(total))
    return int(math.ceil(float(total) / size))


def _download_page(url, cookie, query, start, end, size, page, filename):
    """
    Download one page of results.

    :param str url: URL.
    :param str cookie: Cookie data.
    :param str query: Query string.
    :param arrow.Arrow start: Start time.
    :param arrow.Arrow end: End time.
    :param int size: Size.
    :param int page: Page number.
    :param str filename: Filename.
    """
    from_ = page * size
    logging.info('Requesting {} records starting at {}'.format(size, from_))
    body = build_request_body(query, start, end, size, from_)
    logging.debug(json.dumps(json.loads(body), indent=2))
    command = build_curl_cmd(url, body, cookie)
    logging.debug('\n'.join(command))
    execute_curl(command, filename)


def get_time_interval(start=None, end=None):
    """
    Get time interval.

    :param str|None start: Start time.
    :param str|None end: End time.

    :rtype: tuple(arrow.Arrow, arrow.Arrow)
    :return: Start time, end time.
    """
    if start and end:
        return arrow.get(start), arrow.get(end)
    if start or end:
        raise Exception('Must provide both start and end time')
    # Default to 5am - 12pm
    end = arrow.now().replace(hour=12, minute=0, second=0)
    start = end.replace(hour=5, minute=0, second=0)
    return start, end


def build_request_body(query, start, end, size, from_=0):
    """
    Build elastic search request body.

    :param str query: Query string.
    :param arrow.Arrow start: Start time.
    :param arrow.Arrow end: End time.
    :param int size: Size.
    :param int from_: From record number.

    :rtype: str
    :return: Request body.
    """
    body = {
        "query": {
            "filtered": {
                "query": {
                    "bool": {
                        "should": [
                            {
                                "query_string": {
                                    "query": "*"
                                }
                            }
                        ]
                    }
                },
                "filter": {
                    "bool": {
                        "must": [
                            {
                                "range": {
                                    "@timestamp": {
                                        "from": start.timestamp * 1000,
                                        "to": end.timestamp * 1000
                                    }
                                }
                            },
                            {
                                "fquery": {
                                    "query": {
                                        "query_string": {
                                            "query": query
                                        }
                                    },
                                    "_cache": True
                                }
                            }
                        ]
                    }
                }
            }
        },
        "size": size,
        "from": from_,
        "sort": [
            {
                "@timestamp": {
                    "order": "desc",
                    "ignore_unmapped": True
                }
            }
        ]
    }

    return json.dumps(body)


def build_curl_cmd(url, body, cookie):
    """
    Build curl command.

    :param str url: URL.
    :param str body: Body.
    :param str cookie: Cookie.

    :rtype: list[str]
    :return: Curl Command.
    """
    return [
        'curl',
        '-s',
        url,
        '-H',
        'Accept-Encoding: gzip, deflate, br',
        '-H',
        'Content-Type: application/json;charset=UTF-8',
        '-H',
        'Accept: application/json, text/plain, */*',
        '-H',
        'Cookie: {}'.format(cookie),
        '--data-binary',
        body,
        '--compressed'
    ]


def execute_curl(cmd, output_file):
    """
    Execute curl command.

    :param list[str] cmd: Command.
    :param str output_file: Output file.
    """
    with open(output_file, 'w+') as fh:
        p = subprocess.Popen(cmd, stdout=fh)
        p.wait()


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('url', help='Elastic search URL')
    parser.add_argument('cookie', help='Cookie file')
    parser.add_argument('--start', help='Start time')
    parser.add_argument('--end', help='End time')
    parser.add_argument('--size', help='Request size', default=DEFAULT_SIZE, type=int)
    parser.add_argument('-v', help='Verbose output', action='store_true', default=False)
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.v else logging.INFO)

    run(args.url, args.cookie, args.size, args.start, args.end)
