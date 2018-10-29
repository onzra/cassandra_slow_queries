#!/usr/bin/env python
"""
Download slow query logs from ElasticSearch.

This script require the elastic search URL, index, and an authenticated session cookie. The easiest way to get this is
to log into Kibana and inspect a network request with Chrome. Look for the request to `_msearch` and Copy as cURL.
Save that into a file named `curl.txt` and then let the script parse that.

Example usage:

    ./download_slow_queries.py --from-curl curl.txt

Start time and end time can be supplied. Will always use your system local timezone. If no date is provided, will
assume today's date.

   ./download_slow_queries.py --start "10:00:00" --end "11:00:00"

Alternatively you can supply the URL, index, and a cookie file:

    ./download_slow_queries.py --url "https://es.example.com/es/_msearch" --index "my-index:log*" --cookie cookie.txt

Options:

- `--start`: The start of the time interval. Default 5am of current day.
- `--end`: Then end of the time interval. Default 12pm of current day.
- `--size`: Page size to request records in. Default 1,000,000.
- `-v`: Verbose output.

TODO: Currently if a single page download fails we have to retry the whole thing. Add some error checking and log a
curl command that can be used to retry the single failed request.
TODO: Use the scroll API to get more than 10,0000 results.
"""
import math
from argparse import ArgumentParser
import logging
import arrow
import json
import subprocess

DEFAULT_SIZE = 10000
QUERY_STRING = 'Query too slow'


def run(url, index, cookie, size, start=None, end=None, query=None):
    """
    Run.

    :param str url: Kibana URL.
    :param str index: Index name.
    :param str cookie: Cookie.
    :param int size: Request size.
    :param str|None start: Start time.
    :param str|None end: End time.
    :param str|None query: Additional query string.
    """
    # Get start and end times
    start, end = get_time_interval(start, end)
    logging.info('Requesting logs from {} to {}'.format(start, end))
    # Download first page
    filename = _get_filename(start, 0)
    _download_page(url, index, cookie, QUERY_STRING, start, end, size, 0, filename, query)
    # Determine total page count
    total_pages = _determine_pages(size, filename)
    # Download remaining pages
    for i in range(1, total_pages):
        filename = _get_filename(start, i)
        _download_page(url, index, cookie, QUERY_STRING, start, end, size, i, filename, query)
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
        # Hits should be within the first 1000 characters
        sample = fh.read(1000)
    found = sample.find('"hits":{"total":')
    if found == -1:
        raise Exception('Unable to determine total hits from {}'.format(filename))
    end = sample.find(',', found)
    total = sample[found + len('"hits":{"total":'):end]
    logging.info('Found total hits: {}'.format(total))
    if int(total) >= 10000000:
        raise Exception('Total hits {} too high, likely error'.format(total))
    return int(math.ceil(float(total) / size))


def _log_failures(filename):
    """
    Log all failures from a JSON response file.

    :param str filename: Path to file.
    """
    # Peek at contents of file without doing full JSON load
    with open(filename, 'r') as fh:
        # Failures should be within the first 1000 characters
        sample = fh.read(1000)
    found = sample.find('"failures":')
    if found == -1:
        return
    # If there are failures, spend the time to load JSON data
    with open(filename, 'r') as fh:
        data = json.load(fh)
    for response in data['responses']:
        for failure in response['_shards']['failures']:
            logging.error('Elasticsearch failure: "%s"', failure['reason']['reason'])


def _download_page(url, index, cookie, message, start, end, size, page, filename, query):
    """
    Download one page of results.

    :param str url: URL.
    :param str index: Index name.
    :param str cookie: Cookie data.
    :param str message: Query string.
    :param arrow.Arrow start: Start time.
    :param arrow.Arrow end: End time.
    :param int size: Size.
    :param int page: Page number.
    :param str filename: Filename.
    :param str|None query: Additional query string.
    """
    from_ = page * size
    logging.info('Requesting {} records starting at {}'.format(size, from_))
    header = build_request_header(index)
    body = build_request_body(message, start, end, size, from_, query)
    http_body = '{}\n{}\n'.format(header, body)
    logging.debug(json.dumps(json.loads(header), indent=2))
    logging.debug(json.dumps(json.loads(body), indent=2))
    command = build_curl_cmd(url, http_body, cookie)
    logging.debug('\n'.join(command))
    execute_curl(command, filename)
    _log_failures(filename)


def get_time_interval(start=None, end=None):
    """
    Get time interval.

    :param str|None start: Start time.
    :param str|None end: End time.

    :rtype: tuple(arrow.Arrow, arrow.Arrow)
    :return: Start time, end time.
    """
    if not start and not end:
        # Default to 5am - 12pm
        end = arrow.now().replace(hour=12, minute=0, second=0)
        start = end.replace(hour=5, minute=0, second=0)
    elif not start or not end:
        raise Exception('Must provide both start and end time')
    else:
        # Allow only supplying time part and using today's date
        if '-' not in start:
            start = '{} {}'.format(arrow.now().format('YYYY-MM-DD'), start)
        if '-' not in end:
            end = '{} {}'.format(arrow.now().format('YYYY-MM-DD'), end)
        start = arrow.get(start).replace(tzinfo='local')
        end = arrow.get(end).replace(tzinfo='local')
    return start, end


def build_request_body(message, start, end, size, from_=0, query=None):
    """
    Build elastic search request body.

    :param str message: Message query string, "Query too slow".
    :param arrow.Arrow start: Start time.
    :param arrow.Arrow end: End time.
    :param int size: Size.
    :param int from_: From record number.
    :param str|None query: Additional query string.

    :rtype: str
    :return: Request body.
    """
    body = {
        "query": {
            "bool": {
                "must": [
                    {
                        "match_phrase": {
                            "message": {
                                "query": message
                            }
                        }
                    },
                    {
                        "range": {
                            "@timestamp": {
                                "gte": start.timestamp,
                                "lte": end.timestamp,
                                "format": "epoch_second"
                            }
                        }
                    }
                ]
            }
        },
        "size": size,
        "from": from_,
        "sort": [
            {
                "@timestamp": {
                    "order": "desc",
                    "unmapped_type": "boolean"
                }
            }
        ]
    }
    if query:
        body['query']['bool']['must'].append({
            "query_string": {
                "query": "\"{}\"".format(query),
                "analyze_wildcard": True,
                "default_field": "*"
            }
        })

    return json.dumps(body)


def build_request_header(index):
    """
    Build request NDJSON header.

    :param str index: Index name.

    :rtype: str
    :return: Request NDJSON header.
    """
    header = {
        'index': index,
        'ignore_unavailable': True,
        'timeout': 30000
    }
    return json.dumps(header)


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
        '-H', 'Accept-Encoding: gzip, deflate, br',
        '-H', 'kbn-version: 6.3.2',
        '-H', 'Content-Type: application/json;charset=UTF-8',
        '-H', 'Accept: application/json, text/plain, */*',
        '-H', 'Cookie: {}'.format(cookie),
        '--data-binary', body,
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


def _from_curl(curl_file):
    """
    Parse URL, index, and cookie from curl command file.

    :param str curl_file: Path to curl command file.

    :rtype: tuple
    :return: URL, index, and cookie.
    """
    with open(curl_file, 'r') as fh:
        command = fh.read().strip(' \n')
    # curl 'https://es.example.com/es/_msearch'
    start = command.find('curl ') + len('curl ')
    end = command.find(' ', start)
    url = command[start:end].strip('\'"')
    # --data-binary $'{"index":"es-index:log*",
    start = command.find('"index":"') + len('"index":"')
    end = command.find('"', start)
    index = command[start:end]
    # -H 'Cookie: access_token=eyJhbGc
    start = command.find('-H \'Cookie: ') + len('-H \'Cookie: ')
    end = command.find('\'', start)
    cookie = command[start:end]
    return url, index, cookie


def _read_cookie(cookie_file):
    """
    Read cookie data from file.

    :param str cookie_file: Path to cookie file.

    :rtype: str
    :return: Cookie contents.
    """
    with open(cookie_file, 'r') as fh:
        cookie = fh.read().strip(' \n')
    return cookie


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('--url', help='Elasticsearch URL')
    parser.add_argument('--index', help='Elasticsearch index name')
    parser.add_argument('--cookie', help='Cookie file')
    parser.add_argument('--from-curl', help='Parse parameters from curl command file')
    parser.add_argument('--start', help='Start time')
    parser.add_argument('--end', help='End time')
    parser.add_argument('--query', help='Additional query string')
    parser.add_argument('--size', help='Request size', default=DEFAULT_SIZE, type=int)
    parser.add_argument('-v', help='Verbose output', action='store_true', default=False)
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.v else logging.INFO)

    if args.from_curl:
        url, index, cookie = _from_curl(args.from_curl)
    else:
        url = args.url
        index = args.index
        cookie = _read_cookie(args.cookie)

    run(url, index, cookie, args.size, args.start, args.end, args.query)
