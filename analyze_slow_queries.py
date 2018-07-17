#!/usr/bin/env python

"""
Gather aggregate statistics from Kibana search data of Cassandra slow query logs.

Takes input of multiple Kibana search result JSON files. Analyzes this for slow query log lines. Gathers aggregate
information on slowest queries and slowest primary keys. Use the `download_slow_queries.py` script to get the data.

Example usage:

    ./analyze_slow_queries.py 07_13_*.json --schema schema.cql --queries queries.json --tags tags.json

This will output a set of CSV files:

- `primary_keys.csv`: A list of top 100 worst primary keys. Mainly for easy reference to get KS, CF, PK.
- `slow_primary_keys.csv`: Top 100 worst primary key queries. This is the go to for what is putting the most load on
  the system. Lots of entries here are problematic primary keys.
- `slow_queries.csv`: Top 100 slow queries. Regardless of primary key, what queries are adding the most load.
- `volume.csv`: Per minute slow query data. This makes a nice graph of slow query load over time.
- `volume_top_n.csv`: Top 5 slow queries per minute. This is really helpful when you want to dig into "what happened
  right around 15:45".

The `--schema` option is required to get more detailed information on primary keys. This should be a schema dump from
Cassandra.

Process additional queries that aren't using bound parameters by providing query patterns with the `--queries` option.
The patterns are a list of JSON like:

    [
      {
        "start": "SELECT * FROM users WHERE user_id",
        "parameters": [
          "user_id"
        ]
      {
    ]

You can provide a JSON file with a dictionary of tag: keyspace using the `--tags` option to guess keyspace from
ElasticSearch tags.

Options:

- `--schema`: CQL schema dump for identifying primary keys.
- `--queries`: Additional query information for processing.
- `--tags`: Dictionary mapping tag: keyspace for better keyspace guessing.
- `--top-n`: Number of entries to include in the top N reports. Default 100.
- `--rows-per-minute`: Number of entries to include per minute in the per minute reports. Default 5.
- `--order-by`: What to order results by, important for top N cutoff. Choices: duration, avg_duration, count. Default
   duration.
- `-v`: Verbose output.

Note that a lot of the code in this script is not well organized. It is mainly built for speed.

TODO: Probably better to wrap schema, query patterns, and tags into one config object
TODO: Volume report needs to be sorted by timestamp.
TODO: Convert time output to local timezone. Get from system if we can, otherwise use a CLI arg.
TODO: Parse primary key out of DELETE and UPDATE statements.
TODO: .lower() all the strings to avoid extra case sensitive checks.
TODO: Add a report about any slow range queries (no single primary key).
TODO: Configuration for ignoring certain column families. High volume CFs create noise when everything is slow.
TODO: Better output file naming. Include date and maybe order-by.
TODO: Might be good to output the full processed object to JSON file so we can re-analyze without doing all the work.
TODO: Output all reports into a folder. Name with date/time.
TODO: Pull coordinator IP out of log message.
TODO: Add coordinator IPs to existing reports.
TODO: Input IP -> hostname config file to get hostnames instead.
TODO: Report on pk + query + coordinator (maybe also a volume_top_n version of this).
TODO: Filter reports to only include specific coordinators (whitelist).
TODO: Filter reports to only PKs that cross all nodes in whitelist (3 node hotspot kind of thing).
"""

import csv
import logging
import json
import time
import traceback
import itertools
from argparse import ArgumentParser
from datetime import datetime


class Config(object):
    """
    Slow query analysis configuration.
    """

    def __init__(self, top_n=100, rows_per_minute=5, order_by='duration', min_count=5, schema=None, queries=None,
                 tags=None):
        """
        Init.

        :param int top_n: Number of entries to include in the top N reports.
        :param int rows_per_minute: Number of entries to include per minute in the per minute reports.
        :param str order_by: What to order the results by.
        :param int min_count: Minimum number of log entries to be included in reporting at all.
        :param dict schema: Processed schema dictionary.
        :param list[dict] queries: Additional query patterns for primary key identification.
        :param dict tags: Dictionary of tag: keyspace mappings.
        """
        self.top_n = top_n
        self.rows_per_minute = rows_per_minute
        self.order_by = order_by
        self.min_count = min_count
        self.schema = schema or {}
        self.queries = queries
        self.tags = tags


def run(config, data_files, schema_file=None, queries_file=None, tags_file=None):
    """
    Run.

    :param Config config: Configuration.
    :param list[str] data_files: JSON Kibana log files.
    :param str|None schema_file: Schema file.
    :param str|None queries_file: Additional query patterns file.
    :param str|None tags_file: Tag: keyspace mapping file.
    """
    Timer.start('total')
    # Load additional query patterns
    if queries_file:
        with open(queries_file, 'r') as fh:
            config.queries = json.load(fh)
    # Process Schema
    if schema_file:
        with open(schema_file, 'r') as fh:
            schema_string = fh.read()
        config.schema = SchemaProcessor.process(schema_string)
    # Process tag keyspace mapping file
    if tags_file:
        with open(tags_file, 'r') as fh:
            config.tags = json.load(fh)
    # Process JSON log files
    processed = []
    for f in data_files:
        processed += process_file(f, config)
    # Analyze
    analysis = analyze(processed, config)
    # Write reports
    Reporter.report(analysis)
    Timer.end('total')
    logging.info(Timer.timers_display())


class Timer(object):
    """
    Simple timer.
    """
    timers = {}

    @classmethod
    def start(cls, name):
        """
        Start timer.

        :param str name: Name.
        """
        if name not in cls.timers:
            cls.timers[name] = {}
        cls.timers[name]['start'] = time.time()

    @classmethod
    def end(cls, name):
        """
        End timer.

        :param str name: Name.

        :rtype: float
        :return: Duration.
        """
        if name not in cls.timers:
            raise Exception('Timer {} not started'.format(name))
        cls.timers[name]['end'] = time.time()
        duration = cls.timers[name]['end'] - cls.timers[name]['start']
        if 'duration' in cls.timers:
            cls.timers[name]['duration'] += duration
        else:
            cls.timers[name]['duration'] = duration
        return cls.timers[name]['duration']

    @classmethod
    def get_timers(cls):
        """
        Get completed timers.

        :rtype: dict
        :return: Completed timers.
        """
        return {k: v['duration'] for k, v in cls.timers.items() if 'duration' in v}

    @classmethod
    def timers_display(cls):
        """
        Returns timers formatted as a string.

        :rtype: str
        :return: Timers string.
        """
        return '\n'.join(['{}: {}'.format(k, v) for k, v in cls.get_timers().items()])


def str_slice(string, before, after):
    """
    Extract a slice from a string between two other strings.

    Fails if before or after strings cannot be found.

    :param str string: String.
    :param str before: String before slice to extract.
    :param str after: String after slice to extract.

    :rtype: str|None
    :return: Sliced string or None if string does not match.
    """
    start = string.find(before)
    if start == -1:
        return None
    string = string[start + len(before):]
    end = string.find(after)
    if end == -1:
        return None
    string = string[:end]
    return string


class SchemaProcessor(object):
    """
    Process CQL schema.
    """

    @classmethod
    def process(cls, schema):
        """
        Process CQL schema.

        :param str schema: Schemas.

        :rtype: dict
        :return: Schema data.
        """
        ret = {}
        keyspace = None
        column_family = None
        for line in schema.splitlines():
            if 'CREATE TABLE' in line:
                keyspace, column_family = cls._parse_create_table(line)
                if keyspace not in ret:
                    ret[keyspace] = {}
                if column_family not in ret[keyspace]:
                    ret[keyspace][column_family] = {}
            if 'PRIMARY KEY (' in line:
                if not keyspace or not column_family:
                    raise Exception(u'Unable to process schema line {}'.format(line))
                primary_keys, clustering_keys = cls._parse_keys(line)
                ret[keyspace][column_family] = {
                    'primary_key': primary_keys,
                    'clustering_key': clustering_keys,
                }
                keyspace = None
                column_family = None
            elif 'PRIMARY KEY' in line:
                if not keyspace or not column_family:
                    raise Exception(u'Unable to process schema line {}'.format(line))
                primary_key = cls._parse_primary_column(line)
                ret[keyspace][column_family] = {
                    'primary_key': [primary_key],
                    'clustering_key': [],
                }
                keyspace = None
                column_family = None
        return ret

    @classmethod
    def _parse_create_table(cls, create_table_str):
        """
        Parse create table string.

        :param str create_table_str: Create table string.

        :rtype: tuple
        :return: keyspace, column_family
        """
        keyspace = str_slice(create_table_str, 'CREATE TABLE ', '.')
        column_family = str_slice(create_table_str, '.', ' ')
        return keyspace, column_family

    @classmethod
    def _parse_keys(cls, primary_key_str):
        """
        Parse primary key string.

        :param str primary_key_str: Primary key string.

        :rtype: tuple[list]
        :return: primary keys, clustering keys
        """
        primary_key_str = primary_key_str.replace('PRIMARY KEY ', '')
        if primary_key_str.startswith('(('):
            # PRIMARY KEY ((column_1, column_2, column_3), column_4)
            # PRIMARY KEY ((column_1, column_2, column_3))
            pk_ck_split = primary_key_str.find(')')
            primary = primary_key_str[0:pk_ck_split].replace('((', '')
            primary_keys = [v.strip(' ') for v in primary.split(',')]
            clustering = primary_key_str[pk_ck_split + 1:].replace(')', '')
            clustering_keys = [v.strip(' ') for v in filter(None, clustering.split(','))]
        else:
            # PRIMARY KEY (column_1, column_2, column_3, column_4)
            primary_key_str = primary_key_str.replace('(', '').replace(')', '')
            keys = [v.strip(' ') for v in primary_key_str.split(',')]
            primary_keys = [keys[0]]
            clustering_keys = keys[1:]
        return primary_keys, clustering_keys

    @classmethod
    def _parse_primary_column(cls, primary_column_str):
        """
        Parse primary key column string.

        :param str primary_column_str: Primary key column string.

        :rtype: str
        :return: Primary key.
        """
        #     my_id uuid PRIMARY KEY,
        return primary_column_str.strip(' ,').replace(' PRIMARY KEY', '').split(' ')[0]


class MessageProcessor(object):
    """
    Message processor.
    """

    # Cache of keyspace for CF guessing
    CF_KEYSPACES = {}

    @classmethod
    def handles(cls, log):
        """
        Check if processor handles log.

        :param dict log: Slow query log.

        :rtype: bool
        :return: True if processor handles log.
        """
        raise NotImplementedError()

    @classmethod
    def process(cls, log, config):
        """
        Process batch slow query log.

        :param dict log: Slow query log.
        :param Config config: Configuration.

        :rtype: dict
        :return: Log data.
        """
        raise NotImplementedError()

    @classmethod
    def _get_bound_values(cls, bound_values_str):
        """
        Get bound values.

        :param str bound_values_str: Bound values section of slow query log.

        :rtype: dict
        :return: Bound values.
        """
        bound_values_str = bound_values_str.replace('[', '').replace(']', '')
        bound_values = bound_values_str.split(',')
        ret = {}
        for v in bound_values:
            try:
                key, value = v.split(':', 1)
                key = key.strip(' ')
                value = value.strip("'")
                ret[key] = value
            except ValueError:
                if 'in(' not in bound_values_str and 'truncated output' not in bound_values_str:
                    logging.warn(u'Bad bound values {}'.format(bound_values_str))
        return ret

    @classmethod
    def _get_primary_key(cls, bound_values, keyspace, column_family, config):
        """
        Get primary key out of bound values.

        :param dict bound_values: Bound values.
        :param str keyspace: Keyspace.
        :param str column_family: Column family.
        :param Config config: Configuration.

        :rtype: str|None
        :return: Primary key.
        """
        try:
            cf_meta = config.schema[keyspace][column_family]
            primary_key = []
            for field in cf_meta['primary_key']:
                try:
                    primary_key.append(bound_values[field])
                except KeyError:
                    logging.warn(
                        u'Primary key field {} not in bound values for {}.{}'.format(field, keyspace, column_family))
            return '-'.join(primary_key)
        except KeyError:
            logging.warn(u'No schema for {}.{}. Tags: {}'.format(keyspace, column_family, ', '.join(config.tags)))
            return None

    @classmethod
    def _get_keyspace_cf(cls, table, tags, config):
        """
        Get keyspace and column family from table segment

        :param str table: Table segment, either keyspace.column_family or just column_family.
        :param list[str] tags: Slow query log tags.
        :param Config config: Configuration.

        :rtype: tuple[str]
        :return: Keyspace and column family.
        """
        if '.' in table:
            keyspace, column_family = table.split('.')
            keyspace = keyspace.lower()
            column_family = column_family.lower()
        else:
            column_family = table.lower()
            keyspace = cls._guess_keyspace(column_family, tags, config)
        return keyspace, column_family

    @classmethod
    def _guess_keyspace(cls, column_family, tags, config):
        """
        Attempt to find keyspace from column family.

        :param str column_family: Column family.
        :param list[str] tags: Slow query log tags.
        :param Config config: Configuration.

        :rtype: str|None
        :return: Keyspace.
        """
        if not cls.CF_KEYSPACES:
            cls._build_keyspace_guesses(config.schema)

        # Use tag_keyspaces mapping if schema guess is not available
        if config.tags and (column_family not in cls.CF_KEYSPACES or cls.CF_KEYSPACES[column_family] == 'unknown'):
            for tag in tags:
                if tag in config.tags:
                    return config.tags[tag]
        try:
            return cls.CF_KEYSPACES[column_family]
        except KeyError:
            # No schema for this column family
            return None

    @classmethod
    def _build_keyspace_guesses(cls, schema):
        """
        Build column family -> keyspace mapping.

        If column family exists in more than one keyspace, keyspace is set to "unknown" since we cannot guess correctly.

        :param dict schema: Schema.
        """
        for keyspace, cfs in schema.items():
            for cf in cfs:
                if cf not in cls.CF_KEYSPACES:
                    cls.CF_KEYSPACES[cf] = keyspace
                else:
                    cls.CF_KEYSPACES[cf] = 'unknown'


class BatchMessageProcessor(MessageProcessor):
    """
    Batch message processor.
    """

    @classmethod
    def handles(cls, log):
        """
        Check if processor handles log.

        :param dict log: Slow query log.

        :rtype: bool
        :return: True if processor handles log.
        """
        return log['query'].startswith('BEGIN BATCH') or log['query'].startswith('begin batch')

    @classmethod
    def process(cls, log, config):
        """
        Process batch slow query log.

        :param dict log: Slow query log.
        :param Config config: Configuration.

        :rtype: dict
        :return: Log data.
        """
        return {
            'type': 'BATCH',
            'duration': int(log['duration']),
            'query': log['query'],
        }


class SelectMessageProcessor(MessageProcessor):
    """
    SELECT message processor.
    """

    @classmethod
    def handles(cls, log):
        """
        Check if processor handles log.

        :param dict log: Slow query log.

        :rtype: bool
        :return: True if processor handles message.
        """
        return log['query'].startswith('SELECT') or log['query'].startswith('select')

    @classmethod
    def process(cls, log, config):
        """
        Process slow query log.

        :param dict log: Slow query log.
        :param Config config: Configuration.

        :rtype: dict
        :return: Log data.
        """
        duration = int(log['duration'])
        query = log['query']

        bound_values = {}
        if log['bound_values']:
            bound_values = cls._get_bound_values(log['bound_values'])
        elif config.queries:
            for pattern in config.queries:
                if QueryPattern.matches(query, pattern):
                    query, bound_values = QueryPattern.process(query, pattern)
                    break

        table_segment = cls._get_table(query)
        if table_segment:
            keyspace, column_family = cls._get_keyspace_cf(table_segment, log['tags'], config)
            if not keyspace:
                logging.warn(u'Unable to get keyspace for column family %s. Tags: %s',
                             column_family, ', '.join(log['tags']))
        else:
            logging.warn(u'Unable to parse table segment out of %s', query)
            keyspace = None
            column_family = None

        if bound_values and keyspace and column_family:
            primary_key = cls._get_primary_key(bound_values, keyspace, column_family, config)
        else:
            primary_key = None

        return {
            'type': 'SELECT',
            'duration': duration,
            'query': query,
            'bound_values': bound_values,
            'keyspace': keyspace,
            'column_family': column_family,
            'primary_key': primary_key,
        }

    @classmethod
    def _get_table(cls, query):
        """
        Get table segment from query string.

        May be keyspace.column_family or just column_family.

        :param str query: Query string.

        :rtype: str|None
        :return: Table segment.
        """
        # TODO: Would be nice to be able to provide multiple start / end strings
        table = str_slice(query, ' FROM ', ' ')
        if not table:
            table = str_slice(query, ' from ', ' ')
        if not table:
            table = str_slice(query, ' FROM ', ';')
        if not table:
            table = str_slice(query, ' from ', ';')
        return table


class InsertMessageProcessor(MessageProcessor):
    """
    INSERT message processor.
    """

    @classmethod
    def handles(cls, log):
        """
        Check if processor handles log.

        :param dict log: Slow query log.

        :rtype: bool
        :return: True if processor handles message.
        """
        return log['query'].startswith('INSERT') or log['query'].startswith('insert')

    @classmethod
    def process(cls, log, config):
        """
        Process slow query log.

        :param dict log: Slow query log.
        :param Config config: Configuration.

        :rtype: dict
        :return: Log data.
        """
        bound_values = {}
        if log['bound_values']:
            bound_values = cls._get_bound_values(log['bound_values'])

        table_segment = cls._get_table(log['query'])
        if table_segment:
            keyspace, column_family = cls._get_keyspace_cf(table_segment, log['tags'], config)
            if not keyspace:
                logging.warn(u'Unable to get keyspace for column family %s', column_family)
        else:
            logging.warn(u'Unable to parse table segment out of %s', log['query'])
            keyspace = None
            column_family = None

        if bound_values and keyspace and column_family:
            primary_key = cls._get_primary_key(bound_values, keyspace, column_family, config)
        else:
            primary_key = None

        return {
            'type': 'INSERT',
            'duration': int(log['duration']),
            'query': log['query'],
            'bound_values': bound_values,
            'keyspace': keyspace,
            'column_family': column_family,
            'primary_key': primary_key,
        }

    @classmethod
    def _get_table(cls, query):
        """
        Get table segment from query string.

        May be keyspace.column_family or just column_family.

        :param str query: Query string.

        :rtype: str|None
        :return: Table segment.
        """
        table = str_slice(query, 'INSERT INTO ', ' ')
        if not table:
            table = str_slice(query, 'insert into ', ' ')
        return table


class DeleteMessageProcessor(MessageProcessor):
    """
    DELETE message processor.
    """

    @classmethod
    def handles(cls, log):
        """
        Check if processor handles log.

        :param dict log: Slow query log.

        :rtype: bool
        :return: True if processor handles message.
        """
        return log['query'].startswith('DELETE') or log['query'].startswith('delete')

    @classmethod
    def process(cls, log, config):
        """
        Process slow query log.

        :param dict log: Slow query log.
        :param Config config: Configuration.

        :rtype: dict
        :return: Log data.
        """
        return {
            'type': 'DELETE',
            'duration': int(log['duration']),
            'query': log['query'],
        }


class UpdateMessageProcessor(MessageProcessor):
    """
    UPDATE message processor.
    """

    @classmethod
    def handles(cls, log):
        """
        Check if processor handles log.

        :param dict log: Slow query log.

        :rtype: bool
        :return: True if processor handles message.
        """
        return log['query'].startswith('UPDATE') or log['query'].startswith('update')

    @classmethod
    def process(cls, log, config):
        """
        Process slow query log.

        :param dict log: Slow query log.
        :param Config config: Configuration.

        :rtype: dict
        :return: Log data.
        """
        return {
            'type': 'UPDATE',
            'duration': int(log['duration']),
            'query': log['query'],
        }


class QueryPattern(object):
    """
    Processes additional queries that do not use bound values.
    """

    @classmethod
    def matches(cls, query, pattern):
        """
        Test if a query string matches a pattern.

        :param str query: Query string.
        :param dict pattern: Pattern metadata.

        :rtype: bool
        :return: True if pattern matches query
        """
        return query.startswith(pattern['start'])

    @classmethod
    def process(cls, query, pattern):
        """
        Process a query that does not use bound values.

        :param str query: Query string.
        :param dict pattern: Pattern metadata.

        :rtype: tuple(str,dict)
        :return: Query string, bound values
        """
        bound_values = {}
        for name in pattern['parameters']:
            # Find `<param>=`
            start = query.find('=', query.find(name) + len(name)) + 1
            temp = query[start:].strip(' ')
            # Find end of parameter value
            end = temp.find(' ')
            if end == -1:
                end = temp.find(',')
            if end == -1:
                end = temp.find(';')
            if end == -1:
                continue
            value = temp[:end]
            # Replace parameter value with `?`, like bound values
            query = query.replace(value, '?')
            # Set bound value (without quotes)
            bound_values[name.lower()] = value.strip("'")
        return query, bound_values


processors = [
    SelectMessageProcessor,
    BatchMessageProcessor,
    InsertMessageProcessor,
    DeleteMessageProcessor,
    UpdateMessageProcessor
]


def process_message(timestamp, message, tags, config):
    """
    Process a slow query message string.

    :param str timestamp: Timestamp.
    :param str message: Slow query log message.
    :param list[str] tags: Tags.
    :param Config config: Configuration.

    :rtype: dict
    :return: Slow query log data.
    """
    timestamp = datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%fZ')
    log = get_log(message)
    log['tags'] = tags

    ret = {
        'type': None,
        'timestamp': timestamp,
        'duration': None,
        'query': None,
        'bound_values': {},
        'primary_key': None,
        'keyspace': None,
        'column_family': None,
    }
    data = None
    for processor in processors:
        if processor.handles(log):
            data = processor.process(log, config)
            break
    if not data:
        logging.debug(log)
        raise Exception('No processor available')
    ret.update(data)
    return ret


def get_log(message):
    """
    Parse slow query low parts out of message.

    :param str message: Kibana log message.

    :rtype: dict
    :return: Slow query log parts.
    """
    buff = message
    # Query too slow
    ptr = buff.find('Query too slow, took ')
    if ptr == -1:
        raise Exception('Not a slow query log')
    # ms:
    pos_ms = buff.find(' ms: ', ptr)
    if pos_ms == -1:
        raise Exception('Unable to find query time')
    duration = buff[ptr + 21:pos_ms]
    ptr = pos_ms + 5
    # [1 bound values]
    if buff[pos_ms + 5] == '[':
        end = buff.find(']', ptr)
        counts = buff[ptr:end + 1]
        ptr = end + 2
    else:
        counts = None
    # [my_uuid:'9a92990c-b54f-4175-aa0f-707ca0f0d763']
    bound_values = None
    rptr = len(buff)
    if counts:
        start = buff.find('; [', ptr)
        if start == -1:
            start = buff.find('] [', ptr)
        if start != -1:
            bound_values = buff[start + 2:]
            rptr = start + 1
    # CQL query
    query = buff[ptr:rptr]
    return {
        'duration': duration,
        'counts': counts,
        'bound_values': bound_values,
        'query': query,
    }


def analyze(data, config):
    """
    Analyze query too slow log data.

    :param list[dict] data: Query too slow log data.
    :param Config config: Configuration.

    :rtype: dict
    :return: Analysis.
    """
    logging.info('Analyzing slow query data')
    Timer.start('analysis')
    analysis = {
        'query': {},
        'query_pk': {},
        'primary_key': {},
        'volume': {},
        'volume_top': {},
    }
    for datum in data:
        query = datum.get('query')
        primary_key = datum.get('primary_key', '') or ''
        keyspace = datum.get('keyspace', '') or ''
        column_family = datum.get('column_family', '') or ''
        minute = datum['timestamp'].strftime('%Y-%m-%d %H:%M')
        query_pk = query + '.' + primary_key
        ks_cf_pk = keyspace + '.' + column_family + '.' + primary_key

        # Query
        if query not in analysis['query']:
            analysis['query'][query] = {
                'count': 0,
                'duration': 0,
                'query': query,
                'keyspace': keyspace,
                'column_family': column_family,
            }
        analysis['query'][query]['count'] += 1
        analysis['query'][query]['duration'] += datum['duration']

        # Query - Primary Key
        if primary_key:
            if query_pk not in analysis['query_pk']:
                analysis['query_pk'][query_pk] = {
                    'count': 0,
                    'duration': 0,
                    'query': query,
                    'primary_key': primary_key,
                    'keyspace': keyspace,
                    'column_family': column_family,
                }
            analysis['query_pk'][query_pk]['count'] += 1
            analysis['query_pk'][query_pk]['duration'] += datum['duration']

        # Keyspace - CF - PK
        if primary_key and keyspace and column_family:
            if ks_cf_pk not in analysis['primary_key']:
                analysis['primary_key'][ks_cf_pk] = {
                    'count': 0,
                    'duration': 0,
                    'keyspace': keyspace,
                    'column_family': column_family,
                    'primary_key': primary_key,
                }
            analysis['primary_key'][ks_cf_pk]['count'] += 1
            analysis['primary_key'][ks_cf_pk]['duration'] += datum['duration']

        # Volume
        if minute not in analysis['volume']:
            analysis['volume'][minute] = {
                'count': 0,
                'duration': 0,
                'minute': minute,
            }
        analysis['volume'][minute]['count'] += 1
        analysis['volume'][minute]['duration'] += datum['duration']

        # Volume - Query - Primary Key
        if minute not in analysis['volume_top']:
            analysis['volume_top'][minute] = {}
        if query_pk not in analysis['volume_top'][minute]:
            analysis['volume_top'][minute][query_pk] = {
                'count': 0,
                'duration': 0,
                'query': query,
                'primary_key': primary_key,
                'minute': minute,
            }
        analysis['volume_top'][minute][query_pk]['count'] += 1
        analysis['volume_top'][minute][query_pk]['duration'] += datum['duration']

    # Min count
    analysis['query'] = {k: v for k, v in analysis['query'].items() if v['count'] >= config.min_count}
    analysis['query_pk'] = {k: v for k, v in analysis['query_pk'].items() if v['count'] >= config.min_count}
    analysis['primary_key'] = {k: v for k, v in analysis['primary_key'].items() if v['count'] >= config.min_count}
    analysis['volume'] = {k: v for k, v in analysis['volume'].items() if v['count'] >= config.min_count}
    analysis['volume_top'] = {
        minute: {
            k: v
            for k, v
            in minute_data.items()
            if v['count'] >= config.min_count
        }
        for minute, minute_data
        in analysis['volume_top'].items()
    }

    # Calculate average durations
    for k, v in analysis['query'].items():
        v['avg_duration'] = int(v['duration'] / v['count'])
    for k, v in analysis['query_pk'].items():
        v['avg_duration'] = int(v['duration'] / v['count'])
    for k, v in analysis['primary_key'].items():
        v['avg_duration'] = int(v['duration'] / v['count'])
    for k, v in analysis['volume'].items():
        v['avg_duration'] = int(v['duration'] / v['count'])
    for minute, minute_data in analysis['volume_top'].items():
        for k, v in minute_data.items():
            v['avg_duration'] = int(v['duration'] / v['count'])

    # Sort and limit
    analysis['query'] = sorted(analysis['query'].values(), key=lambda i: i[config.order_by], reverse=True)[
                        :config.top_n]
    analysis['query_pk'] = sorted(analysis['query_pk'].values(), key=lambda i: i[config.order_by], reverse=True)[
                           :config.top_n]
    analysis['primary_key'] = sorted(analysis['primary_key'].values(), key=lambda i: i[config.order_by],
                                     reverse=True)[:config.top_n]
    # Volume already sorted by timestamp
    analysis['volume'] = analysis['volume'].values()

    # Reduce volume top analysis: sort, limit to N per minute, and flatten.
    analysis['volume_top'] = list(itertools.chain.from_iterable([
        sorted(v.values(), key=lambda i: i[config.order_by], reverse=True)[:config.rows_per_minute]
        for k, v
        in analysis['volume_top'].items()
    ]))

    Timer.end('analysis')
    return analysis


class Reporter(object):
    """
    Report runner.
    """

    @classmethod
    def report(cls, analysis):
        """
        Write reports.

        :param dict analysis: Analysis.
        """
        logging.info('Writing reports')
        Timer.start('reporting')
        cls._write_query_report(analysis['query'])
        cls._write_query_pk_report(analysis['query_pk'])
        cls._write_primary_key_report(analysis['primary_key'])
        cls._write_volume_report(analysis['volume'])
        cls._write_volume_top_report(analysis['volume_top'])
        Timer.end('reporting')

    @classmethod
    def _write_query_report(cls, analysis):
        """
        Write slow_queries report.

        :param dict analysis: Query analysis.
        """
        headers = ['Count', 'Duration', 'Avg. Duration', 'Query']
        data = [
            [
                i['count'],
                i['duration'],
                i['avg_duration'],
                i['query'],
            ]
            for i in analysis
        ]
        cls._write_csv([headers] + data, 'slow_queries')

    @classmethod
    def _write_query_pk_report(cls, analysis):
        """
        Write slow_primary_keys report.

        :param dict analysis: Query PK analysis.
        """
        headers = ['Count', 'Duration', 'Avg. Duration', 'Primary Key', 'Query']
        data = [
            [
                i['count'],
                i['duration'],
                i['avg_duration'],
                i['primary_key'],
                i['query'],
            ]
            for i in analysis
        ]
        cls._write_csv([headers] + data, 'slow_primary_keys')

    @classmethod
    def _write_primary_key_report(cls, analysis):
        """
        Write slow_primary_keys report.

        :param dict analysis: Primary key analysis.
        """
        headers = ['Count', 'Duration', 'Avg. Duration', 'Keyspace', 'Column Family', 'Primary Key']
        data = [
            [
                i['count'],
                i['duration'],
                i['avg_duration'],
                i['keyspace'],
                i['column_family'],
                i['primary_key'],
            ]
            for i in analysis
        ]
        cls._write_csv([headers] + data, 'primary_keys')

    @classmethod
    def _write_volume_report(cls, analysis):
        """
        Write volume report.

        :param dict analysis: Volume analysis.
        """
        headers = ['Time', 'Count', 'Duration', 'Avg. Duration']
        data = [
            [
                i['minute'],
                i['count'],
                i['duration'],
                i['avg_duration'],
            ]
            for i in analysis
        ]
        cls._write_csv([headers] + data, 'volume')

    @classmethod
    def _write_volume_top_report(cls, analysis):
        """
        Write volume_top report.

        :param dict analysis: Volume top analysis.
        """
        headers = ['Time', 'Count', 'Duration', 'Avg. Duration', 'Primary Key', 'Query']
        data = [
            [
                i['minute'],
                i['count'],
                i['duration'],
                i['avg_duration'],
                i['primary_key'],
                i['query'],
            ]
            for i in analysis
        ]
        cls._write_csv([headers] + data, 'volume_top_n')

    @classmethod
    def _write_csv(cls, data, report_name):
        """
        Write CSV data to a file.

        :param list data: CSV rows.
        :param str report_name: Report filename.
        """
        filename = '{}.csv'.format(report_name)
        logging.info('Writing {}'.format(filename))
        with open(filename, 'w+') as fh:
            writer = csv.writer(fh)
            for row in data:
                writer.writerow(row)


def process_file(file_, config):
    """
    Process JSON slow query file.

    :param str file_: File.
    :param Config config: Configuration.

    :rtype: list
    :return: Slow query data.
    """
    ret = []
    logging.info('Loading JSON from {}'.format(file_))
    Timer.start('json_loading')
    with open(file_) as fh:
        data = json.load(fh)
    # TODO: Need a Timer.pause or something instead of end so we can aggregate multiple files
    Timer.end('json_loading')
    logging.info('Processing log messages')
    Timer.start('processing')
    for hit in data['hits']['hits']:
        try:
            timestamp = hit['_source']['@timestamp']
            try:
                message = hit['_source']['message']
            except KeyError:
                message = hit['_source']['@message']
            try:
                tags = hit['_source']['tags']
            except KeyError:
                tags = []
            if 'Query too slow' in message:
                try:
                    data = process_message(timestamp, message, tags, config)
                    ret.append(data)
                except Exception as e:
                    logging.warn(u'{}: {} {}'.format(repr(e), message, traceback.format_exc()))
            else:
                logging.warn(u'Not query too slow {}'.format(message))
        except KeyError:
            logging.warn(u'Invalid hit {}'.format(json.dumps(hit)))
    Timer.end('processing')
    return ret


if __name__ == '__main__':
    parser = ArgumentParser(description='Cassandra slow query log parser')
    parser.add_argument('file', nargs='+', help='Kibana search JSON files')
    parser.add_argument('--schema', help='CQL schema file')
    parser.add_argument('--queries', help='Additional query patterns')
    parser.add_argument('--tags', help='Tag: keyspace mappings')
    parser.add_argument('--top-n', help='Limit to top N rows', type=int, default=100)
    parser.add_argument('--rows-per-minute', help='Number of rows per minute', type=int, default=5)
    parser.add_argument('--min-count', help='Minimum number of occurrences', type=int, default=5)
    parser.add_argument('--order-by', help='Order results by', default='duration',
                        choices=['duration', 'avg_duration', 'count'])
    parser.add_argument('-v', action='store_true', default=False, help='Verbose output')
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.v else logging.INFO)

    configuration = Config(
        top_n=args.top_n,
        rows_per_minute=args.rows_per_minute,
        order_by=args.order_by,
        min_count=args.min_count
    )

    run(configuration, args.file, args.schema, args.queries, args.tags)
