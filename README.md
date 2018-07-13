# Cassandra Slow Query Log Analysis

Gather aggregate statistical information on Cassandra slow query logs from ElasticSearch.

## Download Logs

Use `download_slow_queries.py` to download slow query logs from ElasticSearch.

## Analyze Logs

Use `analyze_slow_queries.py` to analyze the slow query data and generate a set of CSV files. These identify the slowest query and slowest primary keys.

## Find Primary Key Owners

Use `find_pk_nodes.py` to find out what nodes in your cluster own the slow primary keys.
