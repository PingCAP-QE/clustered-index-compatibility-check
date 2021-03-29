# Clustered Index Compatibility Check

This is a simple tool to test data compatibility of clustered index of tidb.

## Usage

1. setup test data: `clustered-index-compatibility-check -o table-digests.out -dsn '...' setup`.
2. do some operations, eg.
   1. upgrading the cluster.
   2. backup and restore data.
3. check data after operations: `clustered-index-compatibility-check -i table-digests.out -admin-check -dsn '...' check`.
