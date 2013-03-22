"""High-level S3 functions."""

import re
import threading

import boto


class Error(Exception):
  pass


_S3_URI_RE = re.compile(r's3://([^/]+)/?(.*)')


def parse_s3_uri(s3_path):
  """Parses an s3:// URI into (bucket, key)."""
  m = _S3_URI_RE.match(s3_path)
  if not m:
    raise Error('%s is not a valid S3 URI' % (s3_path,))
  bucket = m.group(1)
  key_prefix = m.group(2)
  return (bucket, key_prefix)


def is_s3_uri(path):
  """Returns true if PATH is an S3 URI."""
  m = _S3_URI_RE.match(path)
  return not(not m)


g_s3_connections = threading.local()


def get_s3_connection():
  """Gets an S3 (boto) connection.

  Connections are cached per-thread.
  """
  global g_s3_connections
  if not hasattr(g_s3_connections, 'connection'):
    g_s3_connections.connection = boto.connect_s3(anon=True)
  return g_s3_connections.connection


class BotoMap(object):
  """A random-access interface to an S3 file."""
  def __init__(self, s3_conn, s3_uri):
    bucket_name, key_name = parse_s3_uri(s3_uri)
    bucket = s3_conn.lookup(bucket_name)
    self.key = bucket.lookup(key_name)
    self.block_size = 2 ** 16
    self.cached_block = -1

  def __getitem__(self, i):
    if isinstance(i, slice):
      start = i.start
      end = i.stop - 1
    else:
      start = i
      end = start + 1
    return self.fetch(start, end)

  def fetch(self, start, end):
    try:
      return self.key.get_contents_as_string(
        headers={'Range': 'bytes={0}-{1}'.format(start, end)}
      )
    except boto.exception.S3ResponseError, e:
      # invalid range, we've reached the end of the file.
      if e.status == 416:
        return ''
