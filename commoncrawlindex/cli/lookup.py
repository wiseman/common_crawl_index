"""Looks up URLs in the Common Crawl Index."""

import struct

import boto
import gflags

from commoncrawlindex.cli import s3

FLAGS = gflags.FLAGS

gflags.DEFINE_string(
  'index_path',
  ('s3://aws-publicdatasets/common-crawl/projects/url-index/'
   'url-index.1356128792'),
  'Path to the URL index to use.  Can be an s3:// URI or a local file path.',
  short_name='i')


def open_index_stream(path=None):
  path = path or FLAGS.index_path
  if s3.is_s3_uri(path):
    s3_conn = boto.connect_s3(anon=True)
    stream = s3.BotoMap(s3_conn, path)
  else:
    stream = open(path, 'rb')
  return stream
