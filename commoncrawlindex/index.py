"""High-level index access functions."""

import gflags

from commoncrawlindex import pbtree
from commoncrawlindex import s3

FLAGS = gflags.FLAGS

gflags.DEFINE_string(
  'index_path',
  ('s3://aws-publicdatasets/common-crawl/projects/url-index/'
   'url-index.1356128792'),
  'Path to the URL index to use.  Can be an s3:// URI or a local file path.',
  short_name='i')


def open_index_stream(path=None):
  """Opens a index stream for the index at a given path.

  Args:
    path(string): A local file path or an a3:// URI to an index file.
      Default is the value of --index_path.
  """
  path = path or FLAGS.index_path
  if s3.is_s3_uri(path):
    s3_conn = s3.get_s3_connection()
    stream = s3.BotoMap(s3_conn, path)
  else:
    stream = open(path, 'rb')
  return stream


def open_index_reader(path=None):
  """Opens and returns a reader for the index at a given path.

  Args:
    path(string): A local file path or an s3:// URI to an index
      file. Default is the value of --index_path.
  """
  path = path or FLAGS.index_path
  stream = open_index_stream(path=path)
  return pbtree.open_pbtree_reader(stream)
