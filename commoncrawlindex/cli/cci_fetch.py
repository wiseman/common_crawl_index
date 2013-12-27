"""Fetches a URL from the Common Crawl dataset.

Usage:
  %s [options] <reversed URL prefix>...
"""

import inspect
import gzip
import StringIO
import sys

import gflags

from commoncrawlindex import cli
from commoncrawlindex import s3
from commoncrawlindex import index

FLAGS = gflags.FLAGS

gflags.DEFINE_boolean(
  'output_to_file',
  False,
  'Write each fetched URL to a file named like the URL.',
  short_name='O')
gflags.DEFINE_boolean(
  'compress',
  False,
  'Keep the URL contents gzipped.',
  short_name='C')


_S3_URI_TMPL = (
  's3://aws-publicdatasets/common-crawl/parse-output/segment/'
  '{arcSourceSegmentId}/{arcFileDate}_{arcFilePartition}.arc.gz')


def arc_file(s3_conn, info, decompress=True):
  """Reads an ARC file (see
  http://commoncrawl.org/data/accessing-the-data/).
  """
  s3_path = _S3_URI_TMPL.format(**info)
  bucket_name, key_name = s3.parse_s3_uri(s3_path)
  bucket = s3_conn.lookup(bucket_name)
  key = bucket.lookup(key_name)
  start = info['arcFileOffset']
  end = start + info['compressedSize'] - 1
  headers = {'Range': 'bytes={}-{}'.format(start, end)}
  contents = key.get_contents_as_string(headers=headers)
  if decompress:
    chunk = StringIO.StringIO(contents)
    return gzip.GzipFile(fileobj=chunk).read()
  else:
    return contents


def url_to_filename(url):
  """Converts a URL to a valid filename."""
  return url.replace('/', '_')


def main(argv):
  if len(argv) < 2:
    raise cli.UsageError('Wrong number of arguments.')
  index_reader = index.open_index_reader()
  s3_conn = s3.get_s3_connection()
  try:
    for url_prefix in argv[1:]:
      for url, d in index_reader.itemsiter(url_prefix):
        sys.stderr.write('Fetching %s\n' % (url,))
        contents = arc_file(s3_conn, d, decompress=(not FLAGS.compress))
        if FLAGS.output_to_file:
          filename = url_to_filename(url)
          if FLAGS.compress:
            filename = filename + '.gz'
          with open(filename, 'wb') as f:
            f.write(contents)
        else:
          print contents
  except KeyboardInterrupt:
    pass


def cli_main():
  cli.App(main=main, usage=inspect.getmodule(main).__doc__).run()

if __name__ == '__main__':
  cli_main()
