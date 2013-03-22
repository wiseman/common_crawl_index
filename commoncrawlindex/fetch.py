"""Fetches a URL from the Common Crawl dataset.
"""

import gzip
import optparse
import StringIO
import sys

import boto

from commoncrawlindex import lookup
from commoncrawlindex import pbtree


OPTION_LIST = [
  optparse.make_option(
    '-b', '--s3-bucket-name',
    default='aws-publicdatasets', dest='s3_bucket_name',
    help='S3 bucket name of the Common Crawl Index.'),
  optparse.make_option(
    '-k', '--s3-key',
    default='/common-crawl/projects/url-index/url-index.1356128792',
    dest='s3_key', help='S3 key of the Common Crawl Index.'),
  optparse.make_option(
    '-l', '--local-index-file',
    default=None, dest='local_index_file',
    help='Path to local index file to use instead of the index in S3.'),
  optparse.make_option(
    '-O', '--output-to-file',
    default=False, action='store_true', dest='output_to_file',
    help='Write each fetched URL to a file named like the URL.'),
  optparse.make_option(
    '-C', '--compress',
    default=False, action='store_true', dest='compress',
    help='Keep the URL contents gzipped.')
  ]

KEY_TMPL = ('/common-crawl/parse-output/segment/{arcSourceSegmentId}/'
            '{arcFileDate}_{arcFilePartition}.arc.gz')


def arc_file(s3, bucket_name, info, decompress=True):
  """Reads an ARC file (see
  http://commoncrawl.org/data/accessing-the-data/).
  """
  bucket = s3.lookup(bucket_name)
  keyname = KEY_TMPL.format(**info)
  key = bucket.lookup(keyname)
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


def parse_options(arguments):
  parser = optparse.OptionParser(
    option_list=OPTION_LIST,
    usage='%prog [options] <reversed URL prefix>...',
    description=sys.modules[__name__].__doc__)
  options, args = parser.parse_args(arguments)
  if len(args) < 1:
    parser.print_help()
    sys.exit(2)
  return options, args


def main():
  options, args = parse_options(sys.argv[1:])
  s3 = boto.connect_s3(anon=True)
  stream = None
  if options.local_index_file:
    stream = open(options.local_index_file, 'rb')
  else:
    stream = lookup.BotoMap(
      s3,
      options.s3_bucket_name,
      options.s3_key)
  reader = pbtree.open_pbtree_reader(stream)

  try:
    for url_prefix in args:
      for url, d in reader.itemsiter(url_prefix):
        sys.stderr.write('Fetching %s\n' % (url,))
        contents = arc_file(
          s3, options.s3_bucket_name, d, decompress=(not options.compress))
        if options.output_to_file:
          filename = url_to_filename(url)
          if options.compress:
            filename = filename + '.gz'
          with open(filename, 'wb') as f:
            f.write(contents)
        else:
          print contents
  except KeyboardInterrupt:
    pass


if __name__ == '__main__':
  main()
