"""Prints a list of URLS from the Common Crawl Index that match a
prefix.

Usage:
  %s [options] <reversed URL prefix>
"""

import sys

import gflags

from commoncrawlindex import index

FLAGS = gflags.FLAGS

gflags.DEFINE_boolean(
  'print_metadata',
  False,
  'Print metadata for each URL.',
  short_name='m')


def main():
  try:
    argv = FLAGS(sys.argv)
  except gflags.FlagsError, e:
    sys.stderr.write('Error: %s\n%s%s\n' % (
        e,
        sys.modules[__name__].__doc__.replace('%s', sys.argv[0]),
        FLAGS))
    sys.exit(2)
  if len(argv) != 2:
    sys.stderr.write('Error: Wrong number of arguments.\n')
    sys.exit(1)
  index_reader = index.open_index_reader()
  try:
    for url, d in index_reader.itemsiter(argv[1]):
      if FLAGS.print_metadata:
        print url, d
      else:
        print url
  except KeyboardInterrupt:
    pass


if __name__ == '__main__':
  main()
