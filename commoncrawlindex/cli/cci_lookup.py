"""Prints a list of URLS from the Common Crawl Index that match a
prefix.

Usage:
  %s [options] <reversed URL prefix>
"""

import inspect
import json

import gflags

from commoncrawlindex import cli
from commoncrawlindex import index

FLAGS = gflags.FLAGS

gflags.DEFINE_boolean(
  'print_metadata',
  False,
  ('Print metadata for each URL. Metadata is in JSON format and is separated '
   'from the URL with a tab character.'),
  short_name='m')


def main(argv):
  if len(argv) != 2:
    raise cli.UsageError('Wrong number of arguments.')
  index_reader = index.open_index_reader()
  try:
    for url, d in index_reader.itemsiter(argv[1]):
      if FLAGS.print_metadata:
        print '%s\t%s' % (url, json.dumps(d))
      else:
        print url
  except KeyboardInterrupt:
    pass


def cli_main():
  cli.App(main=main, usage=inspect.getmodule(main).__doc__).run()

if __name__ == '__main__':
  cli_main()
