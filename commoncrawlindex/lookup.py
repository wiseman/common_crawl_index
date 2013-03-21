# Copyright [2012] [Triv.io, Scott Robertson]
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
"""Prints a list of URLs from the Common Crawl Index that match a
prefix.
"""

import optparse
import struct
import sys

import boto

import commoncrawlindex
from commoncrawlindex import pbtree


OPTION_LIST = [
  optparse.make_option(
    '-m', '--print-metadata',
    default=False, action='store_true', dest='print_metadata',
    help='Print metadata.'),
  ]


class BotoMap(object):
  def __init__(self, s3, bucket_name, key_name):
    bucket = s3.lookup(bucket_name)
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


def parse_options(arguments):
  parser = optparse.OptionParser(
    option_list=OPTION_LIST,
    usage='%prog [options] <URL prefix>',
    description=sys.modules[__name__].__doc__)
  options, args = parser.parse_args(arguments)
  if len(args) != 1:
    parser.print_help()
    sys.exit(2)
  return options, args


def main():
  options, args = parse_options(sys.argv[1:])
  s3 = boto.connect_s3(anon=True)
  mmap = BotoMap(
    s3,
    'aws-publicdatasets',
    '/common-crawl/projects/url-index/url-index.1356128792',
    )

  reader = pbtree.PBTreeDictReader(
    mmap,
    value_format="<QQIQI",
    item_keys=(
      'arcSourceSegmentId',
      'arcFileDate',
      'arcFilePartition',
      'arcFileOffset',
      'compressedSize'
    )
  )

  try:
    for url, d in reader.itemsiter(args[0]):
      if options.print_metadata:
        print url, d
      else:
        print url
  except KeyboardInterrupt:
    pass


if __name__ == "__main__":
  main()
