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
