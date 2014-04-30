# commoncrawlindex

Common Crawl is an open repository of web crawl data that can be
accessed and analyzed by everyone.

commoncrawlindex is a simple Python interface to the [Common Crawl URL
Index](http://commoncrawl.org/common-crawl-url-index/) and the [Common
Crawl
dataset](https://commoncrawl.atlassian.net/wiki/display/CRWL/About+the+Data+Set).

This is a pretty significant fork of
[trivio/common_crawl_index](https://github.com/trivio/common_crawl_index),
created with the goal of being easier to use.


## Installation

From github:

```
$ git clone git@github.com:wiseman/common_crawl_index.git
$ cd common_crawl_index
$ python setup.py install
```

Using pip:

```
$ pip install commoncrawlindex
```

## Usage

You can use commoncrawlindex to find URLs that are in the Common Crawl
dataset and you can use it to fetch the stored contents of those URLs.

### Looking up URLs

Searching the index is done with `cci_lookup`.  You give it a URL
prefix on the command line and it will write every matching URL in the
index to stdout:

```
$ cci_lookup com.metafilter  # Find every URL at metafilter.com
com.metafilter.ask/100000/Ideas-for-pureed-food-for-a-30something-who-is-really-picky:http
com.metafilter.ask/100014/How-to-care-for-my-geek-husband:http
com.metafilter.ask/100020/How-can-I-investigateremedy-possibly-misleading-loan-agreements-from-several-years-ago:http
com.metafilter.ask/100033/Whats-the-best-way-to-react-to-a-socially-awkward-situation:http
[...and so on for about 120000 URLs.]
```

Note that the URL index contains reversed domains and has the protocol
last, and you query by giving `cci_lookup` a reversed URL prefix.

URLs are returned in lexicographic order.

Here's an example of a more specifix query that finds all URLs from
the projects.metafilter.com sub-site:

```
$ cci_lookup com.metafilter.projects
com.metafilter.projects/1029/bunt-cake-a-webcomic-thing:http
com.metafilter.projects/1030/Question-Party:http
com.metafilter.projects/1041/Free-magazines-on-your-iPhone:http
[...etc. for a total of about 250 URLs.]
```

Even more specific, finding URLs of projects posts whose IDs begin with 3:

```
$ cci_lookup com.metafilter.projects/3
com.metafilter.projects/3012/You-are-listening-to-Los-Angeles:http
com.metafilter.projects/3031/RetCon-Artists-Improving-the-Future-by-Improving-the-Past:http
com.metafilter.projects/3042/GLTICH-Karaoke:http
[etc.]
```

#### Metadata

You can also display the metadata associated with each URL:

```
$ cci_lookup --print_metadata com.metafilter.projects/3
com.metafilter.projects/3012/You-are-listening-to-Los-Angeles:http	{"compressedSize": 7702, "arcSourceSegmentId": 1346876860782, "arcFilePartition": 4245, "arcFileDate": 1346908921144, "arcFileOffset": 1850172}
com.metafilter.projects/3031/RetCon-Artists-Improving-the-Future-by-Improving-the-Past:http	{"compressedSize": 6464, "arcSourceSegmentId": 1346876860782, "arcFilePartition": 4245, "arcFileDate": 1346908597590, "arcFileOffset": 93006430}
com.metafilter.projects/3042/GLTICH-Karaoke:http	{"compressedSize": 6015, "arcSourceSegmentId": 1346876860782, "arcFilePartition": 3331, "arcFileDate": 1346908329648, "arcFileOffset": 88640475}
[etc.]
```

Metadata is in JSON format and is separated from the URL by a tab
character.  Say you want to use Hadoop to process common crawl data
for a single site.  You don't need to process every source segment,
but which ones do you need?  Here's one way to find out (using
[jq](http://stedolan.github.io/jq/)):

```
$ cci_lookup --print_metadata com.metafilter |
    awk -F '\t' '{print $2;}' |
    jq '.arcSourceSegmentId' |
    sort |
    uniq -c |
    sort -rn

  21439 1346876860765
  20182 1346876860838
  18223 1346876860840
  13568 1346876860819
   8843 1346876860614
   8717 1346876860807
   6748 1346876860877
   6580 1346876860817
   3822 1346823846176
   1662 1346876860782
   1480 1346876860648
   1476 1346876860609
   1055 1346876860804
    979 1346876860843
    935 1346876860798
    912 1346876860493
    633 1346823845675
    537 1346876860454
    409 1346876860795
    296 1346876860567
    232 1346876860828
    226 1346876860445
     44 1346823846125
     42 1346823846039
      2 1346876860779
      2 1346876860777
      1 1346876860774
      1 1346876860611
      1 1346823846150
```

That's 29 different segments, which is only about half of the
[58 total valid segments](https://s3.amazonaws.com/aws-publicdatasets/common-crawl/parse-output/valid_segments.txt).


### Fetching URLs

You can fetch URLs from the Common Crawl dataset using `cci_fetch`.
You give it a specific URL or URL prefix and it will fetch the saved
pages and write them to stdout:

```
# cci_fetch 'com.metafilter.projects/3012/You-are-listening-to-Los-Angeles:http'
Fetching com.metafilter.projects/3012/You-are-listening-to-Los-Angeles:http
http://projects.metafilter.com/3012/You-are-listening-to-Los-Angeles 50.22.177.14 20120516211439 text/html 31096
HTTP/1.1 200 OK
Date:Wed, 16 May 2012 21:14:36 GMT
Server:Apache
Vary:Accept-Encoding
Connection:close
Transfer-Encoding:chunked
Content-Type:text/html; charset=UTF-8
Content-Encoding:gzip
x-commoncrawl-DetectedCharset:UTF-8

<a name="content"></a>

<div id="page">
	


<div id="right" style="margin:0;padding:0;float:right;width:145px;">
[etc.]
```

(Note that the page saved in the Common Crawl dataset includes HTTP
headers.)

If you're fetching lots of pages, you can save them in individual
files.  Here's how you would download every page the dataset has from
metafilter.com:

```
$ cci_fetch --output_to_file com.metafilter
Fetching com.metafilter.ask/100000/Ideas-for-pureed-food-for-a-30something-who-is-really-picky:http
Fetching com.metafilter.ask/100014/How-to-care-for-my-geek-husband:http
Fetching com.metafilter.ask/100020/How-can-I-investigateremedy-possibly-misleading-loan-agreements-from-several-years-ago:http
[etc.]
```

If you're downloading lots of data, you may want to keep it in its
compressed (gzipped) format:

```
$ cci_fetch --compress --output_to_file com.metafilter
```

Here's how to download everything to individual gzipped files using 20
parallel `cci_fetch` processes:

```
$ cci_lookup com.metafilter | xargs -d '\n' -n 100 -P 20 cci_fetch -C -O
```
