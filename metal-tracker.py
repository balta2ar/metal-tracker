#!/usr/bin/env python

import re
import sys
import netrc
import logging
from datetime import datetime
from shutil import copy2
from os.path import join
from os.path import basename
from os.path import dirname

from requests import Session
from bs4 import BeautifulSoup
import feedparser
import html2text
import pandas as pd

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

NETRC_HOST = 'metal-tracker'
LOGIN_URL = 'http://en.metal-tracker.com/user/login.html'
# FEED_FILENAME = 'rss.html'
# DB_FILENAME = 'metal-tracker.csv'
# TORRENT_DIRECTORY = '.'
BITTORRENT_CONTENT_TYPE = 'application/x-bittorrent'
LOG_FILENAME = 'metal-tracker.log'
LOG_FORMAT = '%(asctime)s %(module)s %(process)d %(levelname)s %(message)s'

# http://en.metal-tracker.com/torrents/178698.html
#PAGE_URL = 'http://en.metal-tracker.com/torrents/download/id/178698.html'
# PAGE_URL = 'http://en.metal-tracker.com/torrents/download/id/178693.html'

# Content-Type:
# text/html
# application/x-bittorrent

# {'Accept-Ranges': 'bytes',
#  'Cache-Control': 'no-store, no-cache, must-revalidate, post-check=0, '
#                   'pre-check=0',
#  'Connection': 'keep-alive',
#  'Content-Disposition': 'attachment; filename="Warnungstraum - Mirror Waters - '
#                         'Metal-Tracker.com.torrent"',
#  'Content-Transfer-Encoding': 'binary',
#  'Content-Type': 'application/x-bittorrent',
#  'Date': 'Fri, 24 Jun 2016 15:30:44 GMT',
#  'Expires': 'Tue, 1 Jan 1980 00:00:00 GMT',
#  'Last-Modified': 'Fri, 24 Jun 2016 15:30:44 GMT',
#  'Pragma': 'no-cache',
#  'Server': 'nginx/1.4.6 (Ubuntu)',
#  'Transfer-Encoding': 'chunked',
#  'X-Powered-By': 'PHP/5.5.9-1ubuntu4.9'}

# {'Cache-Control': 'no-store, no-cache, must-revalidate, post-check=0, '
#                    'pre-check=0',
#   'Connection': 'keep-alive',
#   'Content-Encoding': 'gzip',
#   'Content-Type': 'text/html',
#   'Date': 'Fri, 24 Jun 2016 15:33:24 GMT',
#   'Expires': 'Thu, 19 Nov 1981 08:52:00 GMT',
#   'Pragma': 'no-cache',
#   'Server': 'nginx/1.4.6 (Ubuntu)',
#   'Transfer-Encoding': 'chunked',
#   'Vary': 'Accept-Encoding',
#   'X-Powered-By': 'PHP/5.5.9-1ubuntu4.9'}

TIMESTAMP_FORMAT = '%a, %d %b %Y %X %z'
COLUMNS = ['timestamp', 'title', 'page_url', 'torrent_url']
SEPARATOR = '\t'


class Storage(object):
    def __init__(self, filename):
        self._filename =  filename
        self._db = self._read()

    def get_new_items(self, new_items):
        new_incoming = new_items[~new_items.title.isin(self._db.title)]
        missing_torrent_url = self._db[pd.isnull(self._db.torrent_url)]
        result = pd.concat([new_incoming, missing_torrent_url],
                           ignore_index=True, copy=True)
        # Make sure torrent_url is None for missing items.
        # This field will be filled up during download stage if download
        # succeedes.
        result.torrent_url = None
        return result

    def append_items(self, new_items):
        timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
        backup_filename = join(dirname(self._filename),
                               timestamp + '_' + basename(self._filename))
        copy2(self._filename, backup_filename)

        # NOTE: order is important here: new_items, self._db
        # as pd.concat will take first row if duplicates are found
        db = pd.concat([new_items, self._db], ignore_index=True, copy=True)
        db = db.drop_duplicates(subset='title')

        db.to_csv(self._filename, sep=SEPARATOR, encoding='utf-8',
                  header=None, index=False, columns=COLUMNS)

    def _read(self):
        db = pd.read_csv(self._filename, sep=SEPARATOR, header=None,
                         names=COLUMNS, parse_dates=True)
        db.timestamp = pd.to_datetime(db.timestamp) #, format=TIMESTAMP_FORMAT)
        return db


class Feed(object):
    def __init__(self, filename):
        self._filename = filename
        self._summary = {}

    def read(self):
        feed = feedparser.parse(self._filename)
        items = []

        for entry in feed['entries']:
            title = entry['title']
            page_url = entry['link']
            # Do not prepare torrent_url, it will be done in download stage
            #torrent_url = entry['link'].replace('/torrents/', '/torrents/download/id/')
            published = entry['published']
            timestamp = datetime.strptime(published, TIMESTAMP_FORMAT)
            timestamp = pd.to_datetime(timestamp).tz_convert(None)
            summary = html2text.html2text(entry['summary'])
            self._summary[title] = summary

            items.append({'timestamp': timestamp,
                          'title': title,
                          'page_url': page_url,
            # torrent_url=None because new items by definition are not complete
                          'torrent_url': None})
        feed = pd.DataFrame(items, columns=COLUMNS)
        feed.timestamp = feed.timestamp.astype(pd.datetime)
        return feed


class Downloader(Session):
    def __init__(self):
        super(Downloader, self).__init__()

        login, _, password = netrc.netrc().authenticators(NETRC_HOST)
        credentials = {
            'UserLogin[username]': login,
            'UserLogin[password]': password,
            'submit': 'Enter'
        }
        login_page = self.post(LOGIN_URL, data=credentials)


class MetalTracker(object):
    def __init__(self,
                 db_filename, feed_filename, torrent_directory,
                 blacklist_filename, downloader):
        self._db_filename = db_filename
        self._feed_filename = feed_filename
        self._torrent_directory = torrent_directory
        self._downloader = downloader

        with open(blacklist_filename) as file_object:
            self._blacklist_pattern = file_object.read().strip().replace('\n', '')
            self._blacklist_regexp = re.compile(self._blacklist_pattern, re.IGNORECASE)

    def download(self):
        feed = Feed(self._feed_filename)
        feed_items = feed.read()

        db = Storage(self._db_filename)
        new_items = db.get_new_items(feed_items)

        if len(new_items):
            logger.info('Blacklist pattern: %s' % self._blacklist_pattern)
            logger.info('Filtering new items (%d)' % len(new_items))

        def blacklist_matcher(row):
            summary = feed._summary.get(row.title, '')
            return self._blacklist_regexp.search(summary) is not None

        mask = new_items.apply(blacklist_matcher, axis=1)
        whitelisted = new_items[~mask]
        blacklisted = new_items[mask]

        self._print_summary(feed, whitelisted, blacklisted)

        if len(whitelisted):
            logger.info('Downloading new items (%d)' % len(whitelisted))

        downloaded_items = self._download_new_items(whitelisted)
        if len(downloaded_items):
            db.append_items(downloaded_items)
            logger.info('-' * 50)
            logger.info(downloaded_items)

    def _print_summary(self, feed, whitelisted, blacklisted):
        self._print_items('kept (whitelisted)', feed, whitelisted, preview=True)
        self._print_items('filtered out (blacklisted)', feed, blacklisted, preview=True)

        self._print_items('kept (whitelisted)', feed, whitelisted)
        self._print_items('filtered out (blacklisted)', feed, blacklisted)

    def _print_items(self, message, feed, items, preview=False):
        if len(items) == 0:
            return

        logger.info('-' * 50)
        logger.info('The following entries were %s (%d)' % (message, len(items)))
        logger.info('-' * 50)
        for index, row in items.iterrows():
            summary = feed._summary.get(row.title, '')
            style = self._get_style_from_summary(summary)
            logger.info('%s / %s', row.title, style)
            if not preview:
                logger.info(row.page_url)
                logger.info(summary)
                logger.info('-' * 50)

    def _get_style_from_summary(self, summary):
        for line in summary.split('\n'):
            if 'style:' in line.lower():
                style = line.replace('*', '').strip()
                style = style[style.lower().find('style:')+6:]
                return style.strip()

        return '<unknown>'

    def _download_new_items(self, new_items):
        downloaded_items = pd.DataFrame(columns=new_items.columns)

        for i, item in new_items.iterrows():
            torrent_url = self._get_torrent_url(item.page_url)
            if self._download_torrent(torrent_url, item.title, self._torrent_directory):
                # Append torrent_url is succeeded
                copied = item.copy(deep=True)
                copied.torrent_url = torrent_url
                downloaded_items = downloaded_items.append(copied, ignore_index=True)
            else:
                # torrent_url field should remain empty if failed
                downloaded_items = downloaded_items.append(item, ignore_index=True)

        return downloaded_items

    def _get_torrent_url(self, page_url):
        torrent_url = page_url.replace('/torrents/', '/torrents/download/id/')
        return torrent_url

    def _download_torrent(self, torrent_url, title, destination_dir):
        logger.info('Getting "%s" (%s)' % (title, torrent_url))
        page = self._downloader.get(torrent_url)
        if page.status_code != 200:
            logger.error('Failure: %s %s' % (page.status_code, page.reason))
            return False

        content_type = page.headers.get('Content-Type')
        if content_type != BITTORRENT_CONTENT_TYPE:
            logger.warning('Torrent is not ready yet: %s (%d)' % (content_type, len(page.content)))
            return False

        title = title.replace('/', '')
        filename = join(destination_dir, title) + '.torrent'
        with open(filename, 'wb') as file_object:
            file_object.write(page.content)
        logger.info('Saved "%s" to %s' % (title, filename))
        return True


def init_logging():
    fh = logging.FileHandler(LOG_FILENAME)
    fh.setLevel(logging.DEBUG)

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    formatter = logging.Formatter(LOG_FORMAT)
    ch.setFormatter(formatter)
    fh.setFormatter(formatter)

    logger.addHandler(ch)
    logger.addHandler(fh)




def main(args):
    init_logging()

    if len(args) != 4:
        logger.error('usage: metal-tracker.py <db_filename> <feed_filename> <output_dir> <blacklist_filename>')
        exit(1)

    pd.set_option("display.max_columns", 999)
    pd.set_option("display.max_rows", 999)
    pd.set_option('display.max_columns', 999)
    pd.set_option('display.max_colwidth', 999)
    pd.set_option('display.width', None)

    db_filename, feed_filename, torrent_directory, blacklist_filename = args
    tracker = MetalTracker(db_filename, feed_filename,
                           torrent_directory, blacklist_filename,
                           Downloader())
    tracker.download()


if __name__ == '__main__':
    main(sys.argv[1:])
