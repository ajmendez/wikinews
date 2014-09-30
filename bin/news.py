#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Parse wikipedia to get the news'''

import os
import sys
import gzip
import dataset
# import codecs
# import requests
from dateutil import parser
from subprocess import call
import urllib



DEBUG = ('debug' in sys.argv)
if __name__ == '__main__':
    try:
        DATE = sys.argv[1]
    except:
        print 'Usage: {} 2014-09-29-01 # year-month-date-hr'.format(sys.argv[0])
        sys.exit(1)
    


# location where we store the intemediate bits.
# keep it while we debug then use tempfile in the end
TMPDIR = os.path.expanduser('~/tmp/wikinews')

DBFILE = os.path.expanduser('~/tmp/wikinews/wikinews.db')
DBFILE = '/wikinews.db'


# url to get items
URL = 'http://dumps.wikimedia.org/other/pagecounts-raw/{year}/{year}-{month:02d}/pagecounts-{year}{month:02d}{day:02d}-{hour:02d}0000.gz'
HASHURL = 'http://dumps.wikimedia.org/other/pagecounts-raw/{year}/{year}-{month:02d}/md5sums.txt'


BAD = [
    'Special:',
    'Main_Page',
    'Talk:',
    'User:',
    'User_talk:',
    'Wiktionary:',
]

# Have at least this number of hits to be considered
LIMIT = 100

def parsedate(date):
    '''Parse the date into something useful.  dateutil is doing 
    all of the heavy work here, but this function abstracts any extra logic.'''
    return parser.parse(date)


def getpage(date, outdir=TMPDIR):
    '''TODO: Check against hash: http://dumps.wikimedia.org/other/pagecounts-raw/2014/2014-09/md5sums.txt'''
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    
    url = URL.format(year=date.year, month=date.month, 
                     day=date.day, hour=date.hour)
    outfile = os.path.join(outdir, os.path.basename(url))
    if os.path.exists(outfile):
        print "Already retreved filed: {}".format(outfile)
        return outfile
    call(['wget', '-c', '-O', outfile, url])
    return outfile
    # mem hog -- wget displays more information too
    # req = requests.get(url) # stream=True?
    # with open(outfile, 'w') as out:
    #     out.write(req.text)


def tablekey(date):
    return '{year}_{month:02d}_{day:02d}_{hour:02d}'.format(year=date.year, month=date.month, day=date.day, hour=date.hour)

def _update(i):
    if (i%100) == 0:
        sys.stdout.write('.')
        sys.stdout.flush()

def syncdb(filename, date, dbfile=DBFILE):
    '''Sync the date file into the db'''
    print 'Working on {}'.format(filename)
    if DEBUG:
        db = dataset.connect('sqlite:///:memory:')
    else:
        db = dataset.connect('sqlite://{}'.format(dbfile))
    
    titles = db.get_table('titles')
    files = db.get_table('files')
    thisdate = db.get_table(tablekey(date))
    
    if files.find_one(filename=filename) is not None:
        print 'File already added: {}'.format(filename)
        return db
    
    k = 0
    with gzip.open(filename) as gz:
        for i,line in enumerate(gz):
            countrycode,title,requests,contentsize = line.strip().split()
            # remove percent encoded to utf-8
            requests = int(requests)
            if 'en' not in countrycode: continue
            if any([b in title for b in BAD]): continue
            if DEBUG and (requests < LIMIT): continue
            try:
                # Try to use bytes -- otherwise use the string if failure
                title = urllib.unquote(title).decode('utf-8')
            except:
                print '\nBad Title: {}'.format(title)
            
            # get the title id
            if titles.find_one(title=title, code=countrycode) is None:
                # add if needed
                titles.insert(dict(title=title, code=countrycode, total=0))
            row = titles.find_one(title=title)
            total = row['total'] + requests
            titles.update(dict(title=title, total=total), ['title'])
            
            # add to day
            
            title_id=row['id']
            if thisdate.find_one(title_id=title_id) is None:
                thisdate.insert(dict(title_id=title_id, count=0))
            count = thisdate.find_one(title_id=title_id)['count'] + requests
            thisdate.update(dict(title_id=title_id, count=count), ['title_id'])
            
            k+=1
            _update(k)
            if DEBUG and (k > 100):
                break
        print '\nDone!'
        files.insert(dict(filename=filename))
        
    return thisdate, titles

def topdb(thisdate, titles, limit=10):
    '''Print out the top results for this day.  The rows should be 
    sorted coming out of the find, but eh --- sort to be sure that things make
    sense.  Things are not unique.'''
    results = thisdate.find(order_by=['-count'], _limit=limit)
    fmt = u"{r[count]: 10d} {t[code]:<10s} : {t[title]:}"
    for row in sorted(results, key=lambda x: -x['count']):
        t = titles.find_one(id=row['title_id'])
        print fmt.format(r=row, t=t)
    


if __name__ == '__main__':
    date = parsedate(DATE)
    outfile = getpage(date)
    thisdate,titles = syncdb(outfile, date)
    topdb(thisdate, titles)


