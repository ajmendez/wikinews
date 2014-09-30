#!/usr/bin/env python
'''Parse wikipedia to get the news'''

import os
import sys
import requests
from dateutil import parser
import gzip


try:
    DATE = sys.argv[1]
except:
    print 'Usage: {} 2014-09-29-01 # year-month-date-hr'.format(sys.argv[0])
    sys.exit(1)



# location where we store the intemediate bits.
# keep it while we debug then use tempfile in the end
TMPDIR = os.path.expanduser('~/tmp/wikinews')

# url to get items
URL = 'http://dumps.wikimedia.org/other/pagecounts-raw/{year}/{year}-{month:02d}/pagecounts-{year}{month:02d}{day:02d}-{hour:02d}0000.gz'


def parsedate(date):
    '''Parse the date into something useful.  dateutil is doing 
    all of the heavy work here, but this function abstracts any extra logic.'''
    return parser.parse(date)

def getloc(date):
    url = URL.format(year=date.year, month=date.month, 
                     day=date.day, hour=date.hour)
    outfile = os.path.join(outdir, os.path.basename(url))



def getpage(date, outdir=TMPDIR):
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    
    url, outfile = getloc(date)
    if os.path.exists(outfile):
        print "Already retreved filed: {}".format(outfile)
        return
    req = requests.get(url)
    with open(outfile, 'w') as out:
        out.write(req.text)

def syncdb(date):
    outfile = getloc(date)[1]
    with gzip.open(outfile,'r') as gz:
        print gz.readline()


if __name__ == '__main__':
    date = parsedate(DATE)
    # getpage(date)
    syncdb(date)



