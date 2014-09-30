#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Parse wikipedia to get the news'''
import os
import sys
from datetime import datetime, timedelta
from news import getpage, syncdb, getloc, TMPDIR

START = datetime(2014,9,21)
END   = datetime(2014,9,28)
DELTA = timedelta(hours=1)

MULTI = ('multi' in sys.argv)
DOWNLOAD = ('download' in sys.argv)
TMPDIR = os.path.expanduser('~/raid/tmp/wikinews/')

def gendates():
    current = START
    while current <= END:
        yield current
        current += DELTA

def sync(date):
    if DOWNLOAD:
        outfile = getpage(date, TMPDIR)
    else:
        outfile = getloc(date, TMPDIR)[1]
        if os.path.exists(outfile):
            syncdb(outfile, date)


def multisync():
    from pysurvey import multi
    multi.multi(sync, gendates(), n=5)

def singlesync():
    for date in gendates():
        sync(date)

if __name__ == '__main__':
    if MULTI:
        multisync()
    else:
        singlesync()