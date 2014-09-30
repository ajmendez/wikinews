#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Parse wikipedia to get the news'''
from datetime import datetime, timedelta
from news import getpage, syncdb

START = datetime(2014,9,21)
END   = datetime(2014,9,28)
DELTA = timedelta(hours=1)


def gendates():
    current = START
    while current <= END:
        yield current
        current += DELTA

def sync(date):
    outfile = getpage(date)
    syncdb(outfile, date)


def multisync():
    from pysurvey import multi
    multi.multi(sync, gendates(), n=5)

def singlesync():
    for date in gendates():
        sync(date)

if __name__ == '__main__':
    # multisync()
    singlesync()