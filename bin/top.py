import dataset
from datetime import datetime
from news import tablekey,DBFILE
#  sqlite:////absolute/path/to/file.db


if __name__ == '__main__':
    date = datetime(2014,9,21)
    
    dbfile = 'wikinews.db'
    db = dataset.connect('sqlite://{}'.format(DBFILE))
    titles = db.get_table('titles')
    files = db.get_table('files')
    thisdate = db.get_table(tablekey(date))
    
    limit=10
    
    results = thisdate.find(order_by=['-count'], _limit=limit)
    fmt = u"{r[count]: 10d} {t[code]:<10s} : {t[title]:}"
    for row in sorted(results, key=lambda x: -x['count']):
        t = titles.find_one(id=row['title_id'])
        print fmt.format(r=row, t=t)
    
    print '---'*10
    
    results = titles.find(order_by=['-total'], _limit=limit)
    fmt = u"{t[total]: 10d} {t[code]:<10s} : {t[title]:}"
    for row in sorted(results, key=lambda x: -x['total']):
        print fmt.format(t=t)