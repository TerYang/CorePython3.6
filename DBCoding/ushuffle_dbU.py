#!/usr/bin/env python

from distutils.log import warn as printf
import os
from random import randrange as rand
from random import randint


if isinstance(__builtins__, dict) and 'raw_input' in __builtins__:
    scanf = raw_input
elif hasattr(__builtins__, 'raw_input'):
    scanf = raw_input
else:
    scanf = input

COLSIZ = 10
FIELDS = ('login_', 'userid', 'projid')
RDBMSs = {'s': 'sqlite', 'm': 'mysql', 'g': 'gadfly'}
DBNAME = 'menagerie'
DBUSER = 'root'
TABLENAME = 'adaya'
DB_EXC = None
NAMELEN = 16

tformat = lambda s: str(s).title().ljust(COLSIZ)
cformat = lambda s: s.upper().ljust(COLSIZ)

def setup():
    return RDBMSs[scanf('''
        Choose a database system:
        (M)ySQL
        (G)adfly
        (S)QLite
        Enter choice: ''').strip().lower()[0]]

def connect(db):
    global DB_EXC
    dbDir = '%s_%s' % (db, DBNAME)

    if db == 'sqlite':
        try:
            import sqlite3
        except ImportError:
            try:
                from pysqlite2 import dbapi2 as sqlite3
            except ImportError:
                return None

        DB_EXC = sqlite3
        if not os.path.isdir(dbDir):
            os.mkdir(dbDir)
        cxn = sqlite3.connect(os.path.join(dbDir, DBNAME))

    elif db == 'mysql':
        try:
            import pymysql
            cxn = pymysql.connect(host='localhost',
                                         user='root',
                                         password='ada',
                                         db='menagerie',
                                         charset='utf8mb4',
                                         cursorclass=pymysql.cursors.DictCursor)
        except:
            return None
        # try:
        #     import MySQLdb
        #     import _mysql_exceptions as DB_EXC
        #
        #     try:
        #         cxn = MySQLdb.connect(db=DBNAME)
        #     except DB_EXC.OperationalError:
        #         try:
        #             cxn = MySQLdb.connect(user=DBUSER)
        #             cxn.query('CREATE DATABASE %s' % DBNAME)
        #             cxn.commit()
        #             cxn.close()
        #             cxn = MySQLdb.connect(db=DBNAME);
        #
        #         except DB_EXC.OperationalError:
        #             return None
        # except ImportError or ModuleNotFoundError:
        #     try:
        #         import mysql.connector
        #         import mysql.connector.errors as DB_EXC
        #         try:
        #             # print(getattr(mysql.connector.Connect,'password',10))
        #             cxn = mysql.connector.Connect(**{
        #                 'database': DBNAME,
        #                 'user': DBUSER,
        #             })
        #         except DB_EXC.InterfaceError:
        #             return None
        #     except ImportError:
        #         return None
    elif db == 'gadfly':
        try:
            from gadfly import gadfly
            DB_EXC = gadfly
        except ImportError:
            return None

        try:
            cxn = gadfly(DBNAME, dbDir)
        except IOError:
            cxn = gadfly()
            if not os.path.isdir(dbDir):
                os.mkdir(dbDir)
            cxn.startup(DBNAME, dbDir)
    else:
        return None
    return cxn


def create(cur):
    try:
        sql = "CREATE TABLE tuanpinya (%s  CHAR(20) NOT NULL,%s  INT ,%s INT)"%FIELDS
        # sql = "CREATE TABLE `adaya` (`login_`  VARCHAR(%d),`userid` INTEGER,`projid` INTEGER)" % NAMELEN
        cur.execute(sql)
    except (DB_EXC.OperationalError, DB_EXC.ProgrammingError):
        drop(cur)
        create(cur)

drop = lambda cur: cur.execute('DROP TABLE `%s`'% TABLENAME)

NAMES = (
    ('aaron', 8312), ('angela', 7603), ('dave', 7306),
    ('davina',7902), ('elliot', 7911), ('ernie', 7410),
    ('jess', 7912), ('jim', 7512), ('larry', 7311),
    ('leslie', 7808), ('melissa', 8602), ('pat', 7711),
    ('serena', 7003), ('stan', 7607), ('faye', 6812),
    ('amy', 7209), ('mona', 7404), ('jennifer', 7608),
)

def randName():
    pick = set(NAMES)
    while pick:
        yield pick.pop()

def insert(cur, db):
    if db == 'sqlite':
        cur.executemany("INSERT INTO users VALUES(?, ?, ?)",
        [(who, uid, rand(1,5)) for who, uid in randName()])
    elif db == 'gadfly':
        for who, uid in randName():
            cur.execute("INSERT INTO users VALUES(?, ?, ?)",
            (who, uid, rand(1,5)))
    elif db == 'mysql':
        # for who, uid in randName():
        #     print(who, uid,randint(1,5))
        #     print(type(who),type(uid),type(randint(1,5)))
        # sql = "INSERT INTO adaya (login_,userid, projid) VALUES ('%s', '%d',  %d)" %('Mac', 20, 2000)
        # print(cur.mogrify(sql))
        sql1 = "INSERT INTO adaya VALUES(%s,%s,%s)"
        # print(cur.mogrify(sql1))
        # exit()
        l = cur.executemany(sql1,[(who, uid,randint(1,5)) for who, uid in randName()])
        print('{} inserted~~~~~~~~~~~~~~~~~~~'.format(l))

getRC = lambda cur: cur.rowcount if hasattr(cur, 'rowcount') else -1

def update(cur):
    fr = 4#rand(1,5)
    to = 100#rand(1,5)
    l = cur.execute(
        "UPDATE adaya SET projid=%d WHERE projid=%d" % (to, fr))
    return fr, to, getRC(cur),l

def delete(cur):
    rm = rand(1,5)
    sql = 'DELETE FROM %s WHERE projid=%d' % (TABLENAME,rm)
    # print(cur.mogrify(sql))
    # exit()
    # cur.execute('DELETE FROM adaya WHERE projid=%d' % rm)
    cur.execute(sql)
    return rm, getRC(cur)

def dbDump(cur):
    cur.execute('SELECT * FROM adaya')
    printf('\n%s' % ''.join(map(cformat, FIELDS)))
    for data in cur.fetchall():
        # print(data)
        dat = []
        for field in FIELDS:
            dat.append(data[field])
        printf(''.join(map(tformat, dat)))

def main():
    db = setup()
    printf('*** Connect to %r database' % db)
    cxn = connect(db)
    if not cxn:
        printf('ERROR: %r not supported or unreachable, exit' % db)
        return
    cur = cxn.cursor()

    # printf('\n*** Create names into table')
    # create(cur)

    # printf('\n*** Insert names into table')
    # insert(cur, db)
    # dbDump(cur)
    # cxn.commit()

    # printf('\n*** Move users to a random group')
    # fr, to, num,l = update(cur)
    # printf('\t(%d %d users moved) from (%d) to (%d)' % (num, l, fr, to))
    # dbDump(cur)

    printf('\n*** Randomly delete group')
    rm, num = delete(cur)
    printf('\t(group #%d; %d users removed)' % (rm, num))
    dbDump(cur)

    printf('\n*** Drop users table')
    drop(cur)
    dbDump(cur)
    exit()

    printf('\n*** Close cxns')
    cur.close()
    cxn.commit()
    cxn.close()


if __name__ == '__main__':
    main()