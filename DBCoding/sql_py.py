import sqlite3
import pymysql.cursors
import pymysql

# Connect to the database
connection = pymysql.connect(host='localhost',
                             user='root',
                             password='ada',
                             db='menagerie',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

try:
    with connection.cursor() as cursor:
    #     # Create a new record
        sql = "INSERT INTO `users` (`email`, `password`) VALUES (%s, %s)"
        num = cursor.execute(sql, ('webmaster@python.org', 'very-secret'))
        # return number of affected rows
        # print(num)
    # connection is not autocommit by default. So you must commit to save
    # your changes.
    connection.commit()

    with connection.cursor() as cursor:# 保存在缓存中，其他场景使用SSCursor
        # Read a single record
        sql = "SELECT * FROM `users` WHERE `email`=%s"#`id`, `password`
        num = cursor.execute(sql, ('webmaster@python.org',))
        # print(num)
        # fetch many step by step
        # result = cursor.fetchmany(1)
        # print(result)
        # result = cursor.fetchmany(1)
        # print(result)

        # fetch one by one
        # result = cursor.fetchone()
        # # print(cursor._rows)#匹配到的所有数据按照行排列的全部
        # print(result)
        # result = cursor.fetchone()
        # # print(cursor._rows[1:2])#选择任意行
        # print(result)

        # fetch all
        result = cursor.fetchall()
        print(result)
finally:
    connection.close()
