# -*- coding: utf-8 -*-

import MySQLdb


class PyMysql:

    def __init__(self, host, port, user, pw_encode, db):
        import base64
        self.conn = MySQLdb.connect(host=host,
                                    port=port,
                                    user=user,
                                    passwd=base64.b64decode(pw_encode),
                                    db=db,
                                    use_unicode=1,
                                    charset='utf8')
        self.cursor = self.conn.cursor()
        self.conn.autocommit(True)

    def send(self, cmd):
        print("cmd=%s" % cmd)
        self.cursor.execute(cmd)

    def teardown(self):
        self.conn.commit()
        self.cursor.close()
        self.conn.close()

    def get(self):
        return self.cursor.fetchall()

    def rollback(self):
        self.conn.rollback()

    def insert(sef, table, **kwargs):
        sql = 'INSERT INTO {} ({}) VALUES ({})'.format(table, ', '.join(kwargs.keys()), ', '.join(['%({})s'.format(k) for k in kwargs.keys()]))
        print(sql)
        # exit(1)
        sef.cursor.execute(sql, kwargs)
        return sef.cursor.lastrowid

    def update(sef, table, where_condition_dict, **kwargs):
        def mk(x):
            return x + "=%(" + x + ")s"

        sql = 'UPDATE {} SET {} WHERE {}'.format(table, ", ".join(map(mk, kwargs.keys())), " and ".join(map(mk, where_condition_dict.keys())))
        print(sql)
        kwargs.update(where_condition_dict)  # merge into kwargs
        sef.cursor.execute(sql, kwargs)
        return sef.cursor.lastrowid

    @staticmethod
    def setup():
        from crud import settings
        host = settings.host
        port = settings.port
        user = settings.user
        passwd = settings.passwd
        db = settings.db
        instance = PyMysql(host, port, user, passwd, db)
        return instance
