# -*- coding: utf-8 -*-

import datetime
import time
import unittest

from crud.conn import PyMysql


class CRUDTest(unittest.TestCase):
    table_name = "cluster_config"
    instance = None

    @classmethod
    def setUpClass(self):
        self.instance = PyMysql.setup()
        print(">>>> Here is the CRUDTest begin at: %s" % time.strftime("%Y-%m-%d %H:%M:%S"))
        print

    @classmethod
    def tearDownClass(self):
        self.instance.teardown()
        print("<<<< Here is the CRUDTest   end at: %s" % time.strftime("%Y-%m-%d %H:%M:%S"))
        print

    # C
    def test_create_table(self):
        create_sql = """
                CREATE TABLE `%s` (
                    `id` int(11) NOT NULL AUTO_INCREMENT,
                    `cluster_domain` varchar(255) NOT NULL COMMENT '对应组件主节点域名',
                    `cluster_name` varchar(128) NOT NULL COMMENT '集群名称',
                    `version` varchar(128) NOT NULL COMMENT '集群版本',
                    `created_by` varchar(50) NOT NULL DEFAULT '' COMMENT '创建人',
                    `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                    `updated_by` varchar(50) DEFAULT NULL COMMENT '修改人 ',
                    `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
                    `description` text NOT NULL COMMENT '备注信息',
                    PRIMARY KEY (`id`),
                    UNIQUE (`cluster_domain`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='存储集群配置信息';
        """ % self.table_name
        try:
            self.instance.send(create_sql)
        except Exception, e:
            print(repr(e))
            self.instance.rollback()

    def test_insert_doc(self):
        """
        https://stackoverflow.com/questions/3556305/how-to-retrieve-table-names-in-a-mysql-database-with-python-and-mysqldb
        """
        timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print('timestamp=%s' % timestamp)
        id = self.instance.insert(self.table_name,
                                  cluster_domain='192.168.0.3',
                                  cluster_name='es-fifa集群',
                                  version='v1',
                                  created_by='chenfh5',
                                  created_at=timestamp,
                                  updated_by='chenfh5',  # can be null
                                  updated_at=timestamp,  # can be null
                                  description='暂无')
        print(id)
        self.assertTrue(id > 0)

    # R, read
    def test_select(self):
        select_sql = """SELECT * FROM %s LIMIT 15""" % self.table_name
        self.instance.send(select_sql)
        res = self.instance.get()
        for x in res:
            print(x)
        self.assertIsNotNone(res)

    # U
    def test_update(self):
        id = self.instance.update(self.table_name,
                                  {'cluster_name': 'es-fifa集群', 'version': 'v2'},
                                  created_by='chenfh5111',
                                  description='暂无11')
        print(id)
        self.assertTrue(id >= 0)

    # D
    def test_delete_table(self):
        show_sql = """SHOW TABLES"""
        self.instance.send(show_sql)
        print(self.instance.get())
        time.sleep(1)

        select_sql = """DROP TABLE IF EXISTS %s""" % self.table_name
        self.instance.send(select_sql)
        time.sleep(1)

        self.instance.send(show_sql)
        res = self.instance.get()
        print(res)
        self.assertIsNotNone(res)
