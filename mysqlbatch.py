# coding:utf-8

'''
Created on 2013-8-16
MySQL批量执行工具
@author: samchi
'''
from optparse import OptionParser
import MySQLdb as db
import atexit
import yaml


class _ServerConnection(object):
    def __init__(self, name, connection, db, shardbegin, shardend):
        self.name = name
        self.connection, self.db = connection, db
        self.shardbegin, self.shardend = shardbegin, shardend
    
    def csr(self):
        return self.connection.cursor()
        
all_connections = []

def _load_server_config(file_name):
    global all_connections
    server_config = yaml.load(open(file_name, 'r'))
    for server_name in server_config['servers']:
        config = server_config['servers'][server_name]
        connection = db.connect(host=config['host'], port=config['port'],
             user=config['user'], passwd=config['passwd'])
        all_connections.append(_ServerConnection(server_name, connection, config['db'],
             config['shardbegin'], config['shardend']))
        
def _batch_create_databases():
    global all_connections
    for conn in all_connections:
        csr = conn.csr()
        csr.execute('CREATE DATABASE IF NOT EXISTS `%s`;' % conn.db)

def _batch_execute(sql):
    global all_connections
    for conn in all_connections:
        for shard in range(conn.shardbegin, conn.shardend + 1):
            sql_statement = ("use `%s`;" + sql) % (conn.db, shard)
            print 'execute:%s' % sql_statement
            conn.csr().execute(sql_statement)
            conn.connection.commit()

def _batch_execute_with_target_db(db_name_lst, sql):
    global all_connections
    for conn in all_connections:
        if conn.name in db_name_lst:
            for shard in range(conn.shardbegin, conn.shardend + 1):
                sql_statement = ("use `%s`;" + sql) % (conn.db, shard)
                print 'execute:%s' % sql_statement
                conn.csr().execute(sql_statement)
                conn.connection.commit()
                
class _TableInfo(object):
    
    def __init__(self, tablename, rowcount):
        self.tablename, self.rowcount = tablename, rowcount

def _ana(table):
    global all_connections
    table_info_lst = []
    sum_row, table_num = 0, 0
    for conn in all_connections:
        for shard in range(conn.shardbegin, conn.shardend + 1):
            sql = ('show table status like "' + table + '";') % shard
            connection = conn.connection
            connection.select_db(conn.db)
            csr = connection.cursor()
            csr.execute(sql)
            row = int(csr.fetchone()[4])
            table_info_lst.append(_TableInfo(table % shard, row))
            sum_row += row
            table_num += 1
    return table_info_lst, sum_row / table_num

def _close_all():
    global all_connections
    for conn in all_connections:
        conn.connection.close()

atexit.register(_close_all)

if __name__ == '__main__':
    parser = OptionParser(usage="usage:%mysqlbatch [optinos]")
    
    parser.add_option("--server", default=None, help=u"MySQL Server Config file")
    parser.add_option("--createdb", action='store_true', help=u"Create Databases")
    parser.add_option("--db", default=None, help=u"Target database, split with ,")
    parser.add_option("--execute", default=None, action='store', help=u"Execute SQLs")
    parser.add_option("--ana", default=None, 
            help=u"Analyze the usage of the sharding tables")
    
    (options, args) = parser.parse_args()
    
    def exit_with_info(info):
        print info
        exit(0)
    
    if not options.server:
        exit_with_info("You must point a mysql server config file")
        
    _load_server_config(options.server)
    
    if options.ana:
        print 'Analyzing...'
        table_info_lst, avg_row = _ana(options.ana)
        print 'Avg rows:', avg_row
        print '--------- max records ---------'
        count = 0
        for table in sorted(table_info_lst, key=lambda table:table.rowcount, reverse=True):
            print '%s : %d' % (table.tablename, table.rowcount)
            if count == 9:break
            count += 1
        print '--------- min records ---------'
        count = 0
        for table in sorted(table_info_lst, key=lambda table:table.rowcount, reverse=False):
            print '%s : %d' % (table.tablename, table.rowcount)
            if count == 9:break
            count += 1
    
    if options.createdb:
        _batch_create_databases()
        
    def get_sql(execute):
        if execute.endswith(".sql"):
            return open(execute, 'r').read()
        return execute
    
    if options.db:
        if not options.execute:
            exit_with_info("I need sql to execute!")
            
        sql = get_sql(options.execute)
        db_name_lst = [name.strip() for name in options.db.split(',')]
        print "DB NAME LST", db_name_lst
        _batch_execute_with_target_db(db_name_lst, sql)
        exit(0)
    
    if options.execute:
        _batch_execute(get_sql(options.execute))