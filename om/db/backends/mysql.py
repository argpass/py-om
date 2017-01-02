#!coding: utf-8
import time
import copy
import MySQLdb.converters
import MySQLdb
from om.db.base import Database, Driver, DriverSpec, DriverConnection, \
    ConnectionPool


class MySQLSpec(DriverSpec):

    def __init__(self, host, database, user=None, password=None,
                 connect_timeout=0,
                 time_zone="+0:00", charset="utf8", sql_mode="TRADITIONAL"):

        self.host = host
        self.database = database

        args = dict(conv=copy.copy(MySQLdb.converters.conversions),
                    use_unicode=True,
                    charset=charset,
                    db=database,
                    init_command=('SET time_zone = "%s"' % time_zone),
                    connect_timeout=connect_timeout,
                    sql_mode=sql_mode)

        if user is not None:
            args["user"] = user
        if password is not None:
            args["passwd"] = password

        # We accept a path to a MySQL socket file or a host(:port) string
        if "/" in host:
            args["unix_socket"] = host
        else:
            self.socket = None
            pair = host.split(":")
            if len(pair) == 2:
                args["host"] = pair[0]
                args["port"] = int(pair[1])
            else:
                args["host"] = host
                args["port"] = 3306

        self.db_args = args


class MySQLConnection(DriverConnection):
    def __init__(self, db):
        super(MySQLConnection, self).__init__(db)


class MySQLDriver(Driver):
    def open(self, ds):
        """
        Args:
            ds(MySQLSpec):
        """
        db = MySQLdb.connect(**ds.db_args)
        return MySQLConnection(db)


class MySQLDatabase(Database):
    OperationalError = MySQLdb.OperationalError

    def __init__(self, max_idle_time, pool):
        """
        Args:
            max_idle_time(float):
            pool(ConnectionPool):
        """
        super(MySQLDatabase, self).__init__(pool)

        self.max_idle_time = float(max_idle_time)

    @classmethod
    def format_column(cls, t_alias, col_name):
        return "`%s`.`%s`" % (t_alias, col_name)

    @classmethod
    def format_table_name(cls, t_name):
        return "`%s`" % (t_name,)

    def _allocate(self, blocking=0):
        return super(MySQLDatabase, self)._allocate(blocking)

    def _is_ok(self, con):
        """
        Args:
            con(DriverConnection):
        Returns:
            bool
        """
        return (not con.is_closed and
                time.time() - con.last_used_time <= self.max_idle_time)
