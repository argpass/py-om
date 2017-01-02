#!coding: utf-8
import time
import heapq
from threading import RLock, Event


class DriverSpec(object):
    """"""


class Driver(object):
    def open(self, ds):
        """
        Args:
            ds(DriverSpec):
        """
        raise NotImplementedError(u"Implement the method by subclass")


class DriverConnection(object):
    def __init__(self, db):
        self._db = db
        self.create_at = time.time()
        self.last_used_time = time.time()
        self._closed = False

    def cursor(self):
        self.last_used_time = time.time()
        return self._db.cursor()

    @property
    def is_closed(self):
        return self._closed

    def close(self):
        if self._closed:
            return
        self._closed = True
        return self._db.close()

    def commit(self):
        return self._db.commit()

    def rollback(self):
        return self._db.rollback()

    def begin(self):
        return self._db.autocommit(False)


class ConnectionPool(object):
    _FREE_TOKEN = "__freed__"

    def __init__(self, spec, driver, maximum=1):
        """
        Args:
            driver(Driver):
            spec(DriverSpec)
        """
        self._driver = driver
        self._driver_spec = spec
        self._lock = RLock()
        # [(timestamp, connection), ...]
        self._connections = []
        self._maximum = maximum
        self._cnt = 0
        self._no_exhausted = Event()

    @property
    def cached_size(self):
        """How many free connections in the pool
        """
        return len(self._connections)

    @property
    def allocated_cnt(self):
        """How many free connections allocated by the pool"""
        return self._cnt

    def _new_connection(self):
        self._lock.acquire()
        try:
            con = self._driver.open(self._driver_spec)
            self._cnt += 1
            return con
        finally:
            self._lock.release()

    def allocate(self, blocking=0):
        """Get or create a connection from the pool,the oldest one first
        Returns:
            DriverConnection
        """
        con = None
        while True:
            try:
                if con:
                    return con
                _, con = heapq.heappop(self._connections)
                return con
            except IndexError:
                # pool is empty, acquire the lock to try to new a connection
                self._lock.acquire()
                try:
                    if self._maximum and self._cnt <= self._maximum:
                        # no available connections and create a new one
                        con = self._new_connection()
                finally:
                    self._lock.release()
                if not con:
                    # exceeded maximum, check if need to wait
                    if blocking is not None and blocking <= 0:
                        # no blocking, raise error
                        return self._handle_exhausted()
                    # wait
                    ok = self._no_exhausted.wait(blocking)
                    if not ok:
                        # wait timeout, raise error
                        return self._handle_exhausted()
                    else:
                        # flag refreshed, try to allocate again
                        continue
                else:
                    # goe a connection, return it
                    return con
            finally:
                if con:
                    self._mark_un_free(con)

    def _handle_exhausted(self):
        raise RuntimeError(u"Exceeded maximum(%s) connections",
                           self._maximum)

    def close(self, con):
        """Teardown a connection
        Args:
            con(DriverConnection):
        """
        con.close()
        self._cnt -= 1

    def _mark_free(self, con):
        setattr(con, self._FREE_TOKEN, True)

    def _mark_un_free(self, con):
        setattr(con, self._FREE_TOKEN, False)

    def _is_freed(self, con):
        return getattr(con, self._FREE_TOKEN, None) is True

    def free(self, con, err):
        """Return a connection back to the pool"""
        # avoid to free con many times
        if self._is_freed(con):
            return
        self._mark_free(con)
        self._lock.acquire()
        try:
            if isinstance(err, OperationalError):
                # bad connection
                # never to allocate it, teardown it
                self.close(con)
                return
            ts = time.time()
            heapq.heappush(self._connections, (ts, con))
            # clear exhausted flag
            # notify blocked callers to ask for the pool
        finally:
            self._lock.release()
            self._no_exhausted.set()


class OperationalError(Exception):
    def __init__(self, raw_error):
        self._raw_error = raw_error


class MultipleRowsError(Exception):
    """"""


class Row(dict):
    """A dict that allows for object-like property access syntax."""
    def __init__(self, col_names, data_tuple):
        super(Row, self).__init__(zip(col_names, data_tuple))
        self.__col_names__ = col_names
        self.__data__ = data_tuple

    def get_col_names(self):
        """
        Returns:
            tuple
        """
        return self.__col_names__

    def get_data_tuple(self):
        """
        Returns:
            tuple
        """
        return self.__data__

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)


class Database(object):
    """Database wrapper, all operation is on a connection cursor
    all connections managed in a connection pool
    """
    OperationalError = None

    def __init__(self, pool):
        """
        Args:
            pool(ConnectionPool):
        """
        self._pool = pool

    @classmethod
    def format_column(cls, t_alias, col_name):
        return "%s.%s" % (t_alias, col_name)

    @classmethod
    def format_table_name(cls, t_name):
        return "%s" % (t_name,)

    @classmethod
    def quote(cls, name):
        return "`%s`" % name

    def _allocate(self, blocking=0):
        """Allocate a connection
        Returns:
            DriverConnection
        """
        con = None
        while con is None:
            _con = self._pool.allocate(blocking)
            if self._is_ok(_con):
                con = _con
        return con

    def _is_ok(self, con):
        """
        Args:
            con(DriverConnection):
        Returns:
            bool
        """
        return True

    def _free(self, con, err):
        """Give a connection back to the db connection pool
        Args:
            con(DriverConnection):
            err:
        """
        self._pool.free(con, err)

    def iter(self, query, *parameters, **kwparameters):
        """Returns an iterator for the given query and parameters."""
        con = self._allocate()
        cursor = con.cursor()
        err = None
        try:
            self._execute(cursor, query, parameters, kwparameters)
            column_names = [d[0] for d in cursor.description]
            for row in cursor:
                yield Row(column_names, row)
        except Exception as e:
            err = e
            raise
        finally:
            cursor.close()
            self._free(con, err)

    def query(self, query, *parameters, **kwparameters):
        return list(self.iter(query, *parameters, **kwparameters))

    def get(self, query, *parameters, **kwparameters):
        """Returns the (singular) row returned by the given query.

        If the query has no results, returns None.  If it has
        more than one result, raises an exception.
        """
        rows = list(self.iter(query, *parameters, **kwparameters))
        if not rows:
            return None
        elif len(rows) > 1:
            raise MultipleRowsError(u"Multiple rows returned "
                                    u"for Database.get() query")
        else:
            return rows[0]

    def execute_lastrowid(self, query, *parameters, **kwparameters):
        """Executes the given query, returning the lastrowid from the query."""
        con = self._allocate()
        cursor = con.cursor()
        err = None
        try:
            self._execute(cursor, query, parameters, kwparameters)
            return cursor.lastrowid
        except Exception as e:
            err = e
            raise
        finally:
            cursor.close()
            self._free(con, err)

    def execute_rowcount(self, query, *parameters, **kwparameters):
        """Executes the given query, returning the rowcount from the query."""
        con = self._allocate()
        cursor = con.cursor()
        err = None
        try:
            self._execute(cursor, query, parameters, kwparameters)
            return cursor.rowcount
        except Exception as e:
            err = e
            raise
        finally:
            cursor.close()
            self._free(con, err)

    def executemany_lastrowid(self, query, parameters):
        """Executes the given query against all the given param sequences.

        We return the lastrowid from the query.
        """
        print "330:sql:%s, --args:%s" % (query, repr(parameters))
        con = self._allocate()
        cursor = con.cursor()
        err = None
        try:
            cursor.executemany(query, parameters)
            return cursor.lastrowid
        except Exception as e:
            err = e
            raise
        finally:
            cursor.close()
            self._free(con, err)

    def executemany_rowcount(self, query, parameters):
        """Executes the given query against all the given param sequences.

        We return the rowcount from the query.
        """
        con = self._allocate()
        cursor = con.cursor()
        err = None
        try:
            cursor.executemany(query, parameters)
            return cursor.rowcount
        except Exception as e:
            err = e
            raise
        finally:
            cursor.close()
            self._free(con, err)

    update = execute_rowcount
    updatemany = executemany_rowcount

    insert = execute_lastrowid
    insertmany = executemany_lastrowid

    def _execute(self, cursor, query, parameters, kwparameters):
        print "368:%s, --args%s" % (query, repr(parameters))
        try:
            return cursor.execute(query, kwparameters or parameters)
        except self.OperationalError as e:
            # self.OperationalError is driver specific, so i
            # wrap self.OperationalError as `OperationalError`,
            # then all OperationalError is the same
            raise OperationalError(e)

    def transaction(self):
        """
        Returns:
            Transaction
        """
        con = self._allocate()
        return Transaction(con, self._pool)


class Transaction(Database):
    def __init__(self, con, pool, is_inner_transaction=False):
        """
        Args:
            con(DriverConnection):
            pool(ConnectionPool):
        """
        super(Transaction, self).__init__(pool)
        self._con = con
        self._err = None
        self._is_inner_transaction = is_inner_transaction
        self._closed = False

    def transaction(self):
        con = self._con
        return Transaction(con, self._pool, is_inner_transaction=True)

    def _allocate(self, blocking=0):
        return self._con

    def _free(self, con, err):
        if err:
            self._err = err
            self.close()

    def commit(self):
        if self._is_inner_transaction:
            return False
        self._con.commit()
        return True

    def rollback(self):
        return self._con.rollback()

    def close(self):
        if self._closed:
            return
        self._pool.free(self._con, self._err)
        self._closed = True

