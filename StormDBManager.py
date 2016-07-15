import logging

from storm.database import Connection
from storm.database import create_database
from storm.exceptions import OperationalError
from storm.twisted.transact import Transactor, transact
from twisted.internet import reactor
from twisted.internet.defer import DeferredLock, inlineCallbacks


class StormDBManager:
    """
    The StormDBManager is a manager that runs queries using the Storm Framework.
    These queries will be run on the Twisted thread-pool to ensure asynchronous, non-blocking behavior.
    In the future, this database manager will be the basis of an ORM based approach.
    """

    def __init__(self, db_path):
        """
        Sets up the database and all necessary elements for the database manager to function.
        """
        self._logger = logging.getLogger(self.__class__.__name__)

        self.db_path = db_path
        self._database = None
        self.connection = None
        self._version = 0

        # The transactor is required when you have methods decorated with the @transact decorator
        # This field name must NOT be changed.
        self.transactor = Transactor(reactor.getThreadPool())

        # Create a DeferredLock that should be used by callers to schedule their call.
        self.db_lock = DeferredLock()

    @inlineCallbacks
    def initialize(self):
        """
        Open/create the database and initialize the version.
        """
        self._database = create_database(self.db_path)
        self.connection = Connection(self._database)
        self._version = 0
        yield self._retrieve_version()

    @property
    def version(self):
        return self._version

    @inlineCallbacks
    def _retrieve_version(self):
        """
        Attempts to retrieve the current database version from the MyInfo table.
        If it fails, the _version field remains at 0 as defined in the init function.
        """
        try:
            version_str, = yield self.fetchone(u"SELECT value FROM MyInfo WHERE entry == 'version'")
            self._version = int(version_str)
            self._logger.info(u"Current database version is %s", self._version)
        except (TypeError, OperationalError):
            self._logger.warning(u"Failed to load database version, setting the DB version to 0.")
            self._version = 0

    def schedule_query(self, callable, *args, **kwargs):
        """
        Utility function to schedule a query to be executed using the db_lock.

        Args:
            callable: The database function that is to be executed.
            *args: Any additional arguments that will be passed as the callable's arguments.
            **kwargs: Keyword arguments that are passed to the callable function.

        Returns: A deferred that fires with the result of the query.

        """
        return self.db_lock.run(callable, *args, **kwargs)

    def execute(self, query, arguments=None, get_lastrowid=False):
        """
        Executes a query on the twisted thread pool using the Storm framework.

        Args:
            query: The sql query to be executed.
            arguments: Optional arguments that go with the sql query.
            get_lastrowid: If true, this function will return the last inserted row id, otherwise None.

        Returns: A deferred that fires once the execution is done, the result will be None if get_lastrowid is False
        else it returns with the last inserted row id.

        """

        # @transact
        def _execute(self, query, arguments=None, get_lastrowid=False):
            connection = Connection(self._database)
            ret = None
            if get_lastrowid:
                result = connection.execute(query, arguments, noresult=False)
                ret = result._raw_cursor.lastrowid
            else:
                connection.execute(query, arguments, noresult=True)
            connection.close()
            return ret

        return self.db_lock.run(_execute, self, query, arguments, get_lastrowid)

    def executemany(self, query, list):
        """
        Executes a query on the twisted thread pool using the Storm framework many times using the values provided by
        a list.

        Args:
            query: The sql query to be executed.
            list: The list containing tuples of values to execute the query with.

        Returns: A deferred that fires once the execution is done, the result will be None.

        """
        def _execute(connection, query, arguments=None):
            connection.execute(query, arguments, noresult=True)

        # @transact
        def _executemany(self, query, list):
            connection = Connection(self._database)
            for item in list:
                _execute(connection, query, item)
            connection.close()
        return self.db_lock.run(_executemany, self, query, list)

    def executescript(self, sql_statements):
        """
        Executes a script of several sql queries sequentially.
        Note that this function does exist in SQLite, but not in the Storm framework:
        https://www.mail-archive.com/storm@lists.canonical.com/msg00569.html

        Args:
            sql_statements: A list of sql statements to be executed.

        Returns: A deferred that fires with None once all statements have been executed.

        """
        def _execute(connection, query):
            connection.execute(query, noresult=True)

        # @transact
        def _executescript(self, sql_statements):
            connection = Connection(self._database)
            for sql_statement in sql_statements:
                _execute(connection, sql_statement)
            connection.close()
        return self.db_lock.run(_executescript, self, sql_statements)

    def fetchone(self, query, arguments=None):
        """
        Executes a query on the twisted thread pool using the Storm framework and returns the first result.
        The optional arguments should be provided when running a parametrized query. It has to be an iterable data
        structure (tuple, list, etc.).

        Args:
            query: The sql query to be executed.
            arguments: Optional arguments that go with the sql query.

        Returns: A deferred that fires with the first tuple that matches the query or None.
        The result would be the same as using execute and calling the next() function on it, instead now you will get
        None instead of a StopIterationException.

        """
        # @transact
        def _fetchone(self, query, arguments=None):
            connection = Connection(self._database)
            result = connection.execute(query, arguments).get_one()
            connection.close()
            return result

        return self.db_lock.run(_fetchone, self, query, arguments)

    def fetchall(self, query, arguments=None):
        """
        Executes a query on the twisted thread pool using the Storm framework and returns a list of tuples containing
        all matches through a deferred.

        Args:
            query: The sql query to be executed.
            arguments: Optional arguments that go with the sql query.

        Returns: A deferred that fires with a list of tuple results that matches the query, possibly empty.

        """
        # @transact
        def _fetchall(self, query, arguments=None):
            connection = Connection(self._database)
            res = connection.execute(query, arguments).get_all()
            connection.close()
            return res

        return self.db_lock.run(_fetchall, self, query, arguments)

    def insert(self, table_name, **kwargs):
        """
        Inserts data provided as keyword arguments into the table provided as an argument.

        Args:
            table_name: The name of the table the data has to be inserted into.
            **kwargs: A dictionary where the key represents the column and the value the value to be inserted.

        Returns: A deferred that fires with None when the data has been inserted.

        """
        # @transact
        def _insert(self, table_name, **kwargs):
            connection = Connection(self._database)
            self._insert(connection, table_name, **kwargs)
            connection.close()
        return self.db_lock.run(_insert, self, table_name, **kwargs)

    def _insert(self, connection, table_name, **kwargs):
        """
        Utility function to insert data which is not decorated by the @transact to prevent a loop calling this function
        to create many threads.
        Do NOT call this function on the main thread, it will be blocking on that thread.

        Args:
            connection: The database connection object.
            table_name: The name of the table the data has to be inserted into.
            **kwargs: A dictionary where the key represents the column and the corresponding value the value to be
            inserted.

        Returns: A deferred that fires with None when the data has been inserted.

        """
        if len(kwargs) == 0:
            raise ValueError("No keyword arguments supplied.")
        if len(kwargs) == 1:
            sql = u'INSERT INTO %s (%s) VALUES (?);' % (table_name, kwargs.keys()[0])
        else:
            questions = ','.join(('?',)*len(kwargs))
            sql = u'INSERT INTO %s %s VALUES (%s);' % (table_name, tuple(kwargs.keys()), questions)

        connection.execute(sql, kwargs.values(), noresult=True)

    def insert_many(self, table_name, arg_list):
        """
        Inserts many items into a table.

        Args:
            table_name: The table name that you want to insert to.
            arg_list: A list containing dictionaries where the key is the column name and the corresponding value the
            value to be inserted into this column.

        Returns: A deferred that fires with None once the bulk insertion is done.

        """
        # @transact
        def _insert_many(self, table_name, arg_list):
            if len(arg_list) == 0:
                return
            connection = Connection(self._database)
            for args in arg_list:
                self._insert(connection, table_name, **args)
            connection.close()

        return self.db_lock.run(_insert_many, self, table_name, arg_list)

    def delete(self, table_name, **kwargs):
        """
        Utility function to delete from a table.

        Args:
            table_name: The table name to delete data from.
            **kwargs: A dictionary containing key values.
            The key is the column to target and the value can be a tuple or single element.
            In case the value is a tuple, it can specify the operator.
            In case the value is a single element, the equals "=" operator is used.

        Returns: A deferred that fires with None once the deletion has been performed.

        """
        sql = u'DELETE FROM %s WHERE ' % table_name
        arg = []
        for k, v in kwargs.iteritems():
            if isinstance(v, tuple):
                sql += u'%s %s ? AND ' % (k, v[0])
                arg.append(v[1])
            else:
                sql += u'%s=? AND ' % k
                arg.append(v)
        sql = sql[:-5] # Remove the last AND
        return self.execute(sql, arg)

    def count(self, table_name):
        """
        Utility function to get the number of rows of a table.

        Args:
            table_name: The table name.

        Returns: A deferred that fires with the number of rows in the table.

        """
        sql = u"SELECT count(*) FROM %s LIMIT 1" % table_name
        return self.fetchone(sql)
