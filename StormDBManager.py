import logging
import os
import tempfile
from Queue import Queue

from storm.database import create_database
from storm.exceptions import OperationalError
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue, Deferred
from twisted.internet.threads import deferToThread

import thread

from util import find_caller


class StormDBManager(object):
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
        self._cursor = None
        self._version = 0
        self._pending_commits = 0
        self._commit_callbacks = []
        self._queue = Queue()

        # The transactor is required when you have methods decorated with the @transact decorator
        # This field name must NOT be changed.
        # self.transactor = Transactor(reactor.getThreadPool())

    @inlineCallbacks
    def open(self):
        """
        Open/create the database and initialize the version.
        """

        # Start the worker and add an errback to it.
        self.worker = deferToThread(self.start_worker)

        def on_worker_failure(failure):
            print failure
            failure.raiseException()
        self.worker.addErrback(on_worker_failure)

        self._version = 0
        yield self._retrieve_version()
        returnValue(True)

    def close(self, commit=True):
        """
        Closes the worker and returns the worker deferred. Once this fires, the worker has cleaned up.
        :param commit: A boolean indicating if we should commit on close.
        :return:
        """
        if commit:
            self.commit(exiting=True)

        self._queue.put(None)
        self._logger.debug("close database [%s]", self.db_path)
        return self.worker

    def delete_tmp_database(self, _):
        """
        Deletes the temporary database if it was used.
        """

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
        Utility function to schedule a query to be executed using the queue.

        Args:
            callable: The database function that is to be executed.
            *args: Any additional arguments that will be passed as the callable's arguments.
            **kwargs: Keyword arguments that are passed to the callable function.

        Returns: A deferred that fires with the result of the query.

        """
        deferred = Deferred()
        self._queue.put((callable, args, kwargs, deferred))
        return deferred

    def execute(self, *args, **kwargs):
        """
        Executes a query on the twisted thread pool using the Storm framework.

        Args:
            query: The sql query to be executed.
            arguments: Optional arguments that go with the sql query.
            get_lastrowid: If true, this function will return the last inserted row id, otherwise None.

        Returns: A deferred that fires once the execution is done, the result will be None if get_lastrowid is False
        else it returns with the last inserted row id.

        """

        caller = find_caller()
        print "CALLER: ", caller

        def _execute(self, query, arguments=(), get_lastrowid=False):
            ret = None
            if get_lastrowid:
                self._cursor.execute(query, arguments)
                ret = self._cursor.lastrowid
            else:
                self._cursor.execute(query, arguments)
            return ret

        deferred = Deferred()
        self._queue.put((_execute, args, kwargs, deferred))
        return deferred

    def executemany(self, *args, **kwargs):
        """
        Executes a query on the twisted thread pool using the Storm framework many times using the values provided by
        a list.

        Args:
            query: The sql query to be executed.
            list: The list containing tuples of values to execute the query with.

        Returns: A deferred that fires once the execution is done, the result will be None.

        """

        caller = find_caller()
        print "CALLER: ", caller

        def _executemany(self, query, list):
            self._cursor.executemany(query, list)

        deferred = Deferred()
        self._queue.put((_executemany, args, kwargs, deferred))
        return deferred

    def executescript(self, *args, **kwargs):
        """
        Executes a script of several sql queries sequentially.
        Note that this function does exist in SQLite, but not in the Storm framework:
        https://www.mail-archive.com/storm@lists.canonical.com/msg00569.html

        Args:
            sql_statements: A list of sql statements to be executed.

        Returns: A deferred that fires with None once all statements have been executed.

        """

        caller = find_caller()
        print "CALLER: ", caller

        def _executescript(self, sql_statements):
            for sql_statement in sql_statements:
                self.execute(sql_statement)

        deferred = Deferred()
        self._queue.put((_executescript, args, kwargs, deferred))
        return deferred

    def fetchone(self, *args, **kwargs):
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

        caller = find_caller()
        print "CALLER: ", caller

        def _fetchone(self, query, arguments=()):
            try:
                return self._cursor.execute(query, arguments).next()
            except (StopIteration, OperationalError):
                return None

        deferred = Deferred()
        self._queue.put((_fetchone, args, kwargs, deferred))
        return deferred

    def fetchall(self, *args, **kwargs):
        """
        Executes a query on the twisted thread pool using the Storm framework and returns a list of tuples containing
        all matches through a deferred.

        Args:
            query: The sql query to be executed.
            arguments: Optional arguments that go with the sql query.

        Returns: A deferred that fires with a list of tuple results that matches the query, possibly empty.

        """

        caller = find_caller()
        print "CALLER: ", caller

        def _fetchall(self, query, arguments=()):
            return self._cursor.execute(query, arguments).fetchall()

        deferred = Deferred()
        self._queue.put((_fetchall, args, kwargs, deferred))
        return deferred

    def insert(self, *args, **kwargs):
        """
        Inserts data provided as keyword arguments into the table provided as an argument.

        Args:
            table_name: The name of the table the data has to be inserted into.
            **kwargs: A dictionary where the key represents the column and the value the value to be inserted.

        Returns: A deferred that fires with None when the data has been inserted.

        """

        caller = find_caller()
        print "CALLER: ", caller

        def _insert(self, table_name, **kwargs):
            self._insert(table_name, **kwargs)

        deferred = Deferred()
        self._queue.put((_insert, args, kwargs, deferred))
        return deferred

    def _insert(self, table_name, **kwargs):
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

        caller = find_caller()
        print "CALLER: ", caller

        if len(kwargs) == 0:
            raise ValueError("No keyword arguments supplied.")
        if len(kwargs) == 1:
            sql = u'INSERT INTO %s (%s) VALUES (?);' % (table_name, kwargs.keys()[0])
        else:
            questions = ','.join(('?',)*len(kwargs))
            sql = u'INSERT INTO %s %s VALUES (%s);' % (table_name, tuple(kwargs.keys()), questions)

        self._cursor.execute(sql, kwargs.values())

    def insert_many(self, *args, **kwargs):
        """
        Inserts many items into a table.

        Args:
            table_name: The table name that you want to insert to.
            arg_list: A list containing dictionaries where the key is the column name and the corresponding value the
            value to be inserted into this column.

        Returns: A deferred that fires with None once the bulk insertion is done.

        """

        caller = find_caller()
        print "CALLER: ", caller


        def _insert_many(self, table_name, arg_list):
            if len(arg_list) == 0:
                return

            for args in arg_list:
                self._insert(table_name, **args)

        deferred = Deferred()
        self._queue.put((_insert_many, args, kwargs, deferred))
        return deferred

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

        caller = find_caller()
        print "CALLER: ", caller

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

        caller = find_caller()
        print "CALLER: ", caller

        sql = u"SELECT count(*) FROM %s LIMIT 1" % table_name
        return self.fetchone(sql)

    def commit(self, exiting=False):

        caller = find_caller()
        print "CALLER: ", caller

        """Schedules the call to commit the current transaction"""
        deferred = Deferred()
        self._queue.put(exiting)
        return deferred

    def _commit(self, exiting=False):
        """
        Commits the current transaction.
        :param exiting: A boolean indicating if Dispersy is exiting.
        :return: False if there are pending commits else the result of the commit.
        """
        if self._pending_commits:
            self._logger.debug("defer commit [%s]", self.db_path)
            self._pending_commits += 1
            return False

        else:
            self._logger.debug("commit [%s]", self.db_path)
            for callback in self._commit_callbacks:
                try:
                    callback(exiting=exiting)
                except Exception as exception:
                    self._logger.exception("%s [%s]", exception, self.db_path)

            return self.connection.commit()

    def __enter__(self):
        """
        Enters a no-commit state.  The commit will be performed by __exit__.

        @return: The method self.execute
        """
        self._logger.debug("disabling commit [%s]", self.db_path)
        self._pending_commits = max(1, self._pending_commits)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Leaves a no-commit state.  A commit will be performed if Database.commit() was called while
        in the no-commit state.
        """

        self._pending_commits, pending_commits = 0, self._pending_commits

        if exc_type is None:
            self._logger.debug("enabling commit [%s]", self.db_path)
            if pending_commits > 1:
                self._logger.debug("performing %d pending commits [%s]", pending_commits - 1, self.db_path)
                self.commit()
            return True
        elif isinstance(exc_value, IgnoreCommits):
            self._logger.debug("enabling commit without committing now [%s]", self.db_path)
            return True
        else:
            # Niels 23-01-2013, an exception happened from within the with database block
            # returning False to let Python reraise the exception.
            return False

    def attach_commit_callback(self, func):
        assert not func in self._commit_callbacks
        self._commit_callbacks.append(func)

    def detach_commit_callback(self, func):
        assert func in self._commit_callbacks
        self._commit_callbacks.remove(func)

    def start_worker(self):
        # Storm has no :memory: database argument, it has to be sqlite:
        if self.db_path == ":memory:":
            self._database = create_database("sqlite:")
        else:
            self._database = create_database("sqlite:%s" % self.db_path)
        self.connection = self._database.raw_connect()
        self._cursor = self.connection.cursor()
        while True:
            item = self._queue.get()
            if isinstance(item, bool):
                self._commit(exiting=item)
            elif item is None:
                break
            elif isinstance(item, tuple):
                func, args, kwargs, deferred = item
                res = func(self, *args, **kwargs)
                # Make sure the reactor thread calls the callback, else chained functions will run on THIS thread.
                reactor.callFromThread(deferred.callback, res)
            else:
                self._logger.error("Worker does not support %r" % item)

        # We are closing the loop has been terminated
        self._queue.empty()
        self._queue = None
        self._cursor.close()
        self._cursor = None
        self.connection.close()
        self.connection = None
        return True


class IgnoreCommits(Exception):

    """
    Ignore all commits made within the body of a 'with database:' clause.

    with database:
       # all commit statements are delayed until the database.__exit__
       database.commit()
       database.commit()
       # raising IgnoreCommits causes all commits to be ignored
       raise IgnoreCommits()
    """
    def __init__(self):
        super(IgnoreCommits, self).__init__("Ignore all commits made within __enter__ and __exit__")
