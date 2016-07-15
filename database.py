"""
This module provides basic database functionalty and simple version control.

@author: Boudewijn Schoon
@organization: Technical University Delft
@contact: dispersy@frayja.com
"""
import logging
import os
import sys
import tempfile
import thread
from abc import ABCMeta, abstractmethod

from twisted.internet.defer import inlineCallbacks, returnValue

from StormDBManager import StormDBManager
from .util import attach_runtime_statistics


if "--explain-query-plan" in getattr(sys, "argv", []):
    _explain_query_plan_logger = logging.getLogger("explain-query-plan")
    _explain_query_plan = set()


    def attach_explain_query_plan(func):
        @inlineCallbacks
        def attach_explain_query_plan_helper(self, statements, bindings=()):
            if not statements in _explain_query_plan:
                _explain_query_plan.add(statements)

                _explain_query_plan_logger.info("Explain query plan for <<<%s>>>", statements)
                query_plan = yield self.stormdb.fetchall(u"EXPLAIN QUERY PLAN %s" % statements, bindings)
                for line in query_plan:
                    _explain_query_plan_logger.info(line)
                _explain_query_plan_logger.info("--")

            return_value = yield func(self, statements, bindings)
            returnValue(return_value)
        attach_explain_query_plan_helper.__name__ = func.__name__
        return attach_explain_query_plan_helper

else:
    def attach_explain_query_plan(func):
        return func


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


class Database(object):

    __metaclass__ = ABCMeta

    def __init__(self, file_path):
        """
        Initialize a new Database instance.

        @param file_path: the path to the database file.
        @type file_path: unicode
        """
        assert isinstance(file_path, unicode)

        super(Database, self).__init__()
        self._logger = logging.getLogger(self.__class__.__name__)

        self._logger.debug("loading database [%s]", file_path)
        self._file_path = file_path

        # _CONNECTION, _CURSOR, AND _DATABASE_VERSION are set during open(...)
        self._connection = None
        self._cursor = None
        self._database_version = 0
        self.temp_db_path = None
        # Storm does not know :memory: and it doesn't work when doing things multi-threaded. So generate a tmp database
        # Note that this database is a file database.
        if self._file_path == ":memory:":
            temp_dir = tempfile.mkdtemp(prefix="dispersy_tmp_db_")
            self.temp_db_path = os.path.join(temp_dir, u"test.db")
            self.stormdb = StormDBManager("sqlite:%s" % self.temp_db_path)
        else:
            self.stormdb = StormDBManager("sqlite:%s" % self._file_path)

        # _commit_callbacks contains a list with functions that are called on each database commit
        self._commit_callbacks = []

        # Database.commit() is enabled when _pending_commits == 0.  Database.commit() is disabled
        # when _pending_commits > 0.  A commit is required when _pending_commits > 1.
        self._pending_commits = 0

        if __debug__:
            self._debug_thread_ident = 0

    @inlineCallbacks
    def open(self, initial_statements=True, prepare_visioning=True):
        assert self._cursor is None, "Database.open() has already been called"
        assert self._connection is None, "Database.open() has already been called"
        if __debug__:
            self._debug_thread_ident = thread.get_ident()
        self._logger.debug("open database [%s]", self._file_path)
        yield self.stormdb.initialize()
        self._connect()
        if initial_statements:
            yield self._initial_statements()
        if prepare_visioning:
            yield self._prepare_version()
        returnValue(True)

    def close(self, commit=True):
        assert self._cursor is not None, "Database.close() has been called or Database.open() has not been called"
        assert self._connection is not None, "Database.close() has been called or Database.open() has not been called"
        if commit:
            self.commit(exiting=True)
        self._logger.debug("close database [%s]", self._file_path)
        self._cursor.close()
        self._cursor = None
        self._connection.close()
        self._connection = None
        # Clean up the temp database.
        if self.temp_db_path and os.path.exists(self.temp_db_path):
            os.remove(self.temp_db_path)
        return True

    def _connect(self):
        self._connection = self.stormdb.connection._raw_connection
        self._cursor = self._connection.cursor()

    @inlineCallbacks
    def _initial_statements(self):
        assert self._cursor is not None, "Database.close() has been called or Database.open() has not been called"
        assert self._connection is not None, "Database.close() has been called or Database.open() has not been called"

        # collect current database configuration
        db_page_size = yield self.stormdb.fetchone(u"PRAGMA page_size")
        page_size = int(db_page_size[0])
        db_journal_mode = yield self.stormdb.fetchone(u"PRAGMA journal_mode")
        journal_mode = unicode(db_journal_mode[0]).upper()
        db_synchronous = yield self.stormdb.fetchone(u"PRAGMA synchronous")
        synchronous = unicode(db_synchronous[0]).upper()

        #
        # PRAGMA page_size = bytes;
        # http://www.sqlite.org/pragma.html#pragma_page_size
        # Note that changing page_size has no effect unless performed on a new database or followed
        # directly by VACUUM.  Since we do not want the cost of VACUUM every time we load a
        # database, existing databases must be upgraded.
        #
        if page_size < 8192:
            self._logger.debug("PRAGMA page_size = 8192 (previously: %s) [%s]", page_size, self._file_path)

            # it is not possible to change page_size when WAL is enabled
            if journal_mode == u"WAL":
                yield self.stormdb.execute(u"PRAGMA journal_mode = DELETE")
                journal_mode = u"DELETE"
                yield self.stormdb.execute(u"PRAGMA page_size = 8192")
                yield self.stormdb.execute(u"VACUUM")

        else:
            self._logger.debug("PRAGMA page_size = %s (no change) [%s]", page_size, self._file_path)

        #
        # PRAGMA journal_mode = DELETE | TRUNCATE | PERSIST | MEMORY | WAL | OFF
        # http://www.sqlite.org/pragma.html#pragma_page_size
        #
        if not (journal_mode == u"WAL" or self._file_path == u":memory:"):
            self._logger.debug("PRAGMA journal_mode = WAL (previously: %s) [%s]", journal_mode, self._file_path)
            yield self.stormdb.execute(u"PRAGMA locking_mode = EXCLUSIVE")
            yield self.stormdb.execute(u"PRAGMA journal_mode = WAL")

        else:
            self._logger.debug("PRAGMA journal_mode = %s (no change) [%s]", journal_mode, self._file_path)

        #
        # PRAGMA synchronous = 0 | OFF | 1 | NORMAL | 2 | FULL;
        # http://www.sqlite.org/pragma.html#pragma_synchronous
        #
        if not synchronous in (u"NORMAL", u"1"):
            self._logger.debug("PRAGMA synchronous = NORMAL (previously: %s) [%s]", synchronous, self._file_path)
            yield self.stormdb.execute(u"PRAGMA synchronous = NORMAL")

        else:
            self._logger.debug("PRAGMA synchronous = %s (no change) [%s]", synchronous, self._file_path)

    @inlineCallbacks
    def _prepare_version(self):
        assert self._cursor is not None, "Database.close() has been called or Database.open() has not been called"
        assert self._connection is not None, "Database.close() has been called or Database.open() has not been called"

        # check is the database contains an 'option' table
        try:
            count, = yield self.stormdb.fetchone(u"SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'option'")
        except TypeError:
            raise RuntimeError()

        if count:
            # get version from required 'option' table
            try:
                version, = yield self.stormdb.fetchone(u"SELECT value FROM option WHERE key == 'database_version' LIMIT 1")
            except TypeError:
                # the 'database_version' key was not found
                version = u"0"
        else:
            # the 'option' table probably hasn't been created yet
            version = u"0"

        self._database_version = yield self.check_database(version)
        assert isinstance(self._database_version, (int, long)), type(self._database_version)

    @property
    def database_version(self):
        return self._database_version

    @property
    def file_path(self):
        """
        The database filename including path.
        """
        return self._file_path

    def __enter__(self):
        """
        Enters a no-commit state.  The commit will be performed by __exit__.

        @return: The method self.execute
        """
        assert self._cursor is not None, "Database.close() has been called or Database.open() has not been called"
        assert self._connection is not None, "Database.close() has been called or Database.open() has not been called"
        assert self._debug_thread_ident != 0, "please call database.open() first"
        assert self._debug_thread_ident == thread.get_ident(), "Calling Database.execute on the wrong thread"

        self._logger.debug("disabling commit [%s]", self._file_path)
        self._pending_commits = max(1, self._pending_commits)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Leaves a no-commit state.  A commit will be performed if Database.commit() was called while
        in the no-commit state.
        """
        assert self._cursor is not None, "Database.close() has been called or Database.open() has not been called"
        assert self._connection is not None, "Database.close() has been called or Database.open() has not been called"
        assert self._debug_thread_ident != 0, "please call database.open() first"
        assert self._debug_thread_ident == thread.get_ident(), "Calling Database.execute on the wrong thread"

        self._pending_commits, pending_commits = 0, self._pending_commits

        if exc_type is None:
            self._logger.debug("enabling commit [%s]", self._file_path)
            if pending_commits > 1:
                self._logger.debug("performing %d pending commits [%s]", pending_commits - 1, self._file_path)
                self.commit()
            return True

        elif isinstance(exc_value, IgnoreCommits):
            self._logger.debug("enabling commit without committing now [%s]", self._file_path)
            return True

        else:
            # Niels 23-01-2013, an exception happened from within the with database block
            # returning False to let Python reraise the exception.
            return False

    #@attach_explain_query_plan
    #@attach_runtime_statistics(u"{0.__class__.__name__}.{function_name} {1} [{0.file_path}]")
    def execute(self, statement, bindings=(), get_lastrowid=False):
        """
        Execute one SQL statement.

        A SQL query must be presented in unicode format.  This is to ensure that no unicode
        exeptions occur when the bindings are merged into the statement.

        Furthermore, the bindings may not contain any strings either.  For a 'string' the unicode
        type must be used.  For a binary string the buffer(...) type must be used.

        The SQL query may contain placeholder entries defined with a '?'.  Each of these
        placeholders will be used to store one value from bindings.  The placeholders are filled by
        sqlite and all proper escaping is done, making this the preferred way of adding variables to
        the SQL query.

        @param statement: the SQL statement that is to be executed.
        @type statement: unicode

        @param bindings: the values that must be set to the placeholders in statement.
        @type bindings: list, tuple, dict, or set

        @returns: unknown
        @raise sqlite.Error: unknown
        """
        if __debug__:
            assert self._cursor is not None, "Database.close() has been called or Database.open() has not been called"
            assert self._connection is not None, "Database.close() has been called or Database.open() has not been called"
            assert self._debug_thread_ident != 0, "please call database.open() first"
            assert self._debug_thread_ident == thread.get_ident(), "Calling Database.execute on the wrong thread"
            assert isinstance(statement, unicode), "The SQL statement must be given in unicode"
            assert isinstance(bindings, (tuple, list, dict, set)), "The bindings must be a tuple, list, dictionary, or set"

            # bindings may not be strings, text must be given as unicode strings while binary data,
            # i.e. blobs, must be given as a buffer(...)
            if isinstance(bindings, dict):
                tests = (not isinstance(binding, str) for binding in bindings.itervalues())
            else:
                tests = (not isinstance(binding, str) for binding in bindings)
            assert all(tests), "Bindings may not be strings.  Provide unicode for TEXT and buffer(...) for BLOB\n%s" % (statement,)

        self._logger.log(logging.NOTSET, "%s <-- %s [%s]", statement, bindings, self._file_path)
        result = self._cursor.execute(statement, bindings)
        if get_lastrowid:
            result = self._cursor.lastrowid
        return result

    #@attach_runtime_statistics(u"{0.__class__.__name__}.{function_name} {1} [{0.file_path}]")
    def executescript(self, statements):
        assert self._cursor is not None, "Database.close() has been called or Database.open() has not been called"
        assert self._connection is not None, "Database.close() has been called or Database.open() has not been called"
        assert self._debug_thread_ident != 0, "please call database.open() first"
        assert self._debug_thread_ident == thread.get_ident(), "Calling Database.execute on the wrong thread"
        assert isinstance(statements, unicode), "The SQL statement must be given in unicode"

        self._logger.log(logging.NOTSET, "%s [%s]", statements, self._file_path)
        return self._cursor.executescript(statements)

    #@attach_explain_query_plan
    #@attach_runtime_statistics(u"{0.__class__.__name__}.{function_name} {1} [{0.file_path}]")
    def executemany(self, statement, sequenceofbindings):
        """
        Execute one SQL statement several times.

        All SQL queries must be presented in unicode format.  This is to ensure that no unicode
        exeptions occur when the bindings are merged into the statement.

        Furthermore, the bindings may not contain any strings either.  For a 'string' the unicode
        type must be used.  For a binary string the buffer(...) type must be used.

        The SQL query may contain placeholder entries defined with a '?'.  Each of these
        placeholders will be used to store one value from bindings.  The placeholders are filled by
        sqlite and all proper escaping is done, making this the preferred way of adding variables to
        the SQL query.

        @param statement: the SQL statement that is to be executed.
        @type statement: unicode

        @param sequenceofbindings: a list, tuple, set, or generator of bindings, where every binding
                                   contains the values that must be set to the placeholders in
                                   statement.

        @type sequenceofbindings: list, tuple, set or generator

        @returns: unknown
        @raise sqlite.Error: unknown
        """
        assert self._cursor is not None, "Database.close() has been called or Database.open() has not been called"
        assert self._connection is not None, "Database.close() has been called or Database.open() has not been called"
        assert self._debug_thread_ident != 0, "please call database.open() first"
        assert self._debug_thread_ident == thread.get_ident(), "Calling Database.execute on the wrong thread"
        if __debug__:
            # we allow GeneratorType but must convert it to a list in __debug__ mode since a
            # generator can only iterate once
            from types import GeneratorType
            is_iterator = isinstance(sequenceofbindings, GeneratorType)
            if is_iterator:
                sequenceofbindings = list(sequenceofbindings)

            assert isinstance(statement, unicode), "The SQL statement must be given in unicode"
            assert isinstance(sequenceofbindings, (tuple, list, set)), "The sequenceofbindings must be a tuple, list, or set"
            assert all(isinstance(x, (tuple, list, dict, set)) for x in sequenceofbindings), "The sequenceofbindings must be a list with tuples, lists, dictionaries, or sets"

            for bindings in sequenceofbindings:
                # bindings may not be strings, text must be given as unicode strings while binary data,
                # i.e. blobs, must be given as a buffer(...)
                if isinstance(bindings, dict):
                    tests = (not isinstance(binding, str) for binding in bindings.itervalues())
                else:
                    tests = (not isinstance(binding, str) for binding in bindings)
                assert all(tests), "Bindings may not be strings.  Provide unicode for TEXT and buffer(...) for BLOB\n%s" % (statement,)

            if is_iterator:
                sequenceofbindings = iter(sequenceofbindings)

        self._logger.log(logging.NOTSET, "%s [%s]", statement, self._file_path)
        return self._cursor.executemany(statement, sequenceofbindings)

    #@attach_runtime_statistics(u"{0.__class__.__name__}.{function_name} [{0.file_path}]")
    def commit(self, exiting=False):
        assert self._cursor is not None, "Database.close() has been called or Database.open() has not been called"
        assert self._connection is not None, "Database.close() has been called or Database.open() has not been called"
        assert self._debug_thread_ident != 0, "please call database.open() first"
        assert self._debug_thread_ident == thread.get_ident(), "Calling Database.commit on the wrong thread"
        assert not (exiting and self._pending_commits), "No pending commits should be present when exiting"

        if self._pending_commits:
            self._logger.debug("defer commit [%s]", self._file_path)
            self._pending_commits += 1
            return False

        else:
            self._logger.debug("commit [%s]", self._file_path)
            for callback in self._commit_callbacks:
                try:
                    callback(exiting=exiting)
                except Exception as exception:
                    self._logger.exception("%s [%s]", exception, self._file_path)

            return self._connection.commit()

    @abstractmethod
    def check_database(self, database_version):
        """
        Check the database and upgrade if required.

        This method is called once for each Database instance to ensure that the database structure
        and version is correct.  Each Database must contain one table of the structure below where
        the database_version is stored.  This value is used to keep track of the current database
        version.

        >>> CREATE TABLE option(key TEXT PRIMARY KEY, value BLOB);
        >>> INSERT INTO option(key, value) VALUES('database_version', '1');

        @param database_version: the current database_version value from the option table. This
         value reverts to u'0' when the table could not be accessed.
        @type database_version: unicode
        """
        pass

    def attach_commit_callback(self, func):
        assert not func in self._commit_callbacks
        self._commit_callbacks.append(func)

    def detach_commit_callback(self, func):
        assert func in self._commit_callbacks
        self._commit_callbacks.remove(func)
