from storm.database import Connection
from storm.locals import *
from storm.twisted.transact import Transactor, transact
from twisted.internet import reactor


class StormDBManager:
    """
    The StormDBManager is a manager that runs queries using the Storm Framework.
    These queries will be run on the Twisted thread-pool to ensure asynchronous, non-blocking behavior.
    In the future, this database manager will be the basis to an ORM based approach.
    """

    def __init__(self, db_path):
        """
        Sets up the database and all necessary elements for the
        database manager to function.
        """
        # This is currently an IN MEMORY database
        self._database = create_database(db_path)

        # The transactor is required when you have methods decorated with the @transact decorator
        # This field name must NOT be changed.
        self.transactor = Transactor(reactor.getThreadPool())

        self.store = Store(self._database)

    @transact
    def execute_query(self, query, arguments = None):
        """
        Executes a query on the twisted thread-pool using the storm framework.
        :param query: The sql query to be executed
        :param arguments: Optional arguments that go with the sql query
        :return: None as this is function is executed on the thread-pool, database objects
        such as cursors cannot be returned.
        """
        print query
        print arguments
        print self._database
        connection = Connection(self._database)
        connection.execute( query, arguments, noresult=True)
        connection.commit()
        connection.close()

    @transact
    def fetch_one(self, query, arguments = None):
        """
        Executes a query on the twisted thread-pool using the storm framework and returns the first result.
        :param query: The sql query to be executed.
        :param arguments: Optional arguments that go with the sql query.
        :return: A deferred that fires with the first tuple that matches the query or None.
        The result would be the same as using execute and calling the next() function on it.
        """
        connection = Connection(self._database)
        result = connection.execute( query, arguments).get_one()
        connection.commit()
        connection.close()
        return result

    @transact
    def fetch_all(self, query, arguments):
        """
        Executes a query on the twisted thread-pool using the storm framework and
        returns all results as a list of tuples.
        :param query: The sql query to be executed.
        :param arguments: Optional arguments that go with the sql query.
        :return: A deferred that fires with a list of tuple results that matches the query or an empty list.
        """
        connection = Connection(self._database)
        return connection.execute(query, arguments).get_all()

    @transact
    def insert(self, table_name, **argv):
        """
        Inserts data provided as keyword arguments into the table provided as an argument.
        :param table_name: The name of the table the data has to be inserted into.
        :param argv: Keyword arguments for the column and the value to be inserted.
        :return: A deferred that fires when the data has been inserted.
        """
        if len(argv) == 0: return
        if len(argv) == 1:
            sql = u'INSERT INTO %s (%s) VALUES (?);' % (table_name, argv.keys()[0])
        else:
            questions = '?,' * len(argv)
            sql = u'INSERT INTO %s %s VALUES (%s);' % (table_name, tuple(argv.keys()), questions[:-1])

        connection = Connection(self._database)
        connection.execute(sql, argv.values())
        connection.commit()
        connection.close()
