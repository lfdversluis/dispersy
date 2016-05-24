from storm.locals import *
from storm.twisted.transact import transact, Transactor
from twisted.internet import reactor


class StormDBManager:
    """
    The StormDBManager is a manager that runs queries using the Storm Framework.
    These queries will be run on the Twisted thread-pool to ensure asynchronous, non-blocking behavior.
    In the future, this database manager will be the basis to an ORM based approach.
    """

    def __init__(self):
        """
        Sets up the database and all necessary elements for the
        database manager to function.
        """
        # This is currently an IN MEMORY database
        self.database = create_database("sqlite:")


        # The transactor is required when you have methods decorated with the @transact decorator
        self.transactor = Transactor(reactor.getThreadPool())

    @transact
    def execute_query(self, query, arguments = None):
        """
        Executes a query on the twisted thread-pool using the storm framework.
        :param query: The sql query to be executed
        :param arguments: Optional arguments that go with the sql query
        :return: None as this is function is executed on the thread-pool, database objects
        such as cursors cannot be returned.
        """
        self.database._cursor.execute(query, arguments)

    @transact
    def fetch_one(self, query, arguments = None):
        """
        Executes a query on the twisted thread-pool using the storm framework and returns the first result.
        :param query: The sql query to be executed.
        :param arguments: Optional arguments that go with the sql query.
        :return: The first tuple that matches the query or None, would be the same as using execute
        and calling the next() functions on it.
        """
        return self.database._cursor.fetchone(query, arguments)
