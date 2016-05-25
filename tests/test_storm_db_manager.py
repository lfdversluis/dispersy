import os
from unittest import TestCase

from nose.twistedtools import deferred

from StormDBManager import StormDBManager


class TestStormDBManager(TestCase):

    FILE_DIR = os.path.abspath(os.path.dirname(os.path.realpath(__file__)))
    TEST_DATA_DIR = os.path.abspath(os.path.join(FILE_DIR, u"data"))
    SQLITE_TEST_DB = os.path.abspath(os.path.join(TEST_DATA_DIR, u"test.db"))

    def setUp(self):
        super(TestStormDBManager, self).setUp()
        self.storm_db = StormDBManager("sqlite:%s" % self.SQLITE_TEST_DB)

    def tearDown(self):
        super(TestStormDBManager, self).tearDown()
        # Delete the database file.
        os.unlink(self.SQLITE_TEST_DB)

    @deferred(timeout=5)
    def test_execute_function(self):
        sql = u"CREATE TABLE car(brand);"

        def check_return_is_none(result):
            self.assertIsNone(result)

        result_deferred = self.storm_db.execute_query(sql)
        result_deferred.addCallback(check_return_is_none)
        return result_deferred

    @deferred(timeout=5)
    def test_insert_and_fetchone(self):
        sql = u"CREATE TABLE car(brand);"

        def run_query(_):
            return self.storm_db.insert("car", brand="BMW")


        result_deferred = self.storm_db.execute_query(sql)
        return result_deferred.addCallback(run_query)
