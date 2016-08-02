import os
import shutil
from unittest import TestCase

from nose.tools import raises
from nose.twistedtools import deferred
from twisted.internet.defer import inlineCallbacks

from ..dispersydatabase import DispersyDatabase


class TestDatabase(TestCase):
    FILE_DIR = os.path.abspath(os.path.dirname(os.path.realpath(__file__)))
    TEST_DATA_DIR = os.path.abspath(os.path.join(FILE_DIR, u"data"))
    TMP_DATA_DIR = os.path.abspath(os.path.join(FILE_DIR, u"tmp"))

    @deferred(timeout=10)
    @inlineCallbacks
    def setUp(self):
        yield super(TestDatabase, self).setUp()

        # Do not use an in-memory database. Different connections to the same
        # in-memory database do not point towards the same database.
        # http://stackoverflow.com/questions/3315046/sharing-a-memory-database-between-different-threads-in-python-using-sqlite3-pa
        if not os.path.exists(self.TEST_DATA_DIR):
            os.mkdir(self.TEST_DATA_DIR)

        if not os.path.exists(self.TMP_DATA_DIR):
            os.mkdir(self.TMP_DATA_DIR)

    @deferred(timeout=10)
    @inlineCallbacks
    def tearDown(self):
        yield super(TestDatabase, self).tearDown()
        # Delete the database file if not using an in-memory database.
        if os.path.exists(self.TMP_DATA_DIR):
            shutil.rmtree(self.TMP_DATA_DIR, ignore_errors=True)

    @raises(RuntimeError)
    @deferred(timeout=10)
    @inlineCallbacks
    def test_unsupported_database_version(self):
        minimum_version_path = os.path.abspath(os.path.join(self.TEST_DATA_DIR, u"dispersy_v1.db"))
        tmp_path = os.path.join(self.TMP_DATA_DIR, u"dispersy.db")
        shutil.copyfile(minimum_version_path, tmp_path)

        database = DispersyDatabase(tmp_path)
        yield database.open()

    @deferred(timeout=10)
    @inlineCallbacks
    def test_upgrade_16_to_latest(self):
        minimum_version_path = os.path.abspath(os.path.join(self.TEST_DATA_DIR, u"dispersy_v16.db"))
        tmp_path = os.path.join(self.TMP_DATA_DIR, u"dispersy.db")
        shutil.copyfile(minimum_version_path, tmp_path)

        database = DispersyDatabase(tmp_path)
        yield database.open()
        self.assertEqual(database.version, 21)

    @raises(RuntimeError)
    @deferred(timeout=10)
    @inlineCallbacks
    def test_upgrade_version_too_high(self):
        minimum_version_path = os.path.abspath(os.path.join(self.TEST_DATA_DIR, u"dispersy_v1337.db"))
        tmp_path = os.path.join(self.TMP_DATA_DIR, u"dispersy.db")
        shutil.copyfile(minimum_version_path, tmp_path)

        database = DispersyDatabase(tmp_path)
        yield database.open()
