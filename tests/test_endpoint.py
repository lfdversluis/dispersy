from tempfile import mkdtemp
from twisted.internet.defer import inlineCallbacks

from nose.twistedtools import deferred

from ..dispersy import Dispersy
from ..candidate import Candidate
from ..endpoint import NullEndpoint
from ..tests.dispersytestclass import DispersyTestFunc


class TestEndpoint(DispersyTestFunc):
    """
    This class contains tests that test the various endpoints
    """

    @deferred(timeout=10)
    @inlineCallbacks
    def setUp(self):
        yield super(TestEndpoint, self).setUp()
        self.nodes = []

    @deferred(timeout=10)
    @inlineCallbacks
    def tearDown(self):
        yield super(TestEndpoint, self).tearDown()

    def test_null_endpoint_address(self):
        """
        Test that the default address is returned
        """
        null_endpoint = NullEndpoint()

        self.assertEqual(null_endpoint.get_address(), ("0.0.0.0", 42))

    def test_null_endpoint_set_address(self):
        """
        Test that the det address tuple is returned when set.
        """
        null_endpoint = NullEndpoint(address=("127.0.0.1", 1337))

        self.assertEqual(null_endpoint.get_address(), ("127.0.0.1", 1337))

    @deferred(timeout=10)
    @inlineCallbacks
    def test_null_endpoint_listen_to(self):
        """
        Tests that null endpoint listen_to does absolutely nothing
        """
        null_endpoint = NullEndpoint()
        memory_database_argument = {'database_filename': u":memory:"}
        working_directory = unicode(mkdtemp(suffix="_dispersy_test_session"))

        dispersy = Dispersy(null_endpoint, working_directory, **memory_database_argument)
        yield dispersy.initialize_statistics()
        null_endpoint.open(dispersy)
        null_endpoint.listen_to(None, None)

        self.assertEqual(dispersy.statistics.total_up, 0)

    @deferred(timeout=10)
    @inlineCallbacks
    def test_null_endpoint_send_packet(self):
        """
        Test that send packet raises the dispersy statistic' s total_up
        """
        null_endpoint = NullEndpoint()
        memory_database_argument = {'database_filename': u":memory:"}
        working_directory = unicode(mkdtemp(suffix="_dispersy_test_session"))

        dispersy = Dispersy(null_endpoint, working_directory, **memory_database_argument)
        yield dispersy.initialize_statistics()
        null_endpoint.open(dispersy)

        packet = "Fake packet"
        candidate = Candidate(("197.168.0.1", 42), False)

        null_endpoint.send([candidate], [packet])

        expected_up = len(packet)

        self.assertEqual(dispersy.statistics.total_up, expected_up)
