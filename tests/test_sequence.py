from collections import defaultdict

from nose.twistedtools import deferred
from twisted.internet.defer import inlineCallbacks, returnValue

from .dispersytestclass import DispersyTestFunc


class TestIncomingMissingSequence(DispersyTestFunc):

    def setUp(self):
        super(TestIncomingMissingSequence, self).setUp()
        self.nodes = []

    @deferred(timeout=10)
    @inlineCallbacks
    # TODO(Laurens): This is not called?
    def incoming_simple_conflict_different_global_time(self):
        """
        A broken NODE creates conflicting messages with the same sequence number that OTHER should
        properly filter.

        We use the following messages:
        - M@5#1 :: global time 5, sequence number 1
        - M@6#1 :: global time 6, sequence number 1
        - etc...

        """
        node, other = yield self.create_nodes(2)
        yield other.send_identity(node)

        msgs = defaultdict(dict)
        @inlineCallbacks
        # TODO(Laurens): Add yields in front of the function calls of this function to handle the deferred
        # once this method will be used again.
        def get_message(global_time, seq):
            if not global_time in msgs or not seq in msgs[global_time]:
                msgs[global_time][seq] = yield node.create_sequence_text("M@%d#%d" % (global_time, seq), global_time, seq)
            message = msgs[global_time][seq]
            returnValue(message)

        # NODE must accept M@6#1
        yield other.give_message(get_message(6, 1), node)
        yield other.assert_is_stored(get_message(6, 1))

        # NODE must reject M@6#1 (already have this message)
        yield other.give_message(get_message(6, 1), node)
        yield other.assert_is_stored(get_message(6, 1))

        # NODE must prefer M@5#1 (duplicate sequence number, prefer lower global time)
        yield other.give_message(get_message(5, 1), node)
        yield other.assert_is_stored(get_message(5, 1))
        yield other.assert_not_stored(get_message(6, 1))

        # NODE must reject M@6#1 (duplicate sequence number, prefer lower global time)
        yield other.give_message(get_message(6, 1), node)
        yield other.assert_is_stored(get_message(5, 1))
        yield other.assert_not_stored(get_message(6, 1))

        # NODE must reject M@4#2 (global time is lower than previous global time in sequence)
        yield other.give_message(get_message(4, 2), node)
        yield other.assert_is_stored(get_message(5, 1))
        yield other.assert_not_stored(get_message(4, 2))

        # NODE must reject M@5#2 (duplicate global time)
        yield other.give_message(get_message(5, 2), node)
        yield other.assert_is_stored(get_message(5, 1))
        yield other.assert_not_stored(get_message(5, 2))

        # NODE must accept M@7#2
        yield other.give_message(get_message(6, 2), node)
        yield other.assert_is_stored(get_message(5, 1))
        yield other.assert_is_stored(get_message(6, 2))

        # NODE must accept M@8#3
        yield other.give_message(get_message(8, 3), node)
        yield other.assert_is_stored(get_message(5, 1))
        yield other.assert_is_stored(get_message(6, 2))
        yield other.assert_is_stored(get_message(8, 3))

        # NODE must accept M@9#4
        yield other.give_message(get_message(9, 4), node)
        yield other.assert_is_stored(get_message(5, 1))
        yield other.assert_is_stored(get_message(6, 2))
        yield other.assert_is_stored(get_message(8, 3))
        yield other.assert_is_stored(get_message(9, 4))

        # NODE must accept M@7#3
        # It would be possible to keep M@9#4, but the way that the code is structures makes this
        # difficult (i.e. M@7#3 has not yet passed all the numerous checks at the point where we
        # have to delete).  In the future we can optimize by pushing the newer messages (such as
        # M@7#3) into the waiting or incoming packet queue, this will allow them to be re-inserted
        # after M@6#2 has been fully accepted.
        yield other.give_message(get_message(7, 3), node)
        yield other.assert_is_stored(get_message(5, 1))
        yield other.assert_is_stored(get_message(6, 2))
        yield other.assert_is_stored(get_message(7, 3))

    @deferred(timeout=10)
    @inlineCallbacks
    def test_requests_1_1(self):
        yield self.requests(1, [1], (1, 1))

    @deferred(timeout=10)
    @inlineCallbacks
    def test_requests_1_2(self):
        yield self.requests(1, [10], (10, 10))

    @deferred(timeout=10)
    @inlineCallbacks
    def test_requests_1_3(self):
        yield self.requests(1, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], (1, 10))

    @deferred(timeout=10)
    @inlineCallbacks
    def test_requests_1_4(self):
        yield self.requests(1, [3, 4, 5, 6, 7, 8, 9, 10], (3, 10))

    @deferred(timeout=10)
    @inlineCallbacks
    def test_requests_1_5(self):
        yield self.requests(1, [1, 2, 3, 4, 5, 6, 7], (1, 7))

    @deferred(timeout=10)
    @inlineCallbacks
    def test_requests_1_6(self):
        yield self.requests(1, [3, 4, 5, 6, 7], (3, 7))

    @deferred(timeout=10)
    @inlineCallbacks
    def test_requests_2_1(self):
        yield self.requests(2, [1], (1, 1))

    @deferred(timeout=10)
    @inlineCallbacks
    def test_requests_2_2(self):
        yield self.requests(2, [10], (10, 10))

    @deferred(timeout=10)
    @inlineCallbacks
    def test_requests_2_3(self):
        yield self.requests(2, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], (1, 10))

    @deferred(timeout=10)
    @inlineCallbacks
    def test_requests_2_4(self):
        yield self.requests(2, [3, 4, 5, 6, 7, 8, 9, 10], (3, 10))

    @deferred(timeout=10)
    @inlineCallbacks
    def test_requests_2_5(self):
        yield self.requests(2, [1, 2, 3, 4, 5, 6, 7], (1, 7))

    @deferred(timeout=10)
    @inlineCallbacks
    def test_requests_2_6(self):
        yield self.requests(2, [3, 4, 5, 6, 7], (3, 7))

    # multi-range requests
    @deferred(timeout=10)
    @inlineCallbacks
    def test_requests_1_7(self):
        yield self.requests(1, [1], (1, 1), (1, 1), (1, 1))

    @deferred(timeout=10)
    @inlineCallbacks
    def test_requests_1_9(self):
        yield self.requests(1, [1, 2, 3, 4, 5], (1, 2), (2, 3), (3, 4), (4, 5))

    @deferred(timeout=10)
    @inlineCallbacks
    def test_requests_1_11(self):
        yield self.requests(1, [1, 2, 4, 5, 7, 8], (1, 2), (4, 5), (7, 8))

    @deferred(timeout=10)
    @inlineCallbacks
    def test_requests_2_7(self):
        yield self.requests(2, [1], (1, 1), (1, 1), (1, 1))

    @deferred(timeout=10)
    @inlineCallbacks
    def test_requests_2_9(self):
        yield self.requests(2, [1, 2, 3, 4, 5], (1, 2), (2, 3), (3, 4), (4, 5))

    @deferred(timeout=10)
    @inlineCallbacks
    def test_requests_2_11(self):
        yield self.requests(2, [1, 2, 4, 5, 7, 8], (1, 2), (4, 5), (7, 8))

    # multi-range requests, in different orders
    @deferred(timeout=10)
    @inlineCallbacks
    def test_requests_1_13(self):
        yield self.requests(1, [1], (1, 1), (1, 1), (1, 1))

    @deferred(timeout=10)
    @inlineCallbacks
    def test_requests_1_15(self):
        yield self.requests(1, [1, 2, 3, 4, 5], (4, 5), (3, 4), (1, 2), (2, 3))

    @deferred(timeout=10)
    @inlineCallbacks
    def test_requests_1_16(self):
        yield self.requests(1, [1, 5], (5, 5), (1, 1))

    @deferred(timeout=10)
    @inlineCallbacks
    def test_requests_1_17(self):
        yield self.requests(1, [1, 2, 4, 5, 7, 8], (1, 2), (7, 8), (4, 5))

    @deferred(timeout=10)
    @inlineCallbacks
    def test_requests_2_13(self):
        yield self.requests(2, [1], (1, 1), (1, 1), (1, 1))

    @deferred(timeout=10)
    @inlineCallbacks
    def test_requests_2_15(self):
        yield self.requests(2, [1, 2, 3, 4, 5], (4, 5), (3, 4), (1, 2), (2, 3))

    @deferred(timeout=10)
    @inlineCallbacks
    def test_requests_2_16(self):
        yield self.requests(2, [1, 5], (5, 5), (1, 1))

    @deferred(timeout=10)
    @inlineCallbacks
    def test_requests_2_17(self):
        yield self.requests(2, [1, 2, 4, 5, 7, 8], (1, 2), (7, 8), (4, 5))

    # single range requests, invalid requests
    @deferred(timeout=10)
    @inlineCallbacks
    def test_requests_1_19(self):
        yield self.requests(1, [10], (10, 11))

    @deferred(timeout=10)
    @inlineCallbacks
    def test_requests_1_20(self):
        yield self.requests(1, [], (11, 11))

    @deferred(timeout=10)
    @inlineCallbacks
    def test_requests_2_19(self):
        yield self.requests(2, [10], (10, 11))

    @deferred(timeout=10)
    @inlineCallbacks
    def test_requests_2_20(self):
        yield self.requests(2, [], (11, 11))

    # multi-range requests, invalid requests
    @deferred(timeout=10)
    @inlineCallbacks
    def test_requests_1_23(self):
        yield self.requests(1, [10], (10, 11), (10, 100), (50, 75))

    @deferred(timeout=10)
    @inlineCallbacks
    def test_requests_1_24(self):
        yield self.requests(1, [], (11, 11), (11, 50), (100, 200))

    @deferred(timeout=10)
    @inlineCallbacks
    def test_requests_2_23(self):
        yield self.requests(2, [10], (10, 11), (10, 100), (50, 75))

    @deferred(timeout=10)
    @inlineCallbacks
    def test_requests_2_24(self):
        yield self.requests(2, [], (11, 11), (11, 50), (100, 200))

    @inlineCallbacks
    def requests(self, node_count, expected_responses, *pairs):
        """
        NODE1 through NODE<NODE_COUNT> requests OTHER (non)overlapping sequences, OTHER should send back the requested messages
        only once.
        """
        other, = yield self.create_nodes(1)
        nodes = yield self.create_nodes(node_count)
        self.nodes.append(other)
        self.nodes.extend(nodes)

        self.patch_send_packet_for_nodes()

        for node in nodes:
            other.send_identity(node)

        messages = []
        for i in xrange(1, 11):
            created_sequence_text = yield other.create_sequence_text("Sequence message #%d" % i, i + 10, i)
            messages.append(created_sequence_text)

        yield other.store(messages)

        # request missing
        # first, create all messages
        rmessages = defaultdict(list)
        for low, high in pairs:
            for node in nodes:
                missing_sequence = yield node.create_missing_sequence(other.my_member, messages[0].meta, low, high)
                rmessages[node].append(missing_sequence)

        # then, send them to other
        for node in nodes:
            for message in rmessages[node]:
                yield other.give_message(message, node, cache=True)

        # receive response
        for node in nodes:
            messages = yield node.receive_messages(names=[u"sequence-text"], timeout=0.1)
            responses = [response.distribution.sequence_number for _, response in messages]
            self.assertEqual(len(responses), len(expected_responses))

            for seq, expected_seq in zip(responses, expected_responses):
                self.assertEqual(seq, expected_seq)


class TestOutgoingMissingSequence(DispersyTestFunc):

    @deferred(timeout=15)
    @inlineCallbacks
    def test_missing(self):
        """
        NODE sends message while OTHER doesn't have the prior sequence numbers, OTHER should request these messages.
        """
        node, other = yield self.create_nodes(2)
        yield other.send_identity(node)

        messages = []
        for sequence in range(1, 11):
            created_sequence_text = yield node.create_sequence_text("Sequence message #%d" % sequence, sequence + 10, sequence)
            messages.append(created_sequence_text)

        # NODE gives #5, hence OTHER will request [#1:#4]
        yield other.give_message(messages[4], node)
        requests = yield node.receive_messages(names=[u"dispersy-missing-sequence"])
        self.assertEqual(len(requests), 1)

        _, request = requests[0]
        self.assertEqual(request.payload.member.public_key, node.my_member.public_key)
        self.assertEqual(request.payload.message.name, u"sequence-text")
        self.assertEqual(request.payload.missing_low, 1)
        self.assertEqual(request.payload.missing_high, 4)

        # NODE gives the missing packets, database should now contain [#1:#5]
        yield other.give_messages(messages[0:4], node)

        for message in messages[0:5]:
            yield other.assert_is_stored(message)

        # NODE gives #10, hence OTHER will request [#6:#9]
        yield other.give_message(messages[9], node)
        requests = yield node.receive_messages(names=[u"dispersy-missing-sequence"])
        self.assertEqual(len(requests), 1)

        _, request = requests[0]
        self.assertEqual(request.payload.member.public_key, node.my_member.public_key)
        self.assertEqual(request.payload.message.name, u"sequence-text")
        self.assertEqual(request.payload.missing_low, 6)
        self.assertEqual(request.payload.missing_high, 9)

        # NODE gives the missing packets, database should now contain [#1:#10]
        yield other.give_messages(messages[5:9], node)

        for message in messages:
            yield other.assert_is_stored(message)
