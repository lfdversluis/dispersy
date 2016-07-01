from random import shuffle

from nose.twistedtools import deferred
from twisted.internet.defer import inlineCallbacks

from .dispersytestclass import DispersyTestFunc


class TestMissingMessage(DispersyTestFunc):

    def setUp(self):
        super(TestMissingMessage, self).setUp()
        self.nodes = []

    @inlineCallbacks
    def _test_with_order(self, batchFUNC):
        """
        NODE generates a few messages and OTHER requests them one at a time.
        """
        node, other = yield self.create_nodes(2)

        self.nodes.append(node)
        self.nodes.append(other)
        self.patch_send_packet_for_nodes()

        yield node.send_identity(other)

        # create messages
        messages = []
        for i in xrange(10):
            created_full_sync_text = yield node.create_full_sync_text("Message #%d" % i, i + 10)
            messages.append(created_full_sync_text)

        yield node.give_messages(messages, node)

        batches = batchFUNC(messages)

        for messages in batches:
            global_times = sorted([message.distribution.global_time for message in messages])
            # request messages
            missing_message = yield other.create_missing_message(node.my_member, global_times)
            yield node.give_message(missing_message, other)

            # receive response
            messages = yield other.receive_messages(names=[message.name])
            responses = [response for _, response in messages]
            self.assertEqual(sorted(response.distribution.global_time for response in responses), global_times)

    @deferred(timeout=10)
    def test_single_request(self):
        def batch(messages):
            return [[message] for message in messages]
        return self._test_with_order(batch)

    @deferred(timeout=10)
    def test_single_request_out_of_order(self):
        def batch(messages):
            shuffle(messages)
            return [[message] for message in messages]
        return self._test_with_order(batch)

    @deferred(timeout=10)
    def test_two_at_a_time(self):
        def batch(messages):
            batches = []
            for i in range(0, len(messages), 2):
                batches.append([messages[i], messages[i + 1]])
            return batches
        return self._test_with_order(batch)
