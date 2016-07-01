from twisted.internet.task import deferLater

from nose.twistedtools import deferred, reactor
from twisted.internet.defer import inlineCallbacks

from .dispersytestclass import DispersyTestFunc


class TestIdenticalPayload(DispersyTestFunc):

    @deferred(timeout=10)
    @inlineCallbacks
    def test_drop_identical_payload(self):
        """
        NODE creates two messages with the same community/member/global-time.
        Sends both of them to OTHER, which should drop the "lowest" one.
        """
        node, other = yield self.create_nodes(2)
        yield other.send_identity(node)

        # create messages
        messages = []
        created_full_sync_text1 = yield node.create_full_sync_text("Identical payload message", 42)
        messages.append(created_full_sync_text1)
        created_full_sync_text2 = yield node.create_full_sync_text("Identical payload message", 42)
        messages.append(created_full_sync_text2)
        self.assertNotEqual(messages[0].packet, messages[1].packet, "the signature must make the messages unique")

        # sort. we now know that the first message must be dropped
        messages.sort(key=lambda x: x.packet)

        # give messages in different batches
        yield other.give_message(messages[0], node)
        yield other.give_message(messages[1], node)

        yield other.assert_not_stored(messages[0])
        yield other.assert_is_stored(messages[1])

    @deferred(timeout=10)
    @inlineCallbacks
    def test_drop_identical(self):
        """
        NODE creates one message, sends it to OTHER twice
        """
        node, other = yield self.create_nodes(2)
        yield other.send_identity(node)

        # create messages
        message = yield node.create_full_sync_text("Message", 42)

        # give messages to other
        yield other.give_message(message, node)
        yield other.give_message(message, node)

        yield other.assert_is_stored(message)
