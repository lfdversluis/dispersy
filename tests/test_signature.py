from nose.twistedtools import deferred, reactor
from twisted.internet.defer import inlineCallbacks
from twisted.internet.task import deferLater

from .dispersytestclass import DispersyTestFunc


class TestSignature(DispersyTestFunc):

    @deferred(timeout=10)
    @inlineCallbacks
    def test_invalid_public_key(self):
        """
        NODE sends a message containing an invalid public-key to OTHER.
        OTHER should drop it
        """
        node, other = yield self.create_nodes(2)
        yield other.send_identity(node)

        message = yield node.create_bin_key_text('Should drop')
        packet = yield node.encode_message(message)

        # replace the valid public-key with an invalid one
        public_key = node.my_member.public_key
        self.assertIn(public_key, packet)

        invalid_packet = packet.replace(public_key, "I" * len(public_key))
        self.assertNotEqual(packet, invalid_packet)

        # give invalid message to OTHER
        yield other.give_packet(invalid_packet, node)

        other_messages = yield other.fetch_messages([u"bin-key-text", ])
        self.assertEqual(other_messages, [])

    @deferred(timeout=10)
    @inlineCallbacks
    def test_invalid_signature(self):
        """
        NODE sends a message containing an invalid signature to OTHER.
        OTHER should drop it
        """
        node, other = yield self.create_nodes(2)
        yield other.send_identity(node)

        message = yield node.create_full_sync_text('Should drop')
        packet = yield node.encode_message(message)

        # replace the valid signature with an invalid one
        invalid_packet = packet[:-node.my_member.signature_length] + 'I' * node.my_member.signature_length
        self.assertNotEqual(packet, invalid_packet)

        # give invalid message to OTHER
        yield other.give_packet(invalid_packet, node)

        other_messages = yield other.fetch_messages([u"full-sync-text", ])
        self.assertEqual(other_messages, [])
