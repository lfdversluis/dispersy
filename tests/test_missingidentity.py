from twisted.internet.task import deferLater

from nose.twistedtools import deferred, reactor
from twisted.internet.defer import inlineCallbacks

from .dispersytestclass import DispersyTestFunc


class TestMissingIdentity(DispersyTestFunc):

    @deferred(timeout=10)
    @inlineCallbacks
    def test_incoming_missing_identity(self):
        """
        NODE generates a missing-identity message and OTHER responds.
        """
        node, other = yield self.create_nodes(2)
        yield node.send_identity(other)

        # use NODE to fetch the identities for OTHER
        created_missing_identity = yield node.create_missing_identity(other.my_member, 10)
        yield other.give_message(created_missing_identity, node)

        # MISSING should reply with a dispersy-identity message
        responses = yield node.receive_messages()

        self.assertEqual(len(responses), 1)
        for _, response in responses:
            self.assertEqual(response.name, u"dispersy-identity")
            self.assertEqual(response.authentication.member.public_key, other.my_member.public_key)

    @deferred(timeout=10)
    @inlineCallbacks
    def test_outgoing_missing_identity(self):
        """
        NODE generates data and sends it to OTHER, resulting in OTHER asking for the other identity.
        """
        node, other = yield self.create_nodes(2)

        # Give OTHER a message from NODE
        message = yield node.create_full_sync_text("Hello World", 10)
        yield other.give_message(message, node)

        # OTHER must not yet process the 'Hello World' message, as it hasnt received the identity message yet
        yield other.assert_not_stored(message)

        # OTHER must send a missing-identity to NODEs
        responses = yield node.receive_messages()
        self.assertEqual(len(responses), 1)
        for _, response in responses:
            self.assertEqual(response.name, u"dispersy-missing-identity")
            self.assertEqual(response.payload.mid, node.my_member.mid)

        # NODE sends the identity to OTHER
        yield node.send_identity(other)

        # OTHER must now process and store the 'Hello World' message
        yield other.assert_is_stored(message)

    @deferred(timeout=10)
    @inlineCallbacks
    def test_outgoing_missing_identity_twice(self):
        """
        NODE generates data and sends it to OTHER twice, resulting in OTHER asking for the other identity once.
        """
        node, other = yield self.create_nodes(2)

        # Give OTHER a message from NODE
        message = yield node.create_full_sync_text("Hello World", 10)
        yield other.give_message(message, node)

        # OTHER must not yet process the 'Hello World' message, as it hasnt received the identity message yet
        yield other.assert_not_stored(message)

        # Give OTHER the message once again
        yield other.give_message(message, node)

        # OTHER must send a single missing-identity to NODE
        responses = yield node.receive_messages()
        self.assertEqual(len(responses), 1)
        for _, response in responses:
            self.assertEqual(response.name, u"dispersy-missing-identity")
            self.assertEqual(response.payload.mid, node.my_member.mid)

        # NODE sends the identity to OTHER
        yield node.send_identity(other)

        # OTHER must now process and store the 'Hello World' message
        yield other.assert_is_stored(message)
