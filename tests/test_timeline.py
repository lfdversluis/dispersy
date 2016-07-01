from twisted.internet.task import deferLater

from nose.twistedtools import deferred, reactor
from twisted.internet.defer import inlineCallbacks

from .dispersytestclass import DispersyTestFunc


class TestTimeline(DispersyTestFunc):

    @deferred(timeout=10)
    @inlineCallbacks
    def test_delay_by_proof(self):
        """
        When OTHER receives a message that it has no permission for, it will send a
        dispersy-missing-proof message to try to obtain the dispersy-authorize.
        """
        node, other = yield self.create_nodes(2)
        yield node.send_identity(other)

        # permit NODE
        proof_msg = yield self._mm.create_authorize([(node.my_member, self._community.get_meta_message(u"protected-full-sync-text"), u"permit"),
                                    (node.my_member, self._community.get_meta_message(u"protected-full-sync-text"), u"authorize")])

        # NODE creates message
        tmessage = yield node.create_protected_full_sync_text("Protected message", 42)
        yield other.give_message(tmessage, node)
        # must NOT have been stored in the database
        yield other.assert_not_stored(tmessage)

        # OTHER sends dispersy-missing-proof to NODE
        responses = yield node.receive_messages()
        self.assertEqual(len(responses), 1)
        for _, message in responses:
            self.assertEqual(message.name, u"dispersy-missing-proof")
            self.assertEqual(message.payload.member.public_key, node.my_member.public_key)
            self.assertEqual(message.payload.global_time, 42)

        # NODE provides proof
        yield other.give_message(proof_msg, node)

        # must have been stored in the database
        yield other.assert_is_stored(tmessage)

    @deferred(timeout=10)
    @inlineCallbacks
    def test_missing_proof(self):
        """
        When OTHER receives a dispersy-missing-proof message it needs to find and send the proof.
        """
        node, other = yield self.create_nodes(2)
        yield node.send_identity(other)

        # permit NODE
        authorize = yield self._mm.create_authorize([(node.my_member, self._community.get_meta_message(u"protected-full-sync-text"), u"permit"),
                                               (node.my_member, self._community.get_meta_message(u"protected-full-sync-text"), u"authorize")])
        yield node.give_message(authorize, self._mm)

        protected_text = yield node.create_protected_full_sync_text("Protected message", 42)
        yield node.store([protected_text])

        # OTHER pretends to received the protected message and requests the proof
        created_missing_proof = yield other.create_missing_proof(node.my_member, 42)
        yield node.give_message(created_missing_proof, other)

        # NODE sends dispersy-authorize to OTHER
        received_message = yield other.receive_message(names=[u"dispersy-authorize"])
        _, authorize = received_message.next()

        permission_triplet = (node.my_member.mid, u"protected-full-sync-text", u"permit")
        authorize_permission_triplets = [(triplet[0].mid, triplet[1].name, triplet[2]) for triplet in authorize.payload.permission_triplets]
        self.assertIn(permission_triplet, authorize_permission_triplets)

    @deferred(timeout=10)
    @inlineCallbacks
    def test_missing_authorize_proof(self):
        """
             MASTER
               \\        authorize(MASTER, OWNER)
                \\
                OWNER
                  \\        authorize(OWNER, NODE1)
                   \\
                   NODE1

        When NODE receives a dispersy-missing-proof message from OTHER for authorize(MM, NODE)
        the dispersy-authorize message for authorize(MASTER, MM) must be returned.
        """
        node, other = yield self.create_nodes(2)
        yield node.send_identity(other)

        # permit NODE
        authorize = yield self._mm.create_authorize([(node.my_member, self._community.get_meta_message(u"protected-full-sync-text"), u"permit"),
                                             (node.my_member, self._community.get_meta_message(u"protected-full-sync-text"), u"authorize")])
        yield node.give_message(authorize, self._mm)

        # OTHER wants the proof that OWNER is allowed to grant authorization to NODE
        created_missing_proof = yield other.create_missing_proof(authorize.authentication.member, authorize.distribution.global_time)
        yield node.give_message(created_missing_proof, other)

        # NODE sends dispersy-authorize containing authorize(MASTER, OWNER) to OTHER
        received_message = yield other.receive_message(names=[u"dispersy-authorize"])
        _, authorize = received_message.next()

        permission_triplet = (self._mm.my_member.mid, u"protected-full-sync-text", u"permit")
        authorize_permission_triplets = [(triplet[0].mid, triplet[1].name, triplet[2]) for triplet in authorize.payload.permission_triplets]
        self.assertIn(permission_triplet, authorize_permission_triplets)
