from nose.twistedtools import deferred
from twisted.internet.defer import inlineCallbacks, returnValue

from .dispersytestclass import DispersyTestFunc


class TestUndo(DispersyTestFunc):

    def setUp(self):
        super(TestUndo, self).setUp()
        self.nodes = []

    @deferred(timeout=10)
    @inlineCallbacks
    def test_self_undo_own(self):
        """
        NODE generates a few messages and then undoes them.

        This is always allowed.  In fact, no check is made since only externally received packets
        will be checked.
        """
        node, = yield self.create_nodes(1)

        # create messages
        messages = []
        for i in xrange(10):
            created_full_sync_text = yield node.create_full_sync_text("Should undo #%d" % i, i + 10)
            messages.append(created_full_sync_text)

        yield node.give_messages(messages, node)

        # check that they are in the database and are NOT undone
        yield node.assert_is_stored(messages=messages)

        # undo all messages
        undoes = []
        for i, message in enumerate(messages):
            create_undo_own_message = yield node.create_undo_own(message, i + 100, i + 1)
            undoes.append(create_undo_own_message)

        yield node.give_messages(undoes, node)

        # check that they are in the database and ARE undone
        yield node.assert_is_undone(messages=messages)
        yield node.assert_is_stored(messages=undoes)

    @deferred(timeout=10)
    @inlineCallbacks
    def test_node_undo_other(self):
        """
        MM gives NODE permission to undo, OTHER generates a few messages and then NODE undoes
        them.
        """
        node, other = yield self.create_nodes(2)
        yield other.send_identity(node)

        # MM grants undo permission to NODE
        mm_claimed_global_time = yield self._mm.claim_global_time()
        authorize = yield self._mm.create_authorize([(node.my_member, self._community.get_meta_message(u"full-sync-text"), u"undo")], mm_claimed_global_time)
        yield node.give_message(authorize, self._mm)
        yield other.give_message(authorize, self._mm)

        # OTHER creates messages
        messages = []
        for i in xrange(10):
            created_full_sync_text = yield other.create_full_sync_text("Should undo #%d" % i, i + 10)
            messages.append(created_full_sync_text)

        yield node.give_messages(messages, other)

        # check that they are in the database and are NOT undone
        yield node.assert_is_stored(messages=messages)

        # NODE undoes all messages
        undoes = []
        for i, message in enumerate(messages):
            create_undo_other_message = yield node.create_undo_other(message, message.distribution.global_time + 100, 1 + i)
            undoes.append(create_undo_other_message)

        yield node.give_messages(undoes, node)

        # check that they are in the database and ARE undone
        yield node.assert_is_undone(messages=messages)
        yield node.assert_is_stored(messages=undoes)

    @deferred(timeout=10)
    @inlineCallbacks
    def test_self_attempt_undo_twice(self):
        """
        NODE generated a message and then undoes it twice. The dispersy core should ensure that
        that the second undo is refused and the first undo message should be returned instead.
        """
        node, = yield self.create_nodes(1)

        # create message
        message = yield node.create_full_sync_text("Should undo @%d" % 1, 1)
        yield node.give_message(message, node)

        # undo twice
        @inlineCallbacks
        def create_undoes():
            u1 = yield node._community.create_undo(message)
            u2 = yield node._community.create_undo(message)
            returnValue((u1, u2))
        undo1, undo2 = yield node.call(create_undoes)

        self.assertEqual(undo1.packet, undo2.packet)

    @deferred(timeout=10)
    @inlineCallbacks
    def test_node_resolve_undo_twice(self):
        """
        Make sure that in the event of receiving two undo messages from the same member, both will be stored,
        and in case of receiving a lower one, that we will send the higher one back to the sender.

        MM gives NODE permission to undo, NODE generates a message and then undoes it twice.
        Both messages should be kept and the lowest one should be undone.

        """
        node, other = yield self.create_nodes(2)
        yield node.send_identity(other)

        # MM grants undo permission to NODE
        mm_claimed_global_time = yield self._mm.claim_global_time()
        authorize = yield self._mm.create_authorize([(node.my_member, self._community.get_meta_message(u"full-sync-text"), u"undo")], mm_claimed_global_time)
        yield node.give_message(authorize, self._mm)
        yield other.give_message(authorize, self._mm)

        # create message
        message = yield node.create_full_sync_text("Should undo @%d" % 10, 10)

        # create undoes
        undo1 = yield node.create_undo_own(message, 11, 1)
        undo2 = yield node.create_undo_own(message, 12, 2)
        low_message, high_message = sorted([undo1, undo2], key=lambda message: message.packet)
        yield other.give_message(message, node)
        yield other.give_message(low_message, node)
        yield other.give_message(high_message, node)
        # OTHER should send the first message back when receiving
        # the second one (its "higher" than the one just received)
        undo_packets = []

        received_packets = yield node.receive_packets()
        for candidate, b in received_packets:
            self._logger.debug(candidate)
            self._logger.debug(type(b))
            self._logger.debug("%d", len(b))
            self._logger.debug("before %d", len(undo_packets))
            undo_packets.append(b)
            self._logger.debug("packets amount: %d", len(undo_packets))
            self._logger.debug("first undo %d", len(undo_packets[0]))
            self._logger.debug("%d", len(b))

            for x in undo_packets:
                self._logger.debug("loop%d", len(x))

        @inlineCallbacks
        def fetch_all_messages():
            other_rows = yield other._dispersy.database.stormdb.fetchall(u"SELECT * FROM sync")
            for row in other_rows:
                self._logger.debug("_______ %s", row)
        yield other.call(fetch_all_messages)

        self._logger.debug("%d", len(low_message.packet))

        self.assertEqual(undo_packets, [low_message.packet])

        # NODE should have both messages on the database and the lowest one should be undone by the highest.
        messages = yield other.fetch_messages((u"dispersy-undo-own",))
        self.assertEquals(len(messages), 2)
        yield other.assert_is_stored(low_message)
        yield other.assert_is_undone(high_message)
        yield other.assert_is_undone(high_message, undone_by=low_message)
        yield other.assert_is_undone(message, undone_by=low_message)

    @deferred(timeout=10)
    @inlineCallbacks
    def test_missing_message(self):
        """
        NODE generates a few messages without sending them to OTHER. Following, NODE undoes the
        messages and sends the undo messages to OTHER. OTHER must now use a dispersy-missing-message
        to request the messages that are about to be undone. The messages need to be processed and
        subsequently undone.
        """
        node, other = yield self.create_nodes(2)
        self.nodes.append(node)
        self.nodes.append(other)
        self.patch_send_packet_for_nodes()
        yield node.send_identity(other)

        # create messages
        messages = []
        for i in xrange(10):
            created_full_sync_text = yield node.create_full_sync_text("Should undo @%d" % i, i + 10)
            messages.append(created_full_sync_text)

        # undo all messages
        undoes = []
        for i, message in enumerate(messages):
            create_undo_own_message = yield node.create_undo_own(message, message.distribution.global_time + 100, i + 1)
            undoes.append(create_undo_own_message)

        # send undoes to OTHER
        yield other.give_messages(undoes, node)

        # receive the dispersy-missing-message messages
        global_times = [message.distribution.global_time for message in messages]
        global_time_requests = []

        received_messages = yield node.receive_messages(names=[u"dispersy-missing-message"])
        for _, message in received_messages:
            self.assertEqual(message.payload.member.public_key, node.my_member.public_key)
            global_time_requests.extend(message.payload.global_times)

        self.assertEqual(sorted(global_times), sorted(global_time_requests))

        # give all 'delayed' messages
        yield other.give_messages(messages, node)

        # check that they are in the database and ARE undone
        yield other.assert_is_undone(messages=messages)
        yield other.assert_is_stored(messages=undoes)

    @deferred(timeout=10)
    @inlineCallbacks
    def test_revoke_causing_undo(self):
        """
        SELF gives NODE permission to undo, OTHER created a message, NODE undoes the message, SELF
        revokes the undo permission AFTER the message was undone -> the message is re-done.
        """
        node, other = yield self.create_nodes(2)
        yield node.send_identity(other)

        # MM grants undo permission to NODE
        mm_claimed_global_time = yield self._mm.claim_global_time()
        authorize = yield self._mm.create_authorize([(node.my_member, self._community.get_meta_message(u"full-sync-text"), u"undo")], mm_claimed_global_time)
        yield node.give_message(authorize, self._mm)
        yield other.give_message(authorize, self._mm)

        # OTHER creates a message
        message = yield other.create_full_sync_text("will be undone", 42)
        yield other.give_message(message, other)
        yield other.assert_is_stored(message)

        # NODE undoes the message
        undo = yield node.create_undo_other(message, message.distribution.global_time + 1, 1)
        yield other.give_message(undo, node)
        yield other.assert_is_undone(message)
        yield other.assert_is_stored(undo)

        # SELF revoke undo permission from NODE, as the globaltime of the mm is lower than 42 the message needs to be done
        revoke = yield self._mm.create_revoke([(node.my_member, self._community.get_meta_message(u"full-sync-text"), u"undo")])
        yield other.give_message(revoke, self._mm)
        yield other.assert_is_stored(message)

    @deferred(timeout=10)
    @inlineCallbacks
    def test_revoke_causing_undo_permitted(self):
        """
        SELF gives NODE permission to undo, OTHER created a message, NODE undoes the message, SELF
        revokes the undo permission AFTER the message was undone -> the message is re-done.
        """
        node, other = yield self.create_nodes(2)
        yield node.send_identity(other)

        # MM grants permit permission to OTHER
        mm_claimed_global_time = yield self._mm.claim_global_time()
        authorize = yield self._mm.create_authorize([(other.my_member, self._community.get_meta_message(u"protected-full-sync-text"), u"permit")], mm_claimed_global_time)
        yield node.give_message(authorize, self._mm)
        yield other.give_message(authorize, self._mm)

        # MM grants undo permission to NODE
        mm_claimed_global_time = yield self._mm.claim_global_time()
        authorize = yield self._mm.create_authorize([(node.my_member, self._community.get_meta_message(u"protected-full-sync-text"), u"undo")], mm_claimed_global_time)
        yield node.give_message(authorize, self._mm)
        yield other.give_message(authorize, self._mm)

        # OTHER creates a message
        message = yield other.create_protected_full_sync_text("will be undone", 42)
        yield other.give_message(message, other)
        yield other.assert_is_stored(message)

        # NODE undoes the message
        undo = yield node.create_undo_other(message, message.distribution.global_time + 1, 1)
        yield other.give_message(undo, node)
        yield other.assert_is_undone(message)
        yield other.assert_is_stored(undo)

        # SELF revoke undo permission from NODE, as the globaltime of the mm is lower than 42 the message needs to be done
        revoke = yield self._mm.create_revoke([(node.my_member, self._community.get_meta_message(u"protected-full-sync-text"), u"undo")])
        yield other.give_message(revoke, self._mm)
        yield other.assert_is_stored(message)
