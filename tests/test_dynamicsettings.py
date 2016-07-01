from nose.twistedtools import deferred, reactor
from twisted.internet.defer import inlineCallbacks
from twisted.internet.task import deferLater

from ..resolution import PublicResolution, LinearResolution
from .dispersytestclass import DispersyTestFunc


class TestDynamicSettings(DispersyTestFunc):

    @deferred(timeout=10)
    @inlineCallbacks
    def test_default_resolution(self):
        """
        Ensure that the default resolution policy is used first.
        """
        other, = yield self.create_nodes(1)

        meta = self._community.get_meta_message(u"dynamic-resolution-text")

        # check default policy
        policy, proof = self._mm.get_resolution_policy(meta, self._community.global_time)
        self.assertIsInstance(policy, PublicResolution)
        self.assertEqual(proof, [])

        # NODE creates a message (should allow, because the default policy is PublicResolution)
        message = yield self._mm.create_dynamic_resolution_text("Message #%d" % 10, policy.implement(), 10)
        yield other.give_message(message, self._mm)

        yield other.assert_is_stored(message)

    @deferred(timeout=10)
    @inlineCallbacks
    def test_change_resolution(self):
        """
        Change the resolution policy from default to linear.
        """
        node, other = yield self.create_nodes(2)
        yield other.send_identity(node)

        meta = node._community.get_meta_message(u"dynamic-resolution-text")
        linear = meta.resolution.policies[1]

        # check default policy
        public_policy, _ = self._mm.get_resolution_policy(meta, self._community.global_time)
        self.assertIsInstance(public_policy, PublicResolution)

        # change and check policy
        message = yield self._mm.create_dynamic_settings([(meta, linear)], 42)
        yield self._mm.give_message(message, self._mm)
        yield node.give_message(message, self._mm)
        yield other.give_message(message, self._mm)

        linear_policy, proof = node.get_resolution_policy(meta, 43)
        self.assertIsInstance(linear_policy, LinearResolution)
        self.assertEqual(proof[0].distribution.global_time, message.distribution.global_time)

        # NODE creates a message (should allow), linear policy takes effect at globaltime + 1
        message = yield node.create_dynamic_resolution_text("Message #%d" % 42, public_policy.implement(), 42)
        yield other.give_message(message, node)
        yield other.assert_is_stored(message)

        # NODE creates another message (should drop), linear policy in effect
        message = yield node.create_dynamic_resolution_text("Message #%d" % 43, public_policy.implement(), 43)
        yield other.give_message(message, node)
        yield other.assert_not_stored(message)

        # NODE creates another message, correct policy (should drop), no permissions
        message = yield node.create_dynamic_resolution_text("Message #%d" % 44, linear_policy.implement(), 44)
        yield other.give_message(message, node)
        yield other.assert_not_stored(message)

    @deferred(timeout=10)
    @inlineCallbacks
    def test_change_resolution_undo(self):
        """
        Change the resolution policy from default to linear, the messages already accepted should be
        undone
        """
        def check_policy(time_low, time_high, meta, policyclass):
            for global_time in range(time_low, time_high):
                policy, _ = other.get_resolution_policy(meta, global_time)
                self.assertIsInstance(policy, policyclass)

        node, other = yield self.create_nodes(2)
        yield other.send_identity(node)

        meta = self._community.get_meta_message(u"dynamic-resolution-text")
        public = meta.resolution.policies[0]
        linear = meta.resolution.policies[1]

        # create policy change, but do not yet process
        policy_linear = yield self._mm.create_dynamic_settings([(meta, linear)], 11)  # hence the linear policy starts at 12
        policy_public = yield self._mm.create_dynamic_settings([(meta, public)], 21)  # hence the public policy starts at 22

        # because above policy changes were not applied (i.e. update=False) everything is still
        # PublicResolution without any proof
        check_policy(1, 32, meta, PublicResolution)

        # NODE creates a message (should allow)
        meta = node._community.get_meta_message(u"dynamic-resolution-text")
        public = meta.resolution.policies[0]

        tmessage = yield node.create_dynamic_resolution_text("Message #%d" % 25, public.implement(), 25)
        yield other.give_message(tmessage, node)
        yield other.assert_is_stored(tmessage)

        # process the policy change
        yield other.give_message(policy_linear, self._mm)
        check_policy(1, 12, meta, PublicResolution)
        check_policy(12, 32, meta, LinearResolution)

        # policy change should have undone the tmessage
        yield other.assert_is_undone(tmessage)

        # process the policy change
        yield other.give_message(policy_public, self._mm)

        check_policy(1, 12, meta, PublicResolution)
        check_policy(12, 22, meta, LinearResolution)
        check_policy(22, 32, meta, PublicResolution)

        # policy change should have redone the tmessage
        yield other.assert_is_stored(tmessage)

    @deferred(timeout=10)
    @inlineCallbacks
    def test_change_resolution_reject(self):
        """
        Change the resolution policy from default to linear and back, to see if other requests the proof
        """
        def check_policy(time_low, time_high, meta, policyclass):
            for global_time in range(time_low, time_high):
                policy, _ = other.get_resolution_policy(meta, global_time)
                self.assertIsInstance(policy, policyclass)

        node, other = yield self.create_nodes(2)
        yield other.send_identity(node)

        meta = self._community.get_meta_message(u"dynamic-resolution-text")
        public = meta.resolution.policies[0]
        linear = meta.resolution.policies[1]

        # create policy change, but do not yet process
        policy_linear = yield self._mm.create_dynamic_settings([(meta, linear)], 11)  # hence the linear policy starts at 12
        policy_public = yield self._mm.create_dynamic_settings([(meta, public)], 21)  # hence the public policy starts at 22

        # because above policy changes were not applied (i.e. update=False) everything is still
        # PublicResolution without any proof
        check_policy(1, 32, meta, PublicResolution)

        # process the policy change
        yield other.give_message(policy_linear, self._mm)
        check_policy(1, 12, meta, PublicResolution)
        check_policy(12, 32, meta, LinearResolution)

        # NODE creates a message (should allow)
        meta = node._community.get_meta_message(u"dynamic-resolution-text")
        public = meta.resolution.policies[0]

        tmessage = yield node.create_dynamic_resolution_text("Message #%d" % 25, public.implement(), 25)
        yield other.give_message(tmessage, node)

        received_message = yield node.receive_message(names=[u"dispersy-missing-proof"])
        _, message = received_message.next()
        yield other.give_message(policy_public, self._mm)
        yield other.assert_is_stored(tmessage)

    @deferred(timeout=10)
    @inlineCallbacks
    def test_change_resolution_send_proof(self):
        """
        Change the resolution policy from default to linear and back, to see if other sends the proofs
        """
        def check_policy(time_low, time_high, meta, policyclass):
            for global_time in range(time_low, time_high):
                policy, _ = other.get_resolution_policy(meta, global_time)
                self.assertIsInstance(policy, policyclass)

        node, other = yield self.create_nodes(2)
        yield other.send_identity(node)

        meta = self._community.get_meta_message(u"dynamic-resolution-text")
        public = meta.resolution.policies[0]
        linear = meta.resolution.policies[1]

        # create policy change, but do not yet process
        policy_linear = yield self._mm.create_dynamic_settings([(meta, linear)], 11)  # hence the linear policy starts at 12
        policy_public = yield self._mm.create_dynamic_settings([(meta, public)], 21)  # hence the public policy starts at 22

        # process both policy changes
        yield other.give_message(policy_linear, self._mm)
        yield other.give_message(policy_public, self._mm)

        check_policy(1, 12, meta, PublicResolution)
        check_policy(12, 22, meta, LinearResolution)
        check_policy(22, 32, meta, PublicResolution)

        # NODE creates a message (should reject)
        meta = node._community.get_meta_message(u"dynamic-resolution-text")
        public = meta.resolution.policies[0]

        tmessage = yield node.create_dynamic_resolution_text("Message #%d" % 12, public.implement(), 12)
        yield other.give_message(tmessage, node)

        received_message = yield node.receive_message(names=[u"dispersy-dynamic-settings"])
        _, message = received_message.next()
        assert message
