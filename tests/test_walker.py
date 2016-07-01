from nose.twistedtools import deferred
from twisted.internet.defer import inlineCallbacks, returnValue

from .dispersytestclass import DispersyTestFunc


class TestWalker(DispersyTestFunc):

    @deferred(timeout=10)
    def test_one_walker(self): return self.check_walker([""])

    @deferred(timeout=10)
    def test_two_walker(self): return self.check_walker(["", ""])

    @deferred(timeout=10)
    def test_many_walker(self): return self.check_walker([""] * 22)

    @deferred(timeout=10)
    def test_one_t_walker(self): return self.check_walker(["t"])

    @deferred(timeout=10)
    def test_two_t_walker(self): return self.check_walker(["t", "t"])

    @deferred(timeout=10)
    def test_many_t_walker(self): return self.check_walker(["t"] * 22)

    @deferred(timeout=10)
    def test_two_mixed_walker_a(self): return self.check_walker(["", "t"])

    @deferred(timeout=10)
    def test_many_mixed_walker_a(self): return self.check_walker(["", "t"] * 11)

    @deferred(timeout=10)
    def test_two_mixed_walker_b(self): return self.check_walker(["t", ""])

    @deferred(timeout=10)
    def test_many_mixed_walker_b(self): return self.check_walker(["t", ""] * 11)

    @inlineCallbacks
    def create_others(self, all_flags):
        assert isinstance(all_flags, list)
        assert all(isinstance(flags, str) for flags in all_flags)

        nodes = []
        for flags in all_flags:
            node, = yield self.create_nodes(tunnel="t" in flags)
            nodes.append(node)

        returnValue(nodes)

    @inlineCallbacks
    def check_walker(self, all_flags):
        """
        All nodes will perform a introduction request to SELF in one batch.
        """
        assert isinstance(all_flags, list)
        assert all(isinstance(flags, str) for flags in all_flags)

        nodes = yield self.create_others(all_flags)

        # create all requests
        requests = []
        for identifier, node in enumerate(nodes, 1):
            created_introduction_request = yield node.create_introduction_request(self._mm.my_candidate,
                                                              node.lan_address,
                                                              node.wan_address,
                                                              True,
                                                              u"unknown",
                                                              None,
                                                              identifier,
                                                              42)
            requests.append(created_introduction_request)

        # give all requests in one batch to dispersy
        incoming_packets = yield self._dispersy.on_incoming_packets
        encoded_messages= []
        for node, request in zip(nodes, requests):
            encoded_message = yield node.encode_message(request)
            encoded_messages.append((node.my_candidate, encoded_message))
        yield self._mm.call(incoming_packets, encoded_messages)

        is_tunnelled_map = dict([(node.lan_address, node.tunnel) for node in nodes])
        num_tunnelled_nodes = len([node for node in nodes if node.tunnel])
        num_non_tunnelled_nodes = len([node for node in nodes if not node.tunnel])

        for node in nodes:
            received_message = yield node.receive_message()
            _, response = received_message.next()

            # MM must not introduce NODE to itself
            self.assertNotEquals(response.payload.lan_introduction_address, node.lan_address)
            self.assertNotEquals(response.payload.wan_introduction_address, node.wan_address)

            if node.tunnel:
                if num_tunnelled_nodes + num_non_tunnelled_nodes > 1:
                    self.assertNotEquals(response.payload.lan_introduction_address, ("0.0.0.0", 0))
                    self.assertNotEquals(response.payload.wan_introduction_address, ("0.0.0.0", 0))

                    # it must be any known node
                    self.assertIn(response.payload.lan_introduction_address, is_tunnelled_map)

            else:
                # NODE is -not- behind a tunnel, MM can only introduce non-tunnelled nodes to NODE.  This is because
                # only non-tunnelled (StandaloneEndpoint) nodes can handle incoming messages -without- the FFFFFFFF
                # prefix.

                if num_non_tunnelled_nodes > 1:
                    self.assertNotEquals(response.payload.lan_introduction_address, ("0.0.0.0", 0))
                    self.assertNotEquals(response.payload.wan_introduction_address, ("0.0.0.0", 0))

                    # it may only be non-tunnelled
                    self.assertFalse(is_tunnelled_map[response.payload.lan_introduction_address], response.payload.lan_introduction_address)
