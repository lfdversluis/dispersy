from nose.twistedtools import deferred, reactor
from twisted.internet.defer import inlineCallbacks
from twisted.internet.task import deferLater

from .dispersytestclass import DispersyTestFunc


class TestDoubleSign(DispersyTestFunc):

    @deferred(timeout=10)
    @inlineCallbacks
    def test_no_response_from_node(self):
        """
        OTHER will request a signature from NODE. NODE will ignore this request and SELF should get
        a timeout on the signature request after a few seconds.
        """
        container = {"timeout": 0}
        node, other = yield self.create_nodes(2)
        yield other.send_identity(node)

        def on_response(request, response, modified):
            self.assertIsNone(response)
            container["timeout"] += 1
            return False

        message = yield other.create_double_signed_text(node.my_pub_member, "Allow=True")
        yield other.call(other._community.create_signature_request, node.my_candidate, message, on_response, timeout=1.0)

        yield deferLater(reactor, 1.5, lambda: None)

        self.assertEqual(container["timeout"], 1)

    @deferred(timeout=10)
    @inlineCallbacks
    def test_response_from_node(self):
        """
        NODE will request a signature from OTHER.
        NODE will receive the response signed by OTHER.
        """
        container = {"response": 0}

        node, other = yield self.create_nodes(2)
        yield other.send_identity(node)

        def on_response(request, response, modified):
            self.assertIsNotNone(response)

            self.assertEqual(container["response"], 0)
            mid_signatures = dict([(member.mid, signature) for signature, member in response.authentication.signed_members])
            # It should be signed by OTHER
            self.assertNotEqual(mid_signatures[other.my_member.mid], '')

            # AND it should be signed by NODE as the payload did not change
            self.assertNotEqual(mid_signatures[node.my_member.mid], '')

            # is_signed should be True, as it is signed by both parties
            self.assertTrue(response.authentication.is_signed)
            self.assertFalse(modified)
            container["response"] += 1
            return False

        # NODE creates the unsigned request and sends it to OTHER
        message = yield node.create_double_signed_text(other.my_pub_member, "Allow=True")
        yield node.call(node._community.create_signature_request, other.my_candidate, message, on_response, timeout=1.0)

        # OTHER receives the request
        received_message = yield other.receive_message(names=[u"dispersy-signature-request"])
        _, message = received_message.next()
        submsg = message.payload.message

        second_signature_offset = len(submsg.packet) - other.my_member.signature_length
        first_signature_offset = second_signature_offset - node.my_member.signature_length
        self.assertNotEqual(submsg.packet[first_signature_offset:second_signature_offset], "\x00" * node.my_member.signature_length, "The first signature MUST NOT BE 0x00's.")
        self.assertEqual(submsg.packet[second_signature_offset:], "\x00" * other.my_member.signature_length, "The second signature MUST BE 0x00's.")

        # reply sent by OTHER is ok, give it to NODE to process it
        yield other.give_message(message, node)
        yield node.process_packets()

        yield deferLater(reactor, 1.5, lambda: None)

        self.assertEqual(container["response"], 1)

    @deferred(timeout=10)
    @inlineCallbacks
    def test_modified_response_from_node(self):
        """
        NODE will request a signature from OTHER.
        NODE will receive the response signed by OTHER.
        """
        container = {"response": 0}

        node, other = yield self.create_nodes(2)
        yield other.send_identity(node)

        def on_response(request, response, modified):
            self.assertIsNotNone(response)

            self.assertEqual(container["response"], 0)
            mid_signatures = dict([(member.mid, signature) for signature, member in response.authentication.signed_members])

            # It should be signed by OTHER
            self.assertNotEqual(mid_signatures[other.my_member.mid], '')

            # BUT it should not be signed by NODE yet, because the payload changed
            self.assertEqual(mid_signatures[node.my_member.mid], '')

            # is_signed should be False, as it is not yet signed by both parties
            self.assertFalse(response.authentication.is_signed)
            self.assertTrue(modified)
            self.assertEqual(response.payload.text, "MODIFIED")

            container["response"] += 1
            return False

        # NODE creates the unsigned request and sends it to OTHER
        message = yield node.create_double_signed_text(other.my_pub_member, "Allow=Modify")
        yield node.call(node._community.create_signature_request, other.my_candidate, message, on_response, timeout=1.0)

        # OTHER receives the request
        received_message = yield other.receive_message(names=[u"dispersy-signature-request"])
        _, message = received_message.next()
        submsg = message.payload.message

        second_signature_offset = len(submsg.packet) - other.my_member.signature_length
        first_signature_offset = second_signature_offset - node.my_member.signature_length
        self.assertNotEqual(submsg.packet[first_signature_offset:second_signature_offset], "\x00" * node.my_member.signature_length, "The first signature MUST NOT BE 0x00's.")
        self.assertEqual(submsg.packet[second_signature_offset:], "\x00" * other.my_member.signature_length, "The second signature MUST BE 0x00's.")

        # reply sent by OTHER is ok, give it to NODE to process it
        yield other.give_message(message, node)
        yield node.process_packets()

        yield deferLater(reactor, 1.5, lambda: None)

        self.assertEqual(container["response"], 1)

    @deferred(timeout=10)
    @inlineCallbacks
    def test_append_response_from_node(self):
        """
        NODE will request a signature from OTHER.
        NODE will receive the response signed by OTHER.
        """
        container = {"response": 0}

        node, other = yield self.create_nodes(2)
        yield other.send_identity(node)


        def on_response(request, response, modified):
            self.assertNotEqual(response, None)

            self.assertEqual(container["response"], 0)
            mid_signatures = dict([(member.mid, signature) for signature, member in response.authentication.signed_members])
            # It should be signed by OTHER
            self.assertNotEqual(mid_signatures[other.my_member.mid], '')

            # AND it should be signed by NODE as the payload did not change
            self.assertNotEqual(mid_signatures[node.my_member.mid], '')

            # is_signed should be True, as it is signed by both parties
            self.assertTrue(response.authentication.is_signed)

            # but it is modified by bob, hence modified is True
            self.assertTrue(modified)
            container["response"] += 1
            return False

        # NODE creates the unsigned request and sends it to OTHER
        message = yield node.create_double_signed_split_payload_text(other.my_pub_member, "Allow=Append,")
        yield node.call(node._community.create_signature_request, other.my_candidate, message, on_response, timeout=1.0)

        # OTHER receives the request
        received_message = yield other.receive_message(names=[u"dispersy-signature-request"])
        _, message = received_message.next()
        submsg = message.payload.message

        second_signature_offset = len(submsg.packet) - other.my_member.signature_length
        first_signature_offset = second_signature_offset - node.my_member.signature_length
        self.assertNotEqual(submsg.packet[first_signature_offset:second_signature_offset], "\x00" * node.my_member.signature_length, "The first signature MUST NOT BE 0x00's.")
        self.assertEqual(submsg.packet[second_signature_offset:], "\x00" * other.my_member.signature_length, "The second signature MUST BE 0x00's.")

        # reply sent by OTHER is ok, give it to NODE to process it
        yield other.give_message(message, node)
        yield node.process_packets()

        yield deferLater(reactor, 1.5, lambda: None)

        self.assertEqual(container["response"], 1)
