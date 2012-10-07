# coding: utf-8

import time
import unittest
from zmq import Context, REP, PUB
from pypelinin import Client


TIMEOUT = 150
API_ADDRESS = 'tcp://127.0.0.1:5555'
API_BIND_ADDRESS = 'tcp://*:5555' # WTF zmq? Why can't I use address above?
BROADCAST_ADDRESS = 'tcp://127.0.0.1:5556'
BROADCAST_BIND_ADDRESS = 'tcp://*:5556' # WTF zmq? Why can't I use address above?

class TestClient(unittest.TestCase):
    def setUp(self):
        self.context = Context()
        self.start_manager_sockets()

    def tearDown(self):
        self.close_sockets()
        self.context.term()

    def start_manager_sockets(self):
        self.api = self.context.socket(REP)
        self.broadcast = self.context.socket(PUB)
        self.api.bind(API_BIND_ADDRESS)
        self.broadcast.bind(BROADCAST_BIND_ADDRESS)

    def close_sockets(self):
        self.api.close()
        self.broadcast.close()

    def test_connect_raises_ValueError_when_no_communication_channel_is_specified(self):
        client = Client()
        with self.assertRaises(ValueError):
            client.connect()

    def test_api_methods_should_raise_RuntimeError_if_not_connected_to_api(self):
        client = Client()
        client.connect(broadcast=BROADCAST_ADDRESS)
        with self.assertRaises(RuntimeError):
            client.send_api_request({'command': 'get configuration'})
        with self.assertRaises(RuntimeError):
            client.get_api_reply()
        with self.assertRaises(RuntimeError):
            client.api_poll(timeout=1) # milliseconds
        with self.assertRaises(RuntimeError):
            client.disconnect_api()

    def test_broadcast_methods_should_raise_RuntimeError_if_not_connected_to_broadcast(self):
        client = Client()
        client.connect(api=API_ADDRESS)
        with self.assertRaises(RuntimeError):
            client.broadcast_subscribe('42')
        with self.assertRaises(RuntimeError):
            client.broadcast_unsubscribe('42')
        with self.assertRaises(RuntimeError):
            client.broadcast_poll(timeout=1) # milliseconds
        with self.assertRaises(RuntimeError):
            client.broadcast_receive()
        with self.assertRaises(RuntimeError):
            client.disconnect_broadcast()

    def test_send_api_request(self):
        client = Client()
        client.connect(api=API_ADDRESS)
        client.send_api_request({'command': 'get configuration'})
        if not self.api.poll(TIMEOUT):
            self.fail('Timeout wainting for API command')
        message = self.api.recv_json()
        self.assertEqual(message, {'command': 'get configuration'})

    def test_get_api_reply(self):
        client = Client()
        client.connect(api=API_ADDRESS)
        client.send_api_request({'command': 'get configuration'})
        if not self.api.poll(TIMEOUT):
            self.fail('Timeout wainting for API command')
        self.api.recv_json()
        self.api.send_json({'configuration': 'spam eggs ham'})
        message = client.get_api_reply() # what if it hangs?
        self.assertEqual(message, {'configuration': 'spam eggs ham'})

    def test_api_poll(self, timeout=0):
        client = Client()
        client.connect(api=API_ADDRESS)
        client.send_api_request({'command': 'get configuration'})
        if not self.api.poll(TIMEOUT):
            self.fail('Timeout waiting for API message')
        self.api.recv_json()
        start_time = time.time()
        result = client.api_poll(TIMEOUT)
        end_time = time.time()
        self.assertFalse(result)
        # there is no message, should wait for the entire TIMEOUT
        total_time = (end_time - start_time) * 1000 # milliseconds
        self.assertTrue(TIMEOUT <= total_time <= 1.1 * TIMEOUT)

        self.api.send_json({'configuration': 'spam eggs ham'})
        start_time = time.time()
        result = client.api_poll(TIMEOUT)
        end_time = time.time()
        self.assertTrue(result)
        # poll should return almost immediatly (there is a message)
        total_time = (end_time - start_time) * 1000 # milliseconds
        self.assertTrue(total_time < TIMEOUT)

    def test_broadcast_subscribe_poll_and_receive(self):
        client = Client()
        client.connect(broadcast=BROADCAST_ADDRESS)
        client.broadcast_subscribe('spam')
        time.sleep(TIMEOUT / 1000.0) # wait for subscribe to take effect

        self.broadcast.send('spam eggs ham')
        start_time = time.time()
        poll_result = client.broadcast_poll(TIMEOUT)
        end_time = time.time()
        self.assertTrue(poll_result)
        total_time = (end_time - start_time) * 1000
        self.assertTrue(total_time < TIMEOUT)
        message = client.broadcast_receive() # what if it hangs?
        self.assertEqual(message, 'spam eggs ham')

        self.broadcast.send('eggs ham')
        start_time = time.time()
        poll_result = client.broadcast_poll(TIMEOUT)
        end_time = time.time()
        self.assertFalse(poll_result)
        total_time = (end_time - start_time) * 1000
        self.assertTrue(TIMEOUT <= total_time <= 1.1 * TIMEOUT)

    def test_broadcast_unsubscribe(self):
        client = Client()
        client.connect(broadcast=BROADCAST_ADDRESS)
        client.broadcast_subscribe('spam')
        time.sleep(TIMEOUT / 1000.0) # wait for subscribe to take effect

        self.broadcast.send('spam eggs ham')
        self.assertTrue(client.broadcast_poll(TIMEOUT))
        self.assertEqual(client.broadcast_receive(), 'spam eggs ham')

        client.broadcast_unsubscribe('spam')
        self.broadcast.send('spam eggs ham')
        self.assertFalse(client.broadcast_poll(TIMEOUT))

    def test_disconnect(self):
        client = Client()
        client.connect(api=API_ADDRESS, broadcast=BROADCAST_ADDRESS)

        #connected we can communicate...
        client.send_api_request({'command': 'get configuration'})
        self.assertTrue(self.api.poll(TIMEOUT))
        self.api.recv_json()
        self.api.send_json({'command': 'ok'})
        self.assertTrue(client.api_poll(TIMEOUT))
        self.assertEqual(client.get_api_reply(), {'command': 'ok'})
        client.broadcast_subscribe('spam')
        time.sleep(TIMEOUT / 1000.0) # wait for subscribe to take effect
        self.broadcast.send('spam eggs ham')
        self.assertTrue(client.broadcast_poll(TIMEOUT))
        self.assertEqual(client.broadcast_receive(), 'spam eggs ham')

        #disconnected not!
        client.disconnect_api()
        with self.assertRaises(RuntimeError):
            client.send_api_request({'command': 'get configuration'})
        with self.assertRaises(RuntimeError):
            client.api_poll(TIMEOUT)
        with self.assertRaises(RuntimeError):
            client.get_api_reply()
        client.broadcast_subscribe('spam')
        time.sleep(TIMEOUT / 1000.0) # wait for subscribe to take effect
        self.broadcast.send('spam eggs ham')
        self.assertTrue(client.broadcast_poll(TIMEOUT))
        self.assertEqual(client.broadcast_receive(), 'spam eggs ham')
        client.disconnect_broadcast()
        with self.assertRaises(RuntimeError):
            client.broadcast_subscribe('spam')
        with self.assertRaises(RuntimeError):
            client.broadcast_poll(TIMEOUT)
        with self.assertRaises(RuntimeError):
            client.broadcast_receive()

        #connect again...
        client.connect(api=API_ADDRESS, broadcast=BROADCAST_ADDRESS)
        client.send_api_request({'command': 'get configuration'})
        self.assertTrue(self.api.poll(TIMEOUT))
        self.api.recv_json()
        self.api.send_json({'command': 'ok'})
        self.assertTrue(client.api_poll(TIMEOUT))
        self.assertEqual(client.get_api_reply(), {'command': 'ok'})
        client.broadcast_subscribe('spam')
        time.sleep(TIMEOUT / 1000.0) # wait for subscribe to take effect
        self.broadcast.send('spam eggs ham')
        self.assertTrue(client.broadcast_poll(TIMEOUT))
        self.assertEqual(client.broadcast_receive(), 'spam eggs ham')

        #disconnected everything
        client.disconnect()
        with self.assertRaises(RuntimeError):
            client.send_api_request({'command': 'get configuration'})
        with self.assertRaises(RuntimeError):
            client.api_poll(TIMEOUT)
        with self.assertRaises(RuntimeError):
            client.get_api_reply()
        with self.assertRaises(RuntimeError):
            client.broadcast_subscribe('spam')
        with self.assertRaises(RuntimeError):
            client.broadcast_poll(TIMEOUT)
        with self.assertRaises(RuntimeError):
            client.broadcast_receive()
        with self.assertRaises(RuntimeError):
            client.disconnect()
        # Should not raises:
        client.disconnect(silent=True)
        client.disconnect_api(silent=True)
        client.disconnect_broadcast(silent=True)

    #TODO: should we test linger?
