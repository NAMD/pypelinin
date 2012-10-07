# coding: utf-8

from zmq import Context, REQ, SUB, SUBSCRIBE, UNSUBSCRIBE


class Client(object):
    '''Base class to communicate with pypelinin's Manager

    Probably you don't want to use this class by hand since it does not
    implement Manager's protocol. Use one of the class that subclass `Client`,
    as `pypelinin.Broker` and `pypelinin.Pipeliner`.
    '''
    #TODO: validate all received data (types, keys etc.)
    #TODO: use some kind of encryption?

    def __init__(self):
        self.context = Context()
        self.api_address = None
        self.broadcast_address = None
        self._manager_api = None
        self._manager_broadcast = None

    def __del__(self):
        self.disconnect_api(silent=True)
        self.disconnect_broadcast(silent=True)

    def connect(self, api=None, broadcast=None):
        '''Connect to Manager's API and/or broadcast channel(s)

        API and broadcast addresses should be specified in this form:
        `tcp://ip-address-or-host:port`, like in `tcp://127.0.0.1:5555`.
        '''
        if api is broadcast is None:
            raise ValueError("At least one of the Manager's communication "
                             "channels (broadcast or API) need to be specified")
        else:
            if api is not None:
                self.api_address = api
                self._manager_api = self.context.socket(REQ)
                self._manager_api.connect(api)
                self._manager_api.linger = 0
            if broadcast is not None:
                self.broadcast_address = broadcast
                self._manager_broadcast = self.context.socket(SUB)
                self._manager_broadcast.connect(broadcast)
                self._manager_broadcast.linger = 0

    def send_api_request(self, data):
        '''Send an API request to Manager

        `data` needs to be a pickleable `dict`.
        '''
        if self._manager_api is None:
            raise RuntimeError("Not connected to Manager's API channel")
        else:
            return self._manager_api.send_json(data)

    def get_api_reply(self):
        '''Receive an API reply from Manager

        It'll hang if you didn't send a request (using `send_api_request`).
        The return data is a `dict`.
        '''
        if self._manager_api is None:
            raise RuntimeError("Not connected to Manager's API channel")
        else:
            return self._manager_api.recv_json()

    def api_poll(self, timeout=0):
        '''Poll API channel until `timeout` (in milliseconds)

        Return `True`/`False` if there is any to be received (or not). If it
        returns `True` so you can use `get_api_reply` and it won't hang.
        '''
        if self._manager_api is None:
            raise RuntimeError("Not connected to Manager's API channel")
        else:
            return self._manager_api.poll(timeout)

    def broadcast_subscribe(self, subscribe_to):
        '''Subscribe to a Manager's broadcast type

        `subscribe_to` needs to be a string.
        '''
        if self._manager_broadcast is None:
            raise RuntimeError("Not connected to Manager's broadcast channel")
        else:
            return self._manager_broadcast.setsockopt(SUBSCRIBE, subscribe_to)

    def broadcast_unsubscribe(self, unsubscribe_to):
        if self._manager_broadcast is None:
            raise RuntimeError("Not connected to Manager's broadcast channel")
        else:
            return self._manager_broadcast.setsockopt(UNSUBSCRIBE,
                                                      unsubscribe_to)

    def broadcast_poll(self, timeout=0):
        if self._manager_broadcast is None:
            raise RuntimeError("Not connected to Manager's broadcast channel")
        else:
            return self._manager_broadcast.poll(timeout)

    def broadcast_receive(self):
        if self._manager_broadcast is None:
            raise RuntimeError("Not connected to Manager's broadcast channel")
        else:
            return self._manager_broadcast.recv()

    def disconnect_api(self, silent=False):
        '''Disconnect from Manager's API channel

        Raise RuntimeError if not connected to API channel and `silent=False`
        '''
        if self._manager_api is None and not silent:
            raise RuntimeError("Not connected to Manager's API channel")
        elif self._manager_api is not None:
            self._manager_api.close()
            self._manager_api = None

    def disconnect_broadcast(self, silent=False):
        '''Disconnect from Manager's broadcast channel

        Raise RuntimeError if not connected to broadcast channel
        '''
        if self._manager_broadcast is None and not silent:
            raise RuntimeError("Not connected to Manager's broadcast channel")
        elif self._manager_broadcast is not None:
            self._manager_broadcast.close()
            self._manager_broadcast = None

    def disconnect(self, silent=False):
        '''Disconnect from both Manager's API and broadcast channels

        Raise RuntimeError if not connected to at least one of both channels
        '''
        if self._manager_broadcast is self._manager_api is None and not silent:
            raise RuntimeError("Not connected")
        else:
            self.disconnect_api(silent=True)
            self.disconnect_broadcast(silent=True)
