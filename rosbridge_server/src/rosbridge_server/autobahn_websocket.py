# Software License Agreement (BSD License)
#
# Copyright (c) 2012, Willow Garage, Inc.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#
#  * Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
#  * Redistributions in binary form must reproduce the above
#    copyright notice, this list of conditions and the following
#    disclaimer in the documentation and/or other materials provided
#    with the distribution.
#  * Neither the name of Willow Garage, Inc. nor the names of its
#    contributors may be used to endorse or promote products derived
#    from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
# FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
# COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
# ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

import sys
import threading
import traceback
import uuid
from collections import deque
from functools import wraps

import rospy
from autobahn.twisted.websocket import WebSocketServerProtocol
from locus_msgs.srv import LiveViewAuth, LiveViewAuthResponse
from rosbridge_library.rosbridge_protocol import RosbridgeProtocol
from rosbridge_library.util import bson, json
from twisted.internet import interfaces, reactor
from zope.interface import implementer


def _log_exception():
    """Log the most recent exception to ROS."""
    exc = traceback.format_exception(*sys.exc_info())
    rospy.logerr("".join(exc))


def log_exceptions(f):
    """Decorator for logging exceptions to ROS."""

    @wraps(f)
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except:
            _log_exception()
            raise

    return wrapper


class IncomingQueue(threading.Thread):
    """Decouples incoming messages from the Autobahn thread.

    This mitigates cases where outgoing messages are blocked by incoming,
    and vice versa.
    """

    def __init__(self, protocol):
        threading.Thread.__init__(self)
        self.daemon = True
        self.queue = deque()
        self.protocol = protocol

        self.cond = threading.Condition()
        self._finished = False

    def finish(self):
        """Clear the queue and do not accept further messages."""
        with self.cond:
            self._finished = True
            while len(self.queue) > 0:
                self.queue.popleft()
            self.cond.notify()

    def push(self, msg):
        with self.cond:
            self.queue.append(msg)
            self.cond.notify()

    def run(self):
        while True:
            with self.cond:
                if len(self.queue) == 0 and not self._finished:
                    self.cond.wait()

                if self._finished:
                    break

                msg = self.queue.popleft()

            self.protocol.incoming(msg)

        self.protocol.finish()


@implementer(interfaces.IPushProducer)
class OutgoingValve:
    """Allows the Autobahn transport to pause outgoing messages from rosbridge.

    The purpose of this valve is to connect backpressure from the WebSocket client
    back to the rosbridge protocol, which depends on backpressure for queueing.
    Without this flow control, rosbridge will happily keep writing messages to
    the WebSocket until the system runs out of memory.

    This valve is closed and opened automatically by the Twisted TCP server.
    In practice, Twisted should only close the valve when its userspace write buffer
    is full and it should only open the valve when that buffer is empty.

    When the valve is closed, the rosbridge protocol instance's outgoing writes
    must block until the valve is opened.
    """

    def __init__(self, proto):
        self._proto = proto
        self._valve = threading.Event()
        self._finished = False

    @log_exceptions
    def relay(self, message):
        self._valve.wait()
        if self._finished:
            return
        reactor.callFromThread(self._proto.outgoing, message)

    def pauseProducing(self):
        if not self._finished:
            self._valve.clear()

    def resumeProducing(self):
        self._valve.set()

    def stopProducing(self):
        self._finished = True
        self._valve.set()


class RosbridgeWebSocket(WebSocketServerProtocol):
    """
    A server implementation of the RosBridge WebSocket protocol.
    Note that this class is instantiated for each user, so for per-user details, we can store them on the instance.
    But if we want "global" state, we need to save it on `cls.`
    """

    client_id_seed = 0
    clients_connected = 0
    authenticate = False

    # The following are passed on to RosbridgeProtocol
    # defragmentation.py:
    fragment_timeout = 600  # seconds
    # protocol.py:
    delay_between_messages = 0  # seconds
    max_message_size = None  # bytes
    unregister_timeout = 10.0  # seconds
    bson_only_mode = False

    def onOpen(self):
        cls = self.__class__
        parameters = {
            "fragment_timeout": cls.fragment_timeout,
            "delay_between_messages": cls.delay_between_messages,
            "max_message_size": cls.max_message_size,
            "unregister_timeout": cls.unregister_timeout,
            "bson_only_mode": cls.bson_only_mode,
        }
        try:
            self.protocol = RosbridgeProtocol(cls.client_id_seed, parameters=parameters)
            self.incoming_queue = IncomingQueue(self.protocol)
            self.incoming_queue.start()
            producer = OutgoingValve(self)
            self.transport.registerProducer(producer, True)
            producer.resumeProducing()
            self.protocol.outgoing = producer.relay
            self.authenticated = False
            self.permissions = []
            cls.client_id_seed += 1
            cls.clients_connected += 1
            self.client_id = uuid.uuid4()
            self.peer = self.transport.getPeer().host
            if cls.client_manager:
                cls.client_manager.add_client(self.client_id, self.peer)

        except Exception as exc:
            rospy.logerr("Unable to accept incoming connection.  Reason: %s", str(exc))
        rospy.loginfo("Client connected.  %d clients total.", cls.clients_connected)
        if cls.authenticate:
            rospy.loginfo("Awaiting proper authentication...")

    def onMessage(self, message, binary):
        cls = self.__class__
        if cls.bson_only_mode:
            msg = bson.BSON(message).decode()
        else:
            msg = json.loads(message)

        if not binary:
            message = message.decode("utf-8")

        # Authenticate? This could be called even if user is authenticated. It resets auth state.
        if cls.authenticate and msg["op"] == "authenticate":
            self.authenticate(msg)
        else:
            self.incoming_queue.push(message)  # push the non-decoded message data.

    def outgoing(self, message):
        if type(message) == bson.BSON:
            binary = True
            message = bytes(message)
        elif type(message) == bytearray:
            binary = True
            message = bytes(message)
        else:
            binary = False
            message = message.encode("utf-8")

        self.sendMessage(message, binary)

    def onClose(self, was_clean, code, reason):
        if not hasattr(self, "protocol"):
            return  # Closed before connection was opened.
        cls = self.__class__
        cls.clients_connected -= 1

        if cls.client_manager:
            cls.client_manager.remove_client(self.client_id, self.peer)
        rospy.loginfo("Client disconnected. %d clients total.", cls.clients_connected)

        self.incoming_queue.finish()

    def authenticate(self, msg):
        # Reset auth state regardless of user being authenticated or not. This means that repeated `authenticate` ops
        # will re-authenticate.
        self.authenticated = False
        self.permissions = []

        # Call the service for auth with the username and password.
        auth_srv = rospy.ServiceProxy(rospy.get_param("auth_service_name"), LiveViewAuth)
        response = auth_srv(msg["username"], msg["password"])

        # An internal error. Handle it locally and close the connection. This needs to be fixed, not handled.
        if response.result == LiveViewAuthResponse.FAILURE:
            reason = f"Could not auth user: {msg['username']}. Service failed with message: {response.message}"
            message = json.dumps(
                {
                    "op": "authorization_response",
                    "username": msg["username"],
                    "message": reason,
                    "status": "failure",
                    "permissions": [],
                }
            )
            rospy.logerr(reason)
            self.sendMessage(message.encode("utf-8"), isBinary=False)
            self.sendClose()
            return

        # A 403-like error. Tell the client of this failure and then close connection.
        if response.result == LiveViewAuthResponse.INVALID_CREDENTIALS:
            reason = f"Invalid credentials for user: {msg['username']}. Reason: {response.message}"
            message = json.dumps(
                {
                    "op": "authorization_response",
                    "username": msg["username"],
                    "message": reason,
                    "status": "invalidCredentials",
                    "permissions": [],
                }
            )
            rospy.logwarn(reason)
            self.sendMessage(message.encode("utf-8"), isBinary=False)
            self.sendClose()
            return

        # Auth worked. Set RosBridge state for authed/permissions, and send a response.
        if response.result == LiveViewAuthResponse.SUCCESS:
            message = json.dumps(
                {
                    "op": "authorization_response",
                    "username": msg["username"],
                    "message": "",
                    "status": "success",
                    "permissions": response.permissions,
                }
            )
            self.authenticated = True
            self.permissions = response.permissions
            rospy.loginfo(f"Authenticated user: {msg['username']}")
            self.sendMessage(message.encode("utf-8"), isBinary=False)
