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

import re
import sys
import threading
import traceback
import uuid
from collections import defaultdict, deque
from functools import wraps
from multiprocessing.sharedctypes import Value
from typing import Tuple

import rospy
from autobahn.twisted.websocket import WebSocketServerProtocol
from locus_msgs.srv import GetLiveViewAuth, GetLiveViewAuthResponse
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


def parsePermission(permissionString: str) -> Tuple[str, str]:
    """Returns an op, resourcePath structure.
    This assumes that the permissionString is of format:  `op,resource/path.  Examples:
    - `subscribe,/robot_names`
    - `publish,/path/to/teleop`
    - `call_service,/explode_robot`
    """
    pattern = fr"""
    ^                                 # Must begin with
    (subscribe|publish|call_service)  # a specific operation
    ,                                 # with a comma separating
    ([a-z|A-Z|~|\/]                   # a valid ROS resource name that begins with a letter, tilde, or slash
    [0-9|a-z|A-Z|_|\/]+)              # and has any number of numbers, letters, underscores, and slashes
    $                                 # with nothing else after.
    """
    result = re.search(pattern, permissionString, re.VERBOSE)
    if not result:
        raise ValueError(f"permissionString: {permissionString} is not in the valid format.")
    return (result.group(1), result.group(2))


class RosbridgeWebSocket(WebSocketServerProtocol):
    """
    A server implementation of the RosBridge WebSocket protocol.
    Note that this class is instantiated for each user, so for per-user details, we can store them on the instance.
    But if we want "global" state, we need to save it on `cls.`
    """

    client_id_seed = 0
    clients_connected = 0
    authenticate = False  # Class-level flag: Do we attempt to authenticate at all?
    auth_service_name = None

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
            self.isAuthenticated = False
            self.username = None  # TODO: populate and clear.
            self.permissions = defaultdict(set)
            cls.client_id_seed += 1
            cls.clients_connected += 1
            self.client_id = uuid.uuid4()
            self.peer = self.transport.getPeer().host
            if cls.client_manager:
                cls.client_manager.add_client(self.client_id, self.peer)

        except Exception as exc:
            rospy.logerr("Unable to accept incoming connection.  Reason: %s", str(exc))
        rospy.loginfo("Client connected.  %d clients total.", cls.clients_connected)

    def onMessage(self, message, binary):
        cls = self.__class__

        if not binary:
            message = message.decode("utf-8")

        if cls.authenticate:
            self.onMessageWithAuth(message)
        else:
            self.incoming_queue.push(message)  # push the non-decoded message data.

    def onMessageWithAuth(self, message):
        cls = self.__class__

        # Decode message to get op/resource details.
        if cls.bson_only_mode:
            msg = bson.BSON(message).decode()
        else:
            msg = json.loads(message)

        if msg["op"] == "authenticate":
            if self.isAuthenticated:
                self.sendStatus("Cannot call op `authenticate` when user is already authenticated.", "error")
            else:
                self.authenticateUser(msg)
            return

        op = msg["op"]
        resourcePath = msg.get("topic", msg.get("service"))  # The resource path for pub/sub/callservice.

        # Do not require any permissions to unsubscribe from something.
        if self.hasPermission(op, resourcePath):
            self.incoming_queue.push(message)  # push the non-decoded message data.
        else:
            reason = f"{self.username} lacks permission to {op} to {resourcePath}"
            self.sendStatus(reason, "error")

    def sendStatus(self, message: str, level: str):
        msg = json.dumps(
            {
                "op": "status",
                "msg": message,
                "level": level,
            }
        )

        if level == "info":
            rospy.loginfo(message)
        elif level == "warning":
            rospy.logwarn(message)
        elif level == "error":
            rospy.logerr(message)
        else:
            raise ValueError("level must be info|warning|error.")

        self.outgoing(msg)

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

    def authenticateUser(self, msg):
        # Reset auth state regardless of user being authenticated or not. This means that repeated `authenticate` ops
        # will re-authenticate.
        self.isAuthenticated = False
        self.username = None
        self.permissions = defaultdict(set)

        # Call the service for auth with the username and password.
        if self.auth_service_name is None:
            raise RuntimeError("rosparam `auth_service_name` not set.")
        auth_srv = rospy.ServiceProxy(self.auth_service_name, GetLiveViewAuth)

        if "token" not in msg:
            self.sendStatus("Message is malformed. Must include `token`.", "error")
            return

        response = auth_srv(msg["token"])

        # An internal error. Handle it locally and close the connection. This needs to be fixed, not handled.
        if response.result == GetLiveViewAuthResponse.FAILURE:
            reason = f"Could not auth user: {msg['username']}. Service failed with message: {response.message}"
            self.sendStatus(reason, "error")
            self.sendClose()
            return

        # A 403-like error. Tell the client of this failure and then close connection.
        if response.result == GetLiveViewAuthResponse.INVALID_CREDENTIALS:
            reason = f"Invalid credentials. Reason: {response.message}"
            self.sendStatus(reason, "error")
            return

        # Auth worked. Set RosBridge state for authed/permissions, and send a response.
        if response.result == GetLiveViewAuthResponse.SUCCESS:
            message = json.dumps(
                {
                    "op": "authentication_response",
                    "username": response.username,
                    "msg": "",
                    "permissions": response.permissions,
                }
            )
            self.isAuthenticated = True
            self.username = response.username

            for p in response.permissions:
                op, permission = parsePermission(p)
                self.permissions[op].add(permission)

            self.permissions = {parsePermission(p) for p in response.permissions}

            permissionsString = "".join(sorted([f"\n  - {p[0]}: {p[1]}" for p in self.permissions]))
            rospy.loginfo(f"Authenticated user: {response.username} with permissions:{permissionsString}")
            self.outgoing(message)

        def hasPermission(self, op: str, resourcePath: str) -> bool:
            if op == "unsubscribe":
                return True

            # Walk all permissions for that operation to find a match. We use `endswith` because some resources might
            # begin with a robot id.  eg.  `/p3_123`,  `/r2_12345`, `v1000`. There is no well-defined schema we can
            # rely on, so we just compare if most/all of the remaining string is a known permission.
            # Note that this is of limited security risk given we control both sides of this.
            for permission in self.permissions[op]:
                if resourcePath.endswith(permission):
                    return True

            return False
