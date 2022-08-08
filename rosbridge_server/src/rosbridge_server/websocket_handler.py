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
from collections import defaultdict
from functools import partial, wraps
from typing import Optional, Tuple

import rospy
from locus_msgs.srv import GetLiveViewAuth, GetLiveViewAuthResponse
from rosbridge_library.rosbridge_protocol import RosbridgeProtocol
from rosbridge_library.util import bson, json
from tornado import version_info as tornado_version_info
from tornado.gen import BadYieldError, coroutine
from tornado.ioloop import IOLoop
from tornado.websocket import WebSocketClosedError, WebSocketHandler


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
        except Exception:
            _log_exception()
            raise

    return wrapper


def parsePermission(permissionString: str) -> Optional[Tuple[str, str]]:
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

    # Is not related to Websocket permissions.  Eg. might be a database permission.
    if not result:
        return None

    return (result.group(1), result.group(2))


class RosbridgeWebSocket(WebSocketHandler):
    client_id_seed = 0
    clients_connected = 0
    authenticate = False
    auth_service_name = None
    use_compression = False

    # The following are passed on to RosbridgeProtocol
    # defragmentation.py:
    fragment_timeout = 600  # seconds
    # protocol.py:
    delay_between_messages = 0  # seconds
    max_message_size = None  # bytes
    unregister_timeout = 10.0  # seconds
    bson_only_mode = False

    @log_exceptions
    def open(self):
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
            self.protocol.outgoing = self.send_message
            self.set_nodelay(True)
            self.authenticated = False
            self._write_lock = threading.RLock()
            cls.client_id_seed += 1
            cls.clients_connected += 1
            self.client_id = uuid.uuid4()
            if cls.client_manager:
                cls.client_manager.add_client(self.client_id, self.request.remote_ip)
        except Exception as exc:
            rospy.logerr("Unable to accept incoming connection.  Reason: %s", str(exc))
        rospy.loginfo("Client connected.  %d clients total.", cls.clients_connected)
        if cls.require_authentication:
            rospy.loginfo("Awaiting proper authentication...")
        else:
            rospy.loginfo("`require_authentication` is false. Client does not need to auth.")

    @classmethod
    def decode_message(cls, message):
        if cls.bson_only_mode:
            return bson.BSON(message).decode()
        else:
            return json.loads(message)

    @log_exceptions
    def on_message(self, message):
        """An incoming message will be handled in one of four cases. Three cases must be handled by the auth system:
        1. require_authentication is True
        2. the request is an authentication request (even if auth is not required, we handle it normally).
        3. the user is authenticated, so it should be handled as such.

        In the final case, all requests are just passed through. This covers when we do not require auth and users
        do not want to (or cannot because v22 FM doesn't support it).
        """
        cls = self.__class__
        msg = self.decode_message(message)

        if msg["op"] == "authenticate" or cls.require_authentication or self.is_authenticated:
            self.on_message_with_auth(message)
        else:
            self.protocol.incoming(message)

    @log_exceptions
    def on_close(self):
        cls = self.__class__
        cls.clients_connected -= 1
        self.protocol.finish()
        if cls.client_manager:
            cls.client_manager.remove_client(self.client_id, self.request.remote_ip)
        rospy.loginfo("Client disconnected. %d clients total.", cls.clients_connected)

    def send_message(self, message):
        if type(message) == bson.BSON:
            binary = True
        elif type(message) == bytearray:
            binary = True
            message = bytes(message)
        else:
            binary = False

        with self._write_lock:
            IOLoop.instance().add_callback(partial(self.prewrite_message, message, binary))

    @coroutine
    def prewrite_message(self, message, binary):
        # Use a try block because the log decorator doesn't cooperate with @coroutine.
        try:
            with self._write_lock:
                yield self.write_message(message, binary)
        except WebSocketClosedError:
            rospy.logwarn("WebSocketClosedError: Tried to write to a closed websocket")
            raise
        except BadYieldError:
            # Tornado <4.5.0 doesn't like its own yield and raises BadYieldError.
            # This does not affect functionality, so pass silently only in this case.
            if tornado_version_info < (4, 5, 0, 0):
                pass
            else:
                _log_exception()
                raise
        except Exception:
            _log_exception()
            raise

    @log_exceptions
    def check_origin(self, origin):
        return True

    @log_exceptions
    def get_compression_options(self):
        # If this method returns None (the default), compression will be disabled.
        # If it returns a dict (even an empty one), it will be enabled.
        cls = self.__class__

        if not cls.use_compression:
            return None

        return {}

    def on_message_with_auth(self, msg, message):

        if msg["op"] == "authenticate":
            if self.is_authenticated:
                self.send_status("Cannot call op `authenticate` when user is already authenticated.", "error")
            else:
                self.authenticate_user(msg)
            return

        op = msg["op"]
        resourcePath = msg.get("topic", msg.get("service"))  # The resource path for pub/sub/callservice.

        # Do not require any permissions to unsubscribe from something.
        if self.has_permission(op, resourcePath):
            self.incoming_queue.push(message)  # push the non-decoded message data.
        else:
            reason = f"{self.username} lacks permission to {op} to {resourcePath}"
            self.send_status(reason, "error")

    def send_status(self, message: str, level: str):
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

    def authenticate_user(self, msg):
        # Reset auth state regardless of user being authenticated or not. This means that repeated `authenticate` ops
        # will re-authenticate.
        self.is_authenticated = False
        self.username = None
        self.permissions = defaultdict(set)

        # Call the service for auth with the username and password.
        if self.auth_service_name is None:
            raise RuntimeError("rosparam `auth_service_name` not set.")
        auth_srv = rospy.ServiceProxy(self.auth_service_name, GetLiveViewAuth)

        if "token" not in msg:
            self.send_status("Message is malformed. Must include `token`.", "error")
            return

        response = auth_srv(msg["token"])

        # An internal error. Handle it locally and close the connection. This needs to be fixed, not handled.
        if response.result == GetLiveViewAuthResponse.FAILURE:
            reason = f"Could not auth user: {msg['username']}. Service failed with message: {response.message}"
            self.send_status(reason, "error")
            self.sendClose()
            return

        # A 403-like error. Tell the client of this failure and then close connection.
        if response.result == GetLiveViewAuthResponse.INVALID_CREDENTIALS:
            reason = f"Invalid credentials. Reason: {response.message}"
            self.send_status(reason, "error")
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
            self.is_authenticated = True
            self.username = response.username

            for p in response.permissions:
                parsedPermission = parsePermission(p)
                if parsedPermission is not None:
                    self.permissions[parsedPermission[0]].add(parsedPermission[1])

            permissionsString = "".join(sorted([f"\n  - {p[0]}: {p[1]}" for p in self.permissions]))
            rospy.loginfo(f"Authenticated user: {response.username} with permissions:{permissionsString}")
            self.outgoing(message)

    def has_permission(self, op: str, resourcePath: str) -> bool:
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
