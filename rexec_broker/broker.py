import os
import pickle
import logging
import threading
from typing import Any, Dict

import zmq
import zmq.utils.monitor

from rexec_broker.auth import validate_token
from rexec_broker.frames import log_routing_envelope, split_envelope

EVENT_MAP = {}

def setup_event_map(event_map: list):
    logging.debug("Event names:")
    for name in dir(zmq):
        if name.startswith('EVENT_'):
            value = getattr(zmq, name)
            logging.debug(f"{name:21} : {value:4}")
            event_map[value] = name

def event_monitor(monitor_socket: zmq.Socket, socket_name: str) -> None:
    while monitor_socket.poll():
        evt: Dict[str, Any] = {}
        mon_evt = zmq.utils.monitor.recv_monitor_message(monitor_socket)
        evt.update(mon_evt)
        evt['description'] = EVENT_MAP[evt['event']]
        logging.debug(f"{socket_name} Event: {evt}")

        if evt['event'] == zmq.EVENT_MONITOR_STOPPED:
            break

    monitor_socket.close()
    logging.debug("event monitor thread done!")

class RExecBroker:
    def __init__(self, args):
        self.zmq_context = zmq.Context()
        
        self.frontend_zmq_addr = "tcp://*:" + args.client_port
        self.frontend_socket = self.zmq_context.socket(zmq.ROUTER)
        self.frontend_socket.bind(self.frontend_zmq_addr)

        self.backend_zmq_addr = "tcp://*:" + args.server_port
        self.backend_socket = self.zmq_context.socket(zmq.ROUTER)
        self.backend_socket.bind(self.backend_zmq_addr)
        self.backend_socket.setsockopt(zmq.ROUTER_MANDATORY, 1) # enable mandatory routing

        self.control_zmq_addr = "tcp://*:" + args.control_port
        self.control_socket = self.zmq_context.socket(zmq.REP)
        self.control_socket.bind(self.control_zmq_addr)

        self.auth_api_url = args.auth_api_url or os.environ.get("AUTH_API_URL")
        if not self.auth_api_url:
            raise RuntimeError("AUTH_API_URL is required to validate execution tokens.")

        self.debug = False
        if args.loglevel == logging.DEBUG:
            if zmq.zmq_version_info() > (4, 0):
                self.debug = True
                setup_event_map(EVENT_MAP)
                self.frontend_monitor = self.frontend_socket.get_monitor_socket()
                self.backend_monitor = self.backend_socket.get_monitor_socket()
                self.control_monitor = self.control_socket.get_monitor_socket()
            else:
                raise RuntimeError("monitoring in libzmq version < 4.0 is not supported")
            
    def _reply_error(self, envelope, message):
        """
        Send an error reply to a client.
        """
        if not envelope:
            logging.error("Cannot reply to client without routing envelope: %s", message)
            return
        payload = pickle.dumps({"error": message})
        self.frontend_socket.send_multipart(envelope + [b"", payload])
    
    def _proxy_loop(self):
        """
        Main loop for proxying messages between frontend(rexec client) and backend(rexec server) sockets.
        """
        poller = zmq.Poller()
        poller.register(self.frontend_socket, zmq.POLLIN)
        poller.register(self.backend_socket, zmq.POLLIN)
        poller.register(self.control_socket, zmq.POLLIN)

        while True:
            events = dict(poller.poll())

            # Handle control messages
            # ----------------------------------------------
            if self.control_socket in events:
                msg = self.control_socket.recv()
                logging.info("Control message received: %r", msg)
                self.control_socket.send(b"OK")
                if msg in (b"TERMINATE", b"STOP", b"QUIT"):
                    logging.info("Control requested broker shutdown")
                    break
            
            # Handle client request message
            # ----------------------------------------------
            if self.frontend_socket in events:
                frames = self.frontend_socket.recv_multipart()
                # client socket is ROUTER, so first frames are routing envelope
                # frames = Generated:[envelope(generated val for client conn), delimiter(b"")] + Received:body(user_token, pfn, pargs)]
                envelope, delimiter_index, body = split_envelope(frames)

                # Logging and validation
                if delimiter_index is None or len(body) < 3:
                    log_routing_envelope(
                        "Frontend->Backend",
                        frames,
                        self.frontend_zmq_addr,
                        self.backend_zmq_addr,
                    )
                    self._reply_error(envelope, "Invalid request framing.")
                    continue

                try:
                    # Extract and decode token from client message
                    token = body[0].decode("utf-8")
                except UnicodeDecodeError:
                    self._reply_error(envelope, "Token is not valid utf-8.")
                    continue
                
                # Validate token and extract user_id
                user_id = validate_token(self.auth_api_url, token)
                if not user_id:
                    self._reply_error(envelope, "Token validation failed.")
                    continue

                # Route to appropriate server based on user_id; Server identity is user_id encoded in utf-8
                server_id = user_id.encode("utf-8")
                # first frame(server_id) will not be delievered to server, it's used for routing(identify server) only
                # so only send (envelope + b"" + body[1:]) to server
                outbound = [server_id] + envelope + [b""] + body[1:]
                # Log routing info
                log_routing_envelope(
                    "Frontend->Backend",
                    frames,
                    self.frontend_zmq_addr,
                    self.backend_zmq_addr,
                    server_id=server_id,
                )
                try:
                    self.backend_socket.send_multipart(outbound) # route to server
                except zmq.ZMQError as exc:
                    logging.warning("Backend route failed for %s: %s", user_id, exc)
                    self._reply_error(envelope, f"Server not available for user {user_id}.")
            
            # Handle server response message
            # ----------------------------------------------
            if self.backend_socket in events:
                frames = self.backend_socket.recv_multipart()
                if not frames:
                    continue
                # server socket is ROUTER, so first frame is server identity
                # frames = Received:[server_id, envelope(client_id), delimiter(b""), body(pret)]
                server_id = frames[0]
                payload = frames[1:]
                log_routing_envelope(
                    "Backend->Frontend",
                    payload,
                    self.backend_zmq_addr,
                    self.frontend_zmq_addr,
                    server_id=server_id,
                )
                self.frontend_socket.send_multipart(payload) # route to client
            
    def run(self):
        try:
            logging.info(f"Proxy Starts...")
            if self.debug:
                frontend_monitor_thread = threading.Thread(target=event_monitor, args=(self.frontend_monitor,"Client Socket",))
                backend_monitor_thread = threading.Thread(target=event_monitor, args=(self.backend_monitor,"Server Socket",))
                control_monitor_thread = threading.Thread(target=event_monitor, args=(self.control_monitor,"Control Socket",))

                frontend_monitor_thread.start()
                backend_monitor_thread.start()
                control_monitor_thread.start()

            # zmq.proxy_steerable(self.frontend_socket, self.backend_socket, None, self.control_socket) # does not support ROUTER sockets
            self._proxy_loop()

        except KeyboardInterrupt:
            print("W: interrupt received, stopping broker...")

        finally:
            self.frontend_socket.close()
            self.backend_socket.close()
            self.control_socket.close()

            if self.debug:
                self.frontend_monitor.disable_monitor()
                self.backend_monitor.disable_monitor()
                self.control_monitor.disable_monitor()

            self.zmq_context.destroy()
