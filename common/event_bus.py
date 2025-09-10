from __future__ import annotations
import json
import os
import threading
import socket
from typing import Callable, Optional, List, Tuple
import paho.mqtt.client as mqtt
from common.logging_config import logger

class EventBus:
    """Generic MQTT event bus with handler dispatch. Reusable across cloud, fog, or edge nodes."""

    def __init__(self, host: str, port: int, log_prefix: str = "[agent]"):
        self.host = host
        self.port = port
        self.log_prefix = log_prefix

        fog = os.getenv("FOG_NAME", None)
        if fog is not None:
            client_id = f"agent-{fog}-{socket.gethostname()}"
        else:
            client_id = f"agent-{socket.gethostname()}"
        self.client = mqtt.Client(client_id=client_id, clean_session=False)
        self.client.enable_logger()

        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._dispatch
        self.client.reconnect_delay_set(min_delay=1, max_delay=30)

        self._connected_event = threading.Event()
        self._on_message: Optional[Callable[[str, dict], None]] = None

        # Track subscriptions so we can re-subscribe after reconnects.
        self._subs: List[Tuple[str, int]] = []

    def start(self, on_message: Callable[[str, dict], None]) -> None:
        self._on_message = on_message
        logger.info(f"[Agent]: {self.log_prefix} MQTT connecting to {self.host}:{self.port}")
        # Small keepalive to detect broker restarts quickly.
        self.client.connect(self.host, self.port, keepalive=30)
        threading.Thread(target=self.client.loop_forever, daemon=True).start()
        if not self._connected_event.wait(timeout=5.0):
            raise RuntimeError(f"{self.log_prefix} MQTT connect timeout to {self.host}:{self.port}")

    def subscribe_many(self, topics: List[str], qos: int = 1) -> None:
        for t in topics:
            self._subs.append((t, qos))
            self.client.subscribe(t, qos=qos)
            logger.info("%s subscribed topic=%s qos=%s", self.log_prefix, t, qos)

    def publish(self, topic: str, payload: dict, qos: int = 1, retain: bool = False) -> None:
        self.client.publish(topic, json.dumps(payload), qos=qos, retain=retain)
        logger.debug("%s published topic=%s qos=%s retain=%s", self.log_prefix, topic, qos, retain)


    # --- callbacks ---
    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logger.info("%s MQTT connected to %s:%s (session present=%s)",
                        self.log_prefix, self.host, self.port, bool(flags.get("session present", 0)))
            self._connected_event.set()
            # Re-subscribe to all topics after any (re)connect.
            if self._subs:
                for t, q in self._subs:
                    client.subscribe(t, qos=q)
                logger.info("%s re-subscribed to %d topics", self.log_prefix, len(self._subs))
        else:
            logger.error("%s MQTT connect failed rc=%s", self.log_prefix, rc)

    def _on_disconnect(self, client, userdata, rc):
        if rc != 0:
            logger.warning("%s MQTT unexpected disconnect rc=%s (will auto-reconnect)", self.log_prefix, rc)
        else:
            logger.info("%s MQTT clean disconnect", self.log_prefix)

    def _dispatch(self, client, userdata, msg):
        if not self._on_message:
            return
        # Ignore retained clears or any empty payloads
        if not msg.payload:
            logger.debug("%s ignore empty payload on %s", self.log_prefix, msg.topic)
            return
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
        except Exception:
            logger.warning("%s bad JSON on %s: %r", self.log_prefix, msg.topic, msg.payload[:256])
            return
        self._on_message(msg.topic, payload)
