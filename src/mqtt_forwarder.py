#!/usr/bin/env python3

import argparse
import json
import signal
import ssl
import sys
from pathlib import Path

import paho.mqtt.client as mqtt


DEFAULT_CONFIG_FILE = "config.json"


class MQTTForwarder:
    def __init__(self, config, verbosity=3):
        self.config = config
        self.verbosity = verbosity

        self.client = mqtt.Client(
            client_id=config.get("client_id", ""),
            clean_session=True
        )

        username = config.get("username")
        password = config.get("password")

        if username:
            self.client.username_pw_set(username, password)

        self.configure_tls()

        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect

    def log(self, level, text):
        if self.verbosity >= level:
            print(text)

    def configure_tls(self):
        if not self.config.get("tls", False):
            return

        if self.config.get("tls_use_os_cert", False):
            self.client.tls_set(
                cert_reqs=ssl.CERT_REQUIRED
            )

            self.log(
                2,
                "[+] TLS aktiviert (OS Zertifikate)"
            )

        else:
            ca_file = self.config.get("ca_cert")

            self.client.tls_set(
                ca_certs=ca_file,
                cert_reqs=ssl.CERT_REQUIRED
            )

            self.log(
                2,
                f"[+] TLS aktiviert (CA={ca_file})"
            )

        if self.config.get("tls_insecure", False):
            self.client.tls_insecure_set(True)

            self.log(
                1,
                "[!] TLS Zertifikatsprüfung deaktiviert"
            )

    def build_subscribe_list(self):
        subscribe_topics = self.config.get(
            "subscribe_topics",
            []
        )

        if subscribe_topics == 0:
            topics = set()

            for rule in self.config.get("rules", []):
                source_topic = rule.get("source_topic")

                if source_topic:
                    topics.add(source_topic)

            return list(topics)

        return subscribe_topics

    def on_connect(self, client, userdata, flags, rc):
        if rc != 0:
            self.log(0, f"[!] Verbindungsfehler rc={rc}")
            return

        self.log(1, "[+] MQTT verbunden")

        subscribe_topics = self.build_subscribe_list()

        qos = self.config.get("subscribe_qos", 0)

        for topic in subscribe_topics:
            client.subscribe(topic, qos=qos)

            self.log(
                1,
                f"[+] Subscribe: {topic} (QoS={qos})"
            )

    def on_disconnect(self, client, userdata, rc):
        self.log(1, "[!] MQTT getrennt")

    def on_message(self, client, userdata, msg):
        try:
            payload = msg.payload.decode(
                "utf-8",
                errors="ignore"
            )
        except Exception as e:
            self.log(0, f"[!] Decode Fehler: {e}")
            return

        self.log(
            5,
            f"[RX] {msg.topic}: {payload}"
        )

        matched = False

        for rule in self.config.get("rules", []):

            rule_source_topic = rule.get("source_topic")

            if rule_source_topic:
                if msg.topic != rule_source_topic:
                    continue

            keyword = rule.get("starts_with")
            target_topic = rule.get("target_topic")

            if not target_topic:
                continue

            if keyword:
                if not payload.startswith(keyword):
                    continue

            matched = True

            new_payload = payload

            if keyword and rule.get("remove_keyword", False):
                new_payload = payload[len(keyword):].lstrip()

            qos = rule.get("qos", 0)
            retain = rule.get("retain", False)

            result = client.publish(
                target_topic,
                payload=new_payload.encode("utf-8"),
                qos=qos,
                retain=retain
            )

            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                self.log(
                    2,
                    f"[TX] {msg.topic} -> "
                    f"{target_topic}: {new_payload}"
                )
            else:
                self.log(
                    0,
                    f"[!] Publish Fehler -> "
                    f"{target_topic}"
                )

        if not matched:
            self.log(7, "[ ] Keine Regel getroffen")

    def run(self):
        host = self.config["host"]
        port = self.config.get("port", 1883)
        keepalive = self.config.get("keepalive", 60)

        self.log(
            1,
            f"[+] Verbinde zu {host}:{port}"
        )

        self.client.connect(
            host,
            port,
            keepalive
        )

        self.client.loop_forever()


def load_config(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def signal_handler(sig, frame):
    print("\\n[+] Beendet")
    sys.exit(0)


def parse_args():
    parser = argparse.ArgumentParser(
        description="MQTT Topic Forwarder"
    )

    parser.add_argument(
        "-c",
        "--config",
        default=DEFAULT_CONFIG_FILE,
        help="Config-Datei"
    )

    parser.add_argument(
        "-u",
        "--username",
        help="MQTT Username überschreiben"
    )

    parser.add_argument(
        "-p",
        "--password",
        help="MQTT Passwort überschreiben"
    )

    parser.add_argument(
        "-v",
        type=int,
        choices=range(0, 10),
        metavar="0-9",
        help="Verbosity 0..9 (überschreibt config)"
    )

    return parser.parse_args()


def main():
    signal.signal(signal.SIGINT, signal_handler)

    args = parse_args()

    config_path = Path(args.config)

    if not config_path.exists():
        print(f"[!] Config fehlt: {config_path}")
        sys.exit(1)

    config = load_config(config_path)

    if args.username is not None:
        config["username"] = args.username

    if args.password is not None:
        config["password"] = args.password

    verbosity = (
        args.v
        if args.v is not None
        else config.get("verbosity", 3)
    )

    forwarder = MQTTForwarder(
        config=config,
        verbosity=verbosity
    )

    forwarder.run()


if __name__ == "__main__":
    main()
