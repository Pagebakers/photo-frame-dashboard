#!/usr/bin/env python3
"""Run the actual Screen and Mqtt classes against a fake MQTT broker.

Requires Python 3 and a JDK on PATH. No Android device, SDK, broker or third-party packages.
This checks the wake rules, discovery payloads and the MQTT exchange, including reconnects,
not Android's brightness setting or Home Assistant itself.
"""
from pathlib import Path
import json
import socket
import struct
import subprocess
import tempfile

SRC = Path(__file__).resolve().parent / "src/com/wesbos/homehq"
BASE = "homehq/test/"
COMMANDS = {BASE + "screen/set", BASE + "wake_brightness/set", BASE + "wake_timeout/set",
            "homeassistant/status"}
CONFIGS = {f"homeassistant/{c}/homehq_test/{o}/config" for c, o in [
    ("light", "screen"), ("number", "wake_brightness"), ("number", "wake_timeout"),
    ("binary_sensor", "touched")]}
STATES = {BASE + s for s in ["screen/state", "wake_brightness/state", "wake_timeout/state",
                             "touched/state"]}

HARNESS = r'''
package com.wesbos.homehq;

public class ScreenHarness {
  static void check(boolean ok, String name) {
    if (!ok) throw new AssertionError(name);
    System.out.println("PASS " + name);
  }

  static void publish(Mqtt mqtt, Screen screen, boolean discovery) {
    if (discovery) {
      for (String[] m : screen.discovery()) mqtt.publish(m[0], m[1], true);
      mqtt.publish(screen.topic("status"), "online", true);
    }
    for (String[] m : screen.states(0)) mqtt.publish(m[0], m[1], true);
  }

  static void rules() {
    Screen s = new Screen("test", "n", "m", "v");
    String set = s.topic("screen/set");
    check(s.level(0) == 180, "on at its brightness");
    check(!s.touch(1000) && s.level(1000) == 180, "touch on a lit screen goes to the page");
    check(s.touched(60999) && !s.touched(61000), "a touch lasts the wake timeout");
    check(s.command(set, "{\"state\":\"ON\",\"brightness\":10}") && s.on && s.brightness == 10
        && s.wakeUntil == 0, "dim command ends a wake");
    check(!s.touch(0) && s.level(0) == 180 && s.level(60000) == 10, "touch lifts a dim screen, then it dims again");
    check(s.command(set, "{\"state\":\"OFF\"}") && !s.on && s.level(0) == 0, "off is dark");
    check(s.touch(0) && s.level(1) == 180, "the touch that wakes a dark screen is not passed on");
    check(!s.touch(2), "the next touch while awake goes to the page");
    check(s.level(60002) == 0, "dark again after the timeout");
    check(s.command(set, "{\"state\":\"ON\"}") && s.on && s.brightness == 10, "on keeps the last brightness");
    check(s.command(set, "{\"state\":\"ON\",\"brightness\":200}") && !s.touch(0) && s.level(1) == 200,
        "a touch never dims a brighter screen");
    check(s.command(set, "{\"brightness\":0}") && !s.on, "brightness 0 is off");
    check(s.command(set, "{\"state\":\"on\",\"brightness\":999}") && s.on && s.brightness == 255, "brightness clamps");
    check(!s.command(set, "{}") && s.on, "empty command ignored");
    check(s.command(s.topic("wake_brightness/set"), "300") && s.wakeBrightness == 255, "wake brightness clamps");
    check(s.command(s.topic("wake_timeout/set"), "90.0") && s.wakeSeconds == 90, "decimal timeout");
    check(s.command(s.topic("wake_timeout/set"), "1") && s.wakeSeconds == 5, "timeout has a minimum");
    check(!s.command(s.topic("wake_timeout/set"), "soon") && s.wakeSeconds == 5, "bad number ignored");
    check(!s.command("homehq/other/screen/set", "{\"state\":\"OFF\"}") && s.on, "other device's topic ignored");
    check(Screen.quote("a\"b\\c\n").equals("\"a\\\"b\\\\c\\u000a\""), "JSON quoting");

    Screen w = new Screen("test", "n", "m", "v");
    String light = w.topic("screen/state");
    w.command(w.topic("screen/set"), "{\"state\":\"OFF\"}");
    w.touch(100);
    check(w.states(101).get(0)[0].equals(light)
        && w.states(101).get(0)[1].equals("{\"state\":\"ON\",\"brightness\":180,\"color_mode\":\"brightness\"}")
        && w.states(60100).get(0)[1].equals("{\"state\":\"OFF\"}"), "the light shows a wake from off, then off");
    w.command(w.topic("screen/set"), "{\"state\":\"ON\",\"brightness\":3}");
    w.touch(100);
    check(w.states(101).get(0)[1].contains("\"brightness\":180")
        && w.states(60100).get(0)[1].contains("\"brightness\":3"), "the light shows a wake from dim, then dim");
  }

  public static void main(String[] args) throws Exception {
    rules();
    Screen screen = new Screen("test", "Van \"frame\"", "Frame", "1.1");
    Object lock = new Object();
    Mqtt[] client = new Mqtt[1];
    client[0] = new Mqtt("127.0.0.1", Integer.parseInt(args[0]), "homehq-test", "frame", "secret",
        screen.topic("status"), "offline", screen.subscriptions(), new Mqtt.Listener() {
          public void onConnected() {
            synchronized (lock) { publish(client[0], screen, true); }
          }
          public void onMessage(String topic, String payload) {
            synchronized (lock) {
              if (Screen.BIRTH_TOPIC.equals(topic)) {
                if ("online".equals(payload)) publish(client[0], screen, true);
              } else if (screen.command(topic, payload)) {
                publish(client[0], screen, false);
              }
            }
          }
          public void onDisconnected(String reason) {
            System.out.println("client: " + reason);
          }
        });
    client[0].keepAliveSeconds = 2;
    client[0].retryMs = 200;
    client[0].maxRetryMs = 400;
    client[0].start();
    Thread.sleep(120000); // stopped by the test
  }
}
'''


def recv_exact(conn, n):
    data = b""
    while len(data) < n:
        chunk = conn.recv(n - len(data))
        if not chunk:
            raise EOFError("client closed the connection")
        data += chunk
    return data


def read_packet(conn):
    first = recv_exact(conn, 1)[0]
    length, shift = 0, 0
    while True:
        b = recv_exact(conn, 1)[0]
        length |= (b & 127) << shift
        shift += 7
        if not b & 128:
            break
    return first >> 4, first & 15, recv_exact(conn, length)


def string_at(body, at):
    n = struct.unpack_from(">H", body, at)[0]
    return body[at + 2:at + 2 + n].decode(), at + 2 + n


def encode(header, body):
    out, n = bytes([header]), len(body)
    while True:
        digit, n = n & 127, n >> 7
        out += bytes([digit | 128 if n else digit])
        if not n:
            return out + body


def utf8(s):
    data = s.encode()
    return struct.pack(">H", len(data)) + data


def send_publish(conn, topic, payload, qos=0, pid=1):
    body = utf8(topic) + (struct.pack(">H", pid) if qos else b"") + payload.encode()
    conn.sendall(encode(0x30 | qos << 1, body))


def handshake(conn, accept=True):
    kind, flags, body = read_packet(conn)
    assert kind == 1, f"expected CONNECT, got {kind}"
    protocol, at = string_at(body, 0)
    level, connect_flags, keepalive = body[at], body[at + 1], struct.unpack_from(">H", body, at + 2)[0]
    client_id, at = string_at(body, at + 4)
    will_topic, at = string_at(body, at)
    will, at = string_at(body, at)
    user, at = string_at(body, at)
    password, at = string_at(body, at)
    assert (protocol, level, keepalive) == ("MQTT", 4, 2), (protocol, level, keepalive)
    assert connect_flags == 0x80 | 0x40 | 0x20 | 0x04 | 0x02, hex(connect_flags)
    assert (client_id, will_topic, will, user, password) == (
        "homehq-test", BASE + "status", "offline", "frame", "secret")
    conn.sendall(bytes([0x20, 2, 0, 0 if accept else 5]))
    if not accept:
        return
    kind, flags, body = read_packet(conn)
    assert (kind, flags) == (8, 2), "expected SUBSCRIBE"
    topics, at = set(), 2
    while at < len(body):
        topic, at = string_at(body, at)
        assert body[at] == 0, "QoS 0 subscriptions"
        topics.add(topic)
        at += 1
    assert topics == COMMANDS, topics
    conn.sendall(bytes([0x90, 2 + len(topics), 0, 1]) + bytes(len(topics)))


def collect(conn, want, others=None):
    """Reads until every topic in want was published; answers pings. Returns topic -> payload."""
    got = {}
    while not want <= got.keys():
        kind, flags, body = read_packet(conn)
        if kind == 3:
            topic, at = string_at(body, 0)
            assert flags & 1, f"{topic} should be retained"
            got[topic] = body[at:].decode()
        elif kind == 12:
            conn.sendall(bytes([0xD0, 0]))
        elif others is not None:
            others.append((kind, body))
        else:
            raise AssertionError(f"unexpected packet type {kind}")
    return got


def wait_for(conn, topic, ok, others=None):
    """Reads until topic arrives with a payload ok accepts; leftovers from earlier bursts are skipped."""
    while True:
        payload = collect(conn, {topic}, others)[topic]
        if ok(payload):
            return payload


def check(name, ok):
    assert ok, name
    print("PASS", name)


def check_announcement(got, light_state):
    configs = {topic: json.loads(got[topic]) for topic in CONFIGS}
    light = configs["homeassistant/light/homehq_test/screen/config"]
    check("light config", light["schema"] == "json" and light["supported_color_modes"] == ["brightness"]
          and light["command_topic"] == BASE + "screen/set" and light["state_topic"] == BASE + "screen/state")
    check("one device, quoted name", all(c["device"]["identifiers"] == ["homehq_test"]
                                         and c["device"]["name"] == 'Van "frame"' for c in configs.values()))
    check("unique ids", len({c["unique_id"] for c in configs.values()}) == 4)
    check("availability", all(c["availability_topic"] == BASE + "status" for c in configs.values())
          and got[BASE + "status"] == "online")
    check("state on connect", json.loads(got[BASE + "screen/state"]) == light_state
          and got[BASE + "touched/state"] == "OFF")


with tempfile.TemporaryDirectory(prefix="homehq-screen-") as directory, \
        socket.socket() as server:
    root = Path(directory)
    (root / "ScreenHarness.java").write_text(HARNESS, encoding="utf-8")
    subprocess.run(["javac", "--release", "8", "-d", str(root), str(SRC / "Mqtt.java"),
                    str(SRC / "Screen.java"), str(root / "ScreenHarness.java")], check=True, timeout=60)
    server.bind(("127.0.0.1", 0))
    server.listen()
    server.settimeout(15)
    client = subprocess.Popen(["java", "-cp", str(root), "com.wesbos.homehq.ScreenHarness",
                               str(server.getsockname()[1])])
    try:
        conn, _ = server.accept()
        conn.settimeout(8)
        handshake(conn)
        check_announcement(collect(conn, CONFIGS | STATES | {BASE + "status"}),
                           {"state": "ON", "brightness": 180, "color_mode": "brightness"})

        # A socket timeout below means the expected message never came.
        others = []
        send_publish(conn, BASE + "screen/set", '{"state":"ON","brightness":42}', qos=1, pid=7)
        wait_for(conn, BASE + "screen/state", lambda p: json.loads(p).get("brightness") == 42, others)
        check("brightness command", True)
        check("QoS 1 command acknowledged", (4, b"\x00\x07") in others)
        send_publish(conn, BASE + "wake_timeout/set", "90.0")
        wait_for(conn, BASE + "wake_timeout/state", lambda p: p == "90")
        check("timeout command", True)
        send_publish(conn, BASE + "screen/set", '{"state":"OFF"}')
        wait_for(conn, BASE + "screen/state", lambda p: json.loads(p) == {"state": "OFF"})
        check("off command", True)
        send_publish(conn, "homeassistant/status", "online")
        collect(conn, CONFIGS)
        check("discovery again when Home Assistant starts", True)

        while True:  # skip what's left of the last burst
            kind, _, _ = read_packet(conn)
            if kind == 12:
                break
            assert kind == 3, f"unexpected packet type {kind}"
        check("keep-alive ping", True)
        conn.sendall(bytes([0xD0, 0]))
        conn.close()

        conn, _ = server.accept()  # reconnects after the broker drops it
        conn.settimeout(8)
        handshake(conn, accept=False)
        conn.close()
        conn, _ = server.accept()  # and again after a refused login
        conn.settimeout(8)
        handshake(conn)
        got = collect(conn, CONFIGS | STATES | {BASE + "status"})
        check("state kept across reconnects", json.loads(got[BASE + "screen/state"]) == {"state": "OFF"}
              and got[BASE + "wake_timeout/state"] == "90")

        pings = 0
        try:
            while True:  # never answer: the client must give up and reconnect
                kind, _, _ = read_packet(conn)
                pings += kind == 12
        except EOFError:
            pass
        conn.close()
        conn, _ = server.accept()
        conn.settimeout(8)
        handshake(conn)
        check("reconnects when the broker stops answering", pings >= 1)
        conn.close()
    finally:
        client.kill()
        client.wait()
