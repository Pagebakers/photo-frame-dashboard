package com.wesbos.homehq;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;

/**
 * A small MQTT 3.1.1 client, enough for Home Assistant: username and password, a retained last
 * will, QoS 0 publishes and QoS 0 subscriptions. It reconnects by itself, waiting longer after
 * each failure. No Android imports, so test-screen.py runs it on the host against a fake broker.
 */
final class Mqtt {
  interface Listener {
    /** Connected and subscribed. Runs on the client's thread. */
    void onConnected();
    /** A message on a subscribed topic. Runs on the client's thread. */
    void onMessage(String topic, String payload);
    /** The connection failed or dropped; a retry follows. Runs on the client's thread. */
    void onDisconnected(String reason);
  }

  static final int CONNECT = 1, CONNACK = 2, PUBLISH = 3, PUBACK = 4, SUBSCRIBE = 8, PINGREQ = 12,
      DISCONNECT = 14;
  static final int CONNECT_TIMEOUT_MS = 10000;

  final String host, clientId, user, password, willTopic, willPayload;
  final int port;
  final List<String> topics;
  final Listener listener;
  // Tests shorten these.
  int keepAliveSeconds = 30;
  long retryMs = 5000, maxRetryMs = 60000;

  // Publishes come from the main thread, where Android forbids network writes.
  private final ExecutorService writer = Executors.newSingleThreadExecutor(r -> {
    Thread t = new Thread(r, "mqtt-write");
    t.setDaemon(true);
    return t;
  });
  private volatile boolean running;
  private volatile Socket socket;
  private OutputStream out; // guarded by this; null until the broker accepts the connection
  private long lastSent;    // guarded by this
  private boolean sessionUp;

  Mqtt(String host, int port, String clientId, String user, String password, String willTopic,
       String willPayload, List<String> topics, Listener listener) {
    this.host = host;
    this.port = port;
    this.clientId = clientId;
    this.user = user;
    this.password = password;
    this.willTopic = willTopic;
    this.willPayload = willPayload;
    this.topics = topics;
    this.listener = listener;
  }

  void start() {
    running = true;
    Thread t = new Thread(this::run, "mqtt");
    t.setDaemon(true);
    t.start();
  }

  /** Publishes the will payload, so Home Assistant shows the device unavailable, and disconnects. */
  void stop() {
    running = false;
    try {
      writer.execute(() -> {
        if (willTopic != null) send(publishPacket(willTopic, willPayload, true));
        send(new byte[] {(byte) (DISCONNECT << 4), 0});
        closeSocket();
      });
    } catch (RejectedExecutionException ignored) {
      // already stopped
    }
    writer.shutdown();
  }

  /** Queues a QoS 0 publish. Dropped while disconnected: callers publish their state again on connect. */
  void publish(String topic, String payload, boolean retain) {
    try {
      writer.execute(() -> send(publishPacket(topic, payload, retain)));
    } catch (RejectedExecutionException ignored) {
      // stopped
    }
  }

  private void run() {
    long delay = retryMs;
    while (running) {
      String reason;
      try {
        session();
        reason = "connection closed";
      } catch (IOException | RuntimeException e) {
        reason = e.getMessage() != null ? e.getMessage() : e.toString();
      } finally {
        synchronized (this) {
          out = null;
        }
        closeSocket();
      }
      if (!running) return;
      if (sessionUp) delay = retryMs; // it worked before, so start the waits short again
      sessionUp = false;
      listener.onDisconnected(reason);
      try {
        Thread.sleep(delay);
      } catch (InterruptedException e) {
        return;
      }
      delay = Math.min(delay * 2, maxRetryMs);
    }
  }

  private void session() throws IOException {
    Socket s = new Socket();
    socket = s;
    s.connect(new InetSocketAddress(host, port), CONNECT_TIMEOUT_MS);
    s.setSoTimeout(keepAliveSeconds * 1000 / 2);
    InputStream in = new BufferedInputStream(s.getInputStream());
    OutputStream o = s.getOutputStream();
    o.write(connectPacket());
    o.flush();
    Packet ack = read(in, false);
    if (ack.type != CONNACK || ack.body.length < 2) throw new IOException("no CONNACK from the broker");
    if (ack.body[1] != 0) throw new IOException("broker refused the connection (code " + ack.body[1] + ")");
    synchronized (this) {
      out = o;
    }
    send(subscribePacket());
    sessionUp = true;
    listener.onConnected();
    long lastHeard = now();
    while (running) {
      Packet p = read(in, true); // null after half the keep-alive with nothing received
      long now = now();
      if (p != null) {
        lastHeard = now;
        if (p.type == PUBLISH) onPublish(p);
      } else if (now - lastHeard > keepAliveSeconds * 1500L) {
        throw new IOException("broker stopped answering");
      }
      // The broker drops clients that send nothing for 1.5 keep-alives; a ping also proves it's there.
      boolean ping;
      synchronized (this) {
        ping = now - lastSent >= keepAliveSeconds * 500L;
      }
      if (ping) send(new byte[] {(byte) (PINGREQ << 4), 0});
    }
  }

  private void onPublish(Packet p) throws IOException {
    int qos = (p.flags >> 1) & 3;
    if (p.body.length < 2) throw new IOException("bad PUBLISH from the broker");
    int topicLength = ((p.body[0] & 255) << 8) | (p.body[1] & 255);
    int at = 2 + topicLength;
    if (at + (qos > 0 ? 2 : 0) > p.body.length) throw new IOException("bad PUBLISH from the broker");
    String topic = new String(p.body, 2, topicLength, StandardCharsets.UTF_8);
    if (qos > 0) {
      // Not expected with QoS 0 subscriptions, but a broker that sends QoS 1 anyway waits for this.
      int id = ((p.body[at] & 255) << 8) | (p.body[at + 1] & 255);
      at += 2;
      if (qos == 1) send(new byte[] {(byte) (PUBACK << 4), 2, (byte) (id >> 8), (byte) id});
    }
    listener.onMessage(topic, new String(p.body, at, p.body.length - at, StandardCharsets.UTF_8));
  }

  /** Reads one packet. With idleOk, a read timeout before the packet starts returns null. */
  private static Packet read(InputStream in, boolean idleOk) throws IOException {
    int first;
    try {
      first = in.read();
    } catch (SocketTimeoutException e) {
      if (idleOk) return null;
      throw new IOException("no answer from the broker");
    }
    if (first < 0) throw new EOFException("broker closed the connection");
    try {
      int length = 0, shift = 0, b;
      do {
        b = in.read();
        if (b < 0) throw new EOFException("broker closed the connection");
        if (shift > 21) throw new IOException("bad packet length from the broker");
        length |= (b & 127) << shift;
        shift += 7;
      } while ((b & 128) != 0);
      byte[] body = new byte[length];
      new DataInputStream(in).readFully(body);
      return new Packet(first >> 4, first & 15, body);
    } catch (SocketTimeoutException e) {
      throw new IOException("broker stopped in the middle of a packet");
    }
  }

  private boolean send(byte[] packet) {
    synchronized (this) {
      if (out == null) return false;
      try {
        out.write(packet);
        out.flush();
        lastSent = now();
        return true;
      } catch (IOException e) {
        out = null;
        closeSocket(); // the reading thread notices and reconnects
        return false;
      }
    }
  }

  private void closeSocket() {
    Socket s = socket;
    if (s == null) return;
    try {
      s.close();
    } catch (IOException ignored) {
      // closing anyway
    }
  }

  private static long now() {
    return System.nanoTime() / 1000000;
  }

  byte[] connectPacket() {
    ByteArrayOutputStream b = new ByteArrayOutputStream();
    string(b, "MQTT");
    b.write(4); // protocol level 3.1.1
    boolean hasUser = user != null && !user.isEmpty();
    int flags = 0x02; // clean session
    if (willTopic != null) flags |= 0x04 | 0x20; // will, retained
    if (hasUser) flags |= 0x80;
    if (hasUser && password != null && !password.isEmpty()) flags |= 0x40; // 3.1.1: no password without a user
    b.write(flags);
    b.write(keepAliveSeconds >> 8);
    b.write(keepAliveSeconds);
    string(b, clientId);
    if (willTopic != null) {
      string(b, willTopic);
      string(b, willPayload);
    }
    if ((flags & 0x80) != 0) string(b, user);
    if ((flags & 0x40) != 0) string(b, password);
    return packet(CONNECT << 4, b.toByteArray());
  }

  byte[] subscribePacket() {
    ByteArrayOutputStream b = new ByteArrayOutputStream();
    b.write(0);
    b.write(1); // packet id; only one subscribe is ever outstanding
    for (String topic : topics) {
      string(b, topic);
      b.write(0); // QoS 0
    }
    return packet((SUBSCRIBE << 4) | 2, b.toByteArray());
  }

  static byte[] publishPacket(String topic, String payload, boolean retain) {
    ByteArrayOutputStream b = new ByteArrayOutputStream();
    string(b, topic);
    byte[] data = payload.getBytes(StandardCharsets.UTF_8);
    b.write(data, 0, data.length);
    return packet((PUBLISH << 4) | (retain ? 1 : 0), b.toByteArray());
  }

  private static byte[] packet(int header, byte[] body) {
    ByteArrayOutputStream b = new ByteArrayOutputStream();
    b.write(header);
    int length = body.length;
    do {
      int digit = length & 127;
      length >>= 7;
      b.write(length > 0 ? digit | 128 : digit);
    } while (length > 0);
    b.write(body, 0, body.length);
    return b.toByteArray();
  }

  private static void string(ByteArrayOutputStream b, String s) {
    byte[] data = s.getBytes(StandardCharsets.UTF_8);
    b.write(data.length >> 8);
    b.write(data.length);
    b.write(data, 0, data.length);
  }

  private static final class Packet {
    final int type, flags;
    final byte[] body;

    Packet(int type, int flags, byte[] body) {
      this.type = type;
      this.flags = flags;
      this.body = body;
    }
  }
}
