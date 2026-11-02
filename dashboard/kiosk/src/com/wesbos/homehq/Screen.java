package com.wesbos.homehq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The screen as Home Assistant sees it over MQTT, plus wake on touch. No Android imports, so
 * test-screen.py runs it on the host; MainActivity shows the level it reports.
 *
 * Entities, announced with MQTT discovery under one device:
 * - light "Screen": on at a brightness of 1-255, or off. Off is a black cover at the lowest
 *   backlight, not Android sleep: some frames drop Wi-Fi while the screen sleeps.
 * - number "Wake brightness": what a touch raises a dimmer or dark screen to.
 * - number "Wake timeout": seconds after the last touch before the screen goes back.
 * - binary_sensor "Touched": on from a touch until the wake timeout runs out.
 * The light reports what the screen shows, so during a wake it reads on at the wake brightness.
 * A command from Home Assistant ends a wake, so it shows at once.
 */
final class Screen {
  static final String DISCOVERY_PREFIX = "homeassistant";
  /** Home Assistant publishes "online" here when it starts; discovery is then sent again. */
  static final String BIRTH_TOPIC = DISCOVERY_PREFIX + "/status";
  static final int MIN_WAKE_SECONDS = 5, MAX_WAKE_SECONDS = 3600;
  private static final Pattern STATE =
      Pattern.compile("\"state\"\\s*:\\s*\"(ON|OFF)\"", Pattern.CASE_INSENSITIVE);
  private static final Pattern BRIGHTNESS = Pattern.compile("\"brightness\"\\s*:\\s*([0-9.]+)");

  final String id, name, model, version;
  boolean on = true;
  int brightness = 180, wakeBrightness = 180, wakeSeconds = 60;
  /** Uptime in ms until which a touch keeps the screen awake; 0 when no touch is active. */
  long wakeUntil;

  Screen(String id, String name, String model, String version) {
    this.id = id;
    this.name = name;
    this.model = model;
    this.version = version;
  }

  String topic(String leaf) {
    return "homehq/" + id + "/" + leaf;
  }

  /** The level to show now: 0 is dark, otherwise a brightness of 1-255. */
  int level(long now) {
    int base = on ? brightness : 0;
    return touched(now) ? Math.max(base, wakeBrightness) : base;
  }

  boolean touched(long now) {
    return now < wakeUntil;
  }

  /** Records a touch. Returns true if the screen was dark, so the touch should only wake it. */
  boolean touch(long now) {
    boolean dark = level(now) == 0;
    wakeUntil = now + wakeSeconds * 1000L;
    return dark;
  }

  /** Applies a command from Home Assistant; returns false if the topic or payload isn't one. */
  boolean command(String topic, String payload) {
    if (topic.equals(topic("screen/set"))) {
      // Home Assistant sends {"state":"ON","brightness":128} or {"state":"OFF"}.
      Matcher state = STATE.matcher(payload), level = BRIGHTNESS.matcher(payload);
      boolean hasState = state.find(), hasLevel = level.find();
      if (!hasState && !hasLevel) return false;
      boolean turnOn = !hasState || state.group(1).equalsIgnoreCase("ON");
      if (hasLevel) {
        Integer b = number(level.group(1));
        if (b == null) return false;
        if (b <= 0) turnOn = false;
        else brightness = Math.min(b, 255);
      }
      on = turnOn;
      wakeUntil = 0;
      return true;
    }
    if (topic.equals(topic("wake_brightness/set"))) {
      Integer v = number(payload);
      if (v == null) return false;
      wakeBrightness = clamp(v, 1, 255);
      return true;
    }
    if (topic.equals(topic("wake_timeout/set"))) {
      Integer v = number(payload);
      if (v == null) return false;
      wakeSeconds = clamp(v, MIN_WAKE_SECONDS, MAX_WAKE_SECONDS);
      return true;
    }
    return false;
  }

  List<String> subscriptions() {
    return Arrays.asList(topic("screen/set"), topic("wake_brightness/set"),
        topic("wake_timeout/set"), BIRTH_TOPIC);
  }

  /** Discovery messages as {topic, payload}; publish them retained. */
  List<String[]> discovery() {
    String common = "\"availability_topic\":" + quote(topic("status"))
        + ",\"device\":{\"identifiers\":[" + quote("homehq_" + id) + "],\"name\":" + quote(name)
        + ",\"manufacturer\":\"Home HQ\",\"model\":" + quote(model)
        + ",\"sw_version\":" + quote(version) + "}";
    List<String[]> m = new ArrayList<>();
    m.add(config("light", "screen", "\"name\":\"Screen\",\"icon\":\"mdi:tablet\",\"schema\":\"json\","
        + "\"brightness\":true,\"supported_color_modes\":[\"brightness\"],\"brightness_scale\":255,"
        + "\"command_topic\":" + quote(topic("screen/set"))
        + ",\"state_topic\":" + quote(topic("screen/state")), common));
    m.add(config("number", "wake_brightness", "\"name\":\"Wake brightness\",\"icon\":\"mdi:brightness-6\","
        + "\"min\":1,\"max\":255,\"step\":1,\"mode\":\"slider\",\"entity_category\":\"config\","
        + "\"command_topic\":" + quote(topic("wake_brightness/set"))
        + ",\"state_topic\":" + quote(topic("wake_brightness/state")), common));
    m.add(config("number", "wake_timeout", "\"name\":\"Wake timeout\",\"icon\":\"mdi:timer-outline\","
        + "\"min\":" + MIN_WAKE_SECONDS + ",\"max\":" + MAX_WAKE_SECONDS + ",\"step\":1,\"mode\":\"box\","
        + "\"unit_of_measurement\":\"s\",\"entity_category\":\"config\","
        + "\"command_topic\":" + quote(topic("wake_timeout/set"))
        + ",\"state_topic\":" + quote(topic("wake_timeout/state")), common));
    m.add(config("binary_sensor", "touched", "\"name\":\"Touched\",\"icon\":\"mdi:gesture-tap\","
        + "\"state_topic\":" + quote(topic("touched/state")), common));
    return m;
  }

  private String[] config(String component, String object, String fields, String common) {
    return new String[] {
        DISCOVERY_PREFIX + "/" + component + "/homehq_" + id + "/" + object + "/config",
        "{\"unique_id\":" + quote("homehq_" + id + "_" + object) + "," + fields + "," + common + "}"};
  }

  /** Current states as {topic, payload}; publish them retained. */
  List<String[]> states(long now) {
    List<String[]> m = new ArrayList<>();
    int shown = level(now);
    m.add(new String[] {topic("screen/state"), shown > 0
        ? "{\"state\":\"ON\",\"brightness\":" + shown + ",\"color_mode\":\"brightness\"}"
        : "{\"state\":\"OFF\"}"});
    m.add(new String[] {topic("wake_brightness/state"), String.valueOf(wakeBrightness)});
    m.add(new String[] {topic("wake_timeout/state"), String.valueOf(wakeSeconds)});
    m.add(new String[] {topic("touched/state"), touched(now) ? "ON" : "OFF"});
    return m;
  }

  /** "180", "180.0" and " 180 " all read as 180; anything else is null. */
  static Integer number(String s) {
    try {
      double d = Double.parseDouble(s.trim());
      if (Double.isNaN(d) || Double.isInfinite(d)) return null;
      return (int) Math.max(Integer.MIN_VALUE, Math.min(Integer.MAX_VALUE, Math.round(d)));
    } catch (NumberFormatException e) {
      return null;
    }
  }

  static int clamp(int v, int lo, int hi) {
    return Math.max(lo, Math.min(hi, v));
  }

  static String quote(String s) {
    StringBuilder b = new StringBuilder("\"");
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (c == '"' || c == '\\') b.append('\\').append(c);
      else if (c < 0x20) b.append(String.format("\\u%04x", (int) c));
      else b.append(c);
    }
    return b.append('"').toString();
  }
}
