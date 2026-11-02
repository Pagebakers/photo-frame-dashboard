# Screen brightness from Home Assistant

Home HQ, the kiosk in [`dashboard/kiosk`](../dashboard/kiosk), can put the frame's screen under
Home Assistant's control over MQTT: a brightness light, off, and wake on touch. It does nothing
until a broker is saved, so a kiosk without one behaves as before.

Tested on one SSA RK3326 frame ([akimart-zn-dp1002.md](akimart-zn-dp1002.md), Android 8.1,
WebView 106) with Home Assistant 2026.10 and the Mosquitto broker add-on, over USB ADB from macOS.

## What Home Assistant gets

One device, `Home HQ` unless you name it, found through MQTT discovery:

| Entity | What it does |
| --- | --- |
| Screen (light) | Brightness 1 to 255, or off. |
| Wake brightness (number) | What a touch raises a dim or off screen to. Default 180. |
| Wake timeout (number) | Seconds after the last touch before the screen goes back. Default 60. |
| Touched (binary sensor) | On from a touch until the wake timeout ends. |

- Off is a black cover over the dashboard at the lowest backlight, not Android sleep: some frames
  drop Wi-Fi while the screen sleeps ([akimart-zn-dp1002.md](akimart-zn-dp1002.md)).
- On an off screen, the first touch only wakes it and doesn't reach the dashboard.
- The light reports what the screen shows: during a touch wake it reads on at Wake brightness,
  and it returns to the level Home Assistant set when the wake ends.
- A command from Home Assistant ends a touch wake at once.
- Home HQ saves the last state, so after a reboot the screen comes back as Home Assistant left it,
  before the broker answers. While Home HQ is offline, Home Assistant shows the device as
  unavailable.

## 1. Home Assistant

1. Settings → Add-ons → Add-on store → **Mosquitto broker**: install it, start it, and turn on
   **Start on boot**.
2. Settings → Devices & services: configure the **MQTT** integration it finds.
3. Settings → People → Users (shown with Advanced mode on): add a user for the frame, not an
   administrator. The Mosquitto add-on accepts Home Assistant users.

Optional check from the frame, where `toybox nc` exists:

```sh
adb shell "(printf '\020\020\000\004MQTT\004\002\000\074\000\004test'; sleep 2) | toybox nc -w 4 <broker-ip> 1883 | od -An -tx1"
```

`20 02 00 05` means the broker is running and refuses a login without a user, as it should.
`Connection refused` means it isn't running yet.

## 2. The frame

Build and install the kiosk ([Build and install](../dashboard/kiosk/README.md#build-and-install)),
then:

```sh
adb shell appops set com.wesbos.homehq WRITE_SETTINGS allow
adb shell am start -n com.wesbos.homehq/.MainActivity --es mqtt_host <broker-ip> --es mqtt_user <user> --es mqtt_password <password> --es mqtt_name "'Living room frame'"
```

- The first line lets Home HQ change Android's brightness setting, so it holds in every app.
  Without it, only the kiosk's own window changes.
- Values pass through the frame's shell a second time. Wrap any value with a space or shell
  character in both kinds of quotes, as with the name above. With single quotes alone, only the
  first word is saved.
- `--es mqtt_port 1883` is the default. `mqtt_name` is the device's name in Home Assistant.

## 3. Check

```sh
adb logcat -d -s HomeHQ | grep MQTT
```

`MQTT connected to <broker-ip>` should appear within a second or two, and the device under
Settings → Devices & services → MQTT. `broker refused the connection (code 5)` means a wrong user
or password; Home HQ keeps retrying, up to once a minute, so fix it by saving the settings again.

## Notes

- Android has a minimum backlight. On the tested frame, anything below about 8% (20 of 255)
  looks the same. Off is darker: the black cover at that minimum.
- The password is kept in the app's private storage, which anyone with ADB on a rooted frame can
  read. Give the frame its own Home Assistant user.

## Undo

```sh
adb shell am start -n com.wesbos.homehq/.MainActivity --es mqtt_host "''"
adb shell appops set com.wesbos.homehq WRITE_SETTINGS default
```

Then delete the device in Home Assistant's MQTT integration; its entities stay until you do.

## Tests

From `dashboard/`, with Python 3 and a JDK on `PATH`:

```sh
python3 kiosk/test-screen.py
```

It runs the actual wake rules and MQTT client against a fake broker, including reconnects after
a dropped connection, a refused login and a silent broker. It does not prove Android's brightness
setting or Home Assistant's side.
