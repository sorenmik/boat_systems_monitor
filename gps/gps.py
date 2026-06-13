import yaml
from datetime import datetime, timezone
import time
from serial import Serial, SerialException
from pynmeagps import NMEAReader

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import ASYNCHRONOUS


NAME = "gps"

# -----------------------------
# Load config
# -----------------------------
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

device      = config[NAME]["device"]
device_type = config[NAME]["device_type"]
baud        = config[NAME]["baud"]

log_period  = config[NAME]["log_period"]
debug       = config[NAME]["debug"]

influx_cfg  = config["influx"]


# -----------------------------
# InfluxDB
# -----------------------------
client = InfluxDBClient(
    url=influx_cfg["url"],
    token=influx_cfg["token"],
    org=influx_cfg["org"],
)

write_api = client.write_api(write_options=ASYNCHRONOUS)

bucket = influx_cfg["bucket"]
org = influx_cfg["org"]


def write_to_influx(write_api, bucket, org, device, fields):
    point = Point(NAME).tag("device", device)

    for k, v in fields.items():

        if v is None:
            continue

        if isinstance(v, (int, float)):
            point = point.field(k, float(v))
        elif isinstance(v, bool):
            point = point.field(k, v)
        else:
            point = point.field(k, str(v))

    # Note: System time, NOT gps time
    point = point.time(datetime.now(timezone.utc))

    write_api.write(bucket=bucket, org=org, record=point)



class GPSSnapshot:
    def __init__(self):
        self.reset()

    def reset(self):
        self.lat = None
        self.lon = None
        self.alt = None
        self.speed = None
        self.track = None
        self.gps_mode = None
        self.last_fix_time = None
        self.latNS = None
        self.lonEW = None

    def update(self, msg):
        if msg is None:
            return

        t = msg.msgID

        if t == "GGA":
            self.lat = msg.lat
            self.latNS = msg.NS
            self.lon = msg.lon
            self.lonEW = msg.EW
            self.alt = msg.alt
            self.gps_mode = msg.quality

        elif t == "RMC":
            if msg.spd is not None:
                self.speed = msg.spd  # knots

            if msg.cog not in (None, ""):
                self.track = msg.cog

            if not self.last_fix_time:
                self.last_fix_time = msg.time

    def as_dict(self):
        return {
            "lat": self.lat,
            "latNS": self.latNS,
            "lon": self.lon,
            "lonEW": self.lonEW,
            "alt": self.alt,
            "speed_knots": self.speed,
            "track_deg": self.track,
            "gps_mode": self.gps_mode
        }

class Main:
    def __init__(self):
        pass


    def run(self, device, baud):
        snapshot = GPSSnapshot()
        last_emit = time.time()

        while True:
            try:
                print("Connecting to GPS...")

                with Serial(device, baud, timeout=3) as stream:
                    nmr = NMEAReader(stream)
                    print("GPS connected")

                    while True:
                        try:
                            _, msg = nmr.read()

                            if msg is None:
                                continue

                            snapshot.update(msg)

                            if time.time() - last_emit >= log_period:
                                if debug:
                                    print(snapshot.as_dict())

                                write_to_influx(write_api, bucket, org, device_type, snapshot.as_dict())

                                snapshot.reset()
                                last_emit = time.time()

                        except Exception as e:
                            if debug:
                                print("Read error:", e)
                            break  # exit inner loop: reconnect

            except SerialException as e:
                if debug:
                    print("GPS disconnected:", e)

            print("Reconnecting in 2 seconds...")
            time.sleep(2)


if __name__ == "__main__":
    main = Main()
    main.run(device, baud)