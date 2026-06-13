import asyncio
import time
import yaml
from datetime import datetime, timezone

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import ASYNCHRONOUS

from victron_ble.scanner import Scanner



NAME = "victron"

# -----------------------------
# Load config
# -----------------------------
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

devices    = config[NAME]["devices"]
influx_cfg = config["influx"]

def norm(mac: str) -> str:
    return mac.strip().lower()

DEVICE_KEYS   = {norm(d["mac"]): d["key"] for d in devices}
DEVICE_CONFIG = {norm(d["mac"]): d for d in devices}


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


def write_to_influx(write_api, bucket, org, name, mac, fields):
    point = Point(NAME)
    point = point.tag("device", name)
    point = point.tag("mac", mac)

    for k, v in fields.items():

        if v is None:
            continue

        if isinstance(v, (int, float)):
            point = point.field(k, float(v))
        elif isinstance(v, bool):
            point = point.field(k, v)
        else:
            point = point.field(k, str(v))

    point = point.time(datetime.now(timezone.utc))

    write_api.write(bucket=bucket, org=org, record=point)


# -----------------------------
# Victron scanner
# -----------------------------
class VictronInfluxScanner(Scanner):

    def __init__(self, device_keys, influx):
        super().__init__(device_keys, indent=None)
        self.influx = influx
        self.last_write = {}

    def should_write(self, mac: str, period: int) -> bool:
        now = time.time()
        last = self.last_write.get(mac, 0)

        if now - last >= period:
            self.last_write[mac] = now
            return True

        return False

    def extract_fields(self, parsed):
        fields = {}

        methods = [
            m for m in dir(parsed)
            if m.startswith("get_") and callable(getattr(parsed, m))
        ]

        for name in methods:
            try:
                value = getattr(parsed, name)()

                if value is None:
                    continue

                if hasattr(value, "name"):
                    value = value.name.lower()

                fields[name[4:]] = value

            except Exception:
                continue

        return fields

    def callback(self, ble_device, raw_data, advertisement):

        try:
            device = self.get_device(ble_device, raw_data)
        except Exception:
            return

        mac = norm(ble_device.address)
        cfg = DEVICE_CONFIG.get(mac, {})

        # Throttle writes
        log_period = cfg.get("log_period", 1)
        if not self.should_write(mac, log_period):
            return
        
        # Extract fields
        name = cfg.get("name", mac)
        parsed = device.parse(raw_data)
        fields = self.extract_fields(parsed)

        # Debug print
        if cfg.get("debug", False):
            print(f"{name} ({mac}):", fields)

        # Filter fields
        allowed_fields = set(cfg.get("fields", []))
        if allowed_fields:
            fields = {
                k: v for k, v in fields.items()
                if k in allowed_fields
            }
        
        # Into db
        write_to_influx(
            self.influx,
            bucket=bucket,
            org=org,
            name=name,
            mac=mac,
            fields=fields,
        )


# -----------------------------
# Main
# -----------------------------
async def main():
    print(f"Starting {NAME} --> Influxdb")
    print(f"Devices: {len(devices)}")

    scanner = VictronInfluxScanner(DEVICE_KEYS, write_api)
    await scanner.start()
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())