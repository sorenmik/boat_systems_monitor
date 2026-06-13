import asyncio
import yaml
import fnmatch
from bleak import BleakClient
from datetime import datetime, timezone

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import ASYNCHRONOUS



NAME = "gatt"


# -----------------------------
# Load config
# -----------------------------
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

devices    = config[NAME]["devices"]
influx_cfg = config["influx"]

def norm(mac: str) -> str:
    return mac.strip().lower()

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
# Handlers for different device types
# -----------------------------
class JbdDevice:
    
    def __init__(self, device):

        # JBD specific request frame
        self.REQ_ALL = bytes.fromhex("DD A5 03 00 FF FD 77")

        # Buffer, since data is split into three frames
        self.buffer = bytearray()

        # Common config
        self.name = device.get("name")
        self.mac  = device.get("mac")
        self.debug = device.get("debug", False)
        self.notify_uuid = device.get("notify_uuid")
        self.write_uuid  = device.get("write_uuid")
        self.log_period  = device.get("log_period", 10)
        self.allowed_fields = set(device.get("fields", []))

    def parse(self, frame: bytes):
        payload = frame[4:-3]

        voltage = int.from_bytes(payload[0:2], "big") * 0.01

        raw_current = int.from_bytes(payload[2:4], "big")

        if raw_current > 32767:
            current = -(65536 - raw_current) * 0.01
        else:
            current = raw_current * 0.01

        remaining_capacity = int.from_bytes(payload[4:6], "big") * 0.01
        nominal_capacity   = int.from_bytes(payload[6:8], "big") * 0.01

        cycles = int.from_bytes(payload[8:10], "big")

        soc = payload[19]

        fet_state = payload[20]
        cell_count = payload[21]
        temp_count = payload[22]

        temps = []

        offset = 23

        for _ in range(temp_count):
            raw = int.from_bytes(payload[offset:offset+2], "big")
            temps.append((raw - 2731) / 10)
            offset += 2
        
        return {
            "voltage": voltage,
            "current": current,
            "power": voltage * current,
            "soc": soc,
            "avg_cell_voltage": voltage / cell_count if cell_count else None,
            "cycles": cycles,
            "remaining_capacity_ah": remaining_capacity,
            "nominal_capacity_ah": nominal_capacity,
            "cell_count": cell_count,
            "temp_count": temp_count,
            "temps": temps,
        }
    
    def flatten_temps(self,fields: dict) -> dict:
        temps = fields.get("temps")

        if isinstance(temps, list):
            for i, t in enumerate(temps):
                fields[f"temperature_{i+1}"] = float(t)

            del fields["temps"]

        return fields
    
    def filter_fields(self, fields: dict, patterns: list[str]) -> dict:
        if not patterns:
            return fields

        out = {}

        for k, v in fields.items():
            for pattern in patterns:
                if fnmatch.fnmatch(k, pattern):
                    out[k] = v
                    break

        return out

    def callback(self, sender: int, data: bytearray):

        # Append to buffer
        self.buffer.extend(data)

        while True:

            # Find start byte
            start = self.buffer.find(b'\xDD')
            if start == -1:
                self.buffer.clear()
                return

            # Remove garbage before start
            if start > 0:
                self.buffer = self.buffer[start:]

            # Need at least a possible frame
            if len(self.buffer) < 8:
                return

            # Find end byte, 77
            try:
                end = self.buffer.index(b'\x77', 1)
            except ValueError:
                return

            frame = self.buffer[:end + 1]
            self.buffer = self.buffer[end + 1:]

            # Parse and write to influx
            parsed = self.parse(frame)

            if parsed:
                # Debug print
                if self.debug:
                    print(f"[DATA] {self.name}: {parsed}")

                # Temps are a list, influx do not like
                parsed = self.flatten_temps(parsed)

                # Rm unallowed fields
                allowed_fields = self.allowed_fields
                parsed = self.filter_fields(parsed, allowed_fields)

                write_to_influx(
                    write_api,
                    bucket=bucket,
                    org=org,
                    name=self.name,
                    mac=self.mac,
                    fields=parsed,
                )

    async def handle_device(self):
        print(f"Handling device: {self.name} @ {self.mac}")

        while True:
            try:
                async with BleakClient(self.mac) as client:
                    print("Connected")

                    await client.start_notify(self.notify_uuid, self.callback)

                    # poll battery
                    while True:
                        await client.write_gatt_char(
                            self.write_uuid,
                            self.REQ_ALL,
                            response=False
                        )

                        await asyncio.sleep(self.log_period)

            except Exception as e:
                print(f"[RECONNECT] {self.name}: {e}")
                await asyncio.sleep(5)


# -----------------------------
# Main
# -----------------------------
async def main():
    print(f"Starting {NAME} --> Influxdb")
    print(f"Devices: {len(devices)}")

    tasks = []
    for device in devices:

        if device.get("type") == "jbd":
            device = JbdDevice(device)
            tasks.append(asyncio.create_task(device.handle_device()))

        # TODO: Add other device types here
        # ...

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())