import json
import sys

import requests

from common import CONNECT, CONNECTOR, log, wait_for

L = log("connector")


def register():
    config = json.load(open("/app/connect/shop-cdc.json"))
    r = requests.put(f"{CONNECT}/connectors/{CONNECTOR}/config", json=config, timeout=30)
    r.raise_for_status()
    wait_for(lambda: running(), 120, what="connector RUNNING")
    L.info("connector %s running", CONNECTOR)


def status():
    return requests.get(f"{CONNECT}/connectors/{CONNECTOR}/status", timeout=10).json()


def running():
    st = status()
    return st["connector"]["state"] == "RUNNING" and st["tasks"] and all(t["state"] == "RUNNING" for t in st["tasks"])


if __name__ == "__main__":
    {"register": register, "status": lambda: print(json.dumps(status(), indent=2))}[sys.argv[1]]()
