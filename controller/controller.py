import time

from confluent_kafka.admin import AdminClient, NewTopic
from prometheus_client import start_http_server

from common import KAFKA, log
from controller import schema, supervisor

L = log("controller")


def ensure_dlq():
    admin = AdminClient({"bootstrap.servers": KAFKA})
    if "cdc.dlq" not in admin.list_topics(timeout=10).topics:
        admin.create_topics([NewTopic("cdc.dlq", 1, 1)])["cdc.dlq"].result()


def main():
    start_http_server(8001)
    ensure_dlq()
    while True:
        try:
            halted, created = schema.sync()
            supervisor.tick(halted, created)
            supervisor.connect_tick()
        except Exception:
            L.exception("tick failed")
        time.sleep(5)


if __name__ == "__main__":
    main()
