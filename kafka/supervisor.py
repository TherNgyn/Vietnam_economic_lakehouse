import os
import sys
import time
import subprocess
import signal
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("kafka-supervisor")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
RESTART_DELAY = int(os.getenv("SUPERVISOR_RESTART_DELAY", "10"))

PROCESSES = [
    # ("producer_unified",    ["python", "/app/kafka/producer_unified.py"]),
    ("producer_index",      ["python", "/app/kafka/producer_index.py"]),
    # ("consumer_bronze",     ["python", "/app/kafka/consumer_bronze_group.py"]),
    ("consumer_influxdb",   ["python", "/app/kafka/consumer_influxdb_group.py"]),
]

_children: dict[str, subprocess.Popen] = {}
_stopping = False


def wait_for_kafka(timeout: int = 120):
    import socket
    host, port = KAFKA_BROKER.rsplit(":", 1)
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, int(port)), timeout=5):
                log.info("Kafka broker reachable at %s", KAFKA_BROKER)
                return
        except OSError:
            log.info("Waiting for Kafka broker %s …", KAFKA_BROKER)
            time.sleep(5)
    raise RuntimeError(f"Kafka broker {KAFKA_BROKER} not reachable after {timeout}s")


def setup_topics():
    topics = [
        ("ticker-realtime",       4),
        ("index-world-realtime",  4),
        ("product-realtime",      4),
        ("index-realtime",        1),
    ]
    for topic, partitions in topics:
        cmd = [
            "kafka-topics",
            "--bootstrap-server", KAFKA_BROKER,
            "--create", "--if-not-exists",
            "--topic", topic,
            "--partitions", str(partitions),
            "--replication-factor", "1",
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            log.info("Topic ready: %s", topic)
        else:
            log.warning("Topic setup warning (%s): %s", topic, result.stderr.strip())


def start_process(name: str, cmd: list) -> subprocess.Popen:
    env = {**os.environ}
    proc = subprocess.Popen(cmd, env=env, stdout=sys.stdout, stderr=sys.stderr)
    log.info("Started %s (pid=%d)", name, proc.pid)
    return proc


def stop_all():
    global _stopping
    _stopping = True
    for name, proc in _children.items():
        if proc.poll() is None:
            log.info("Terminating %s (pid=%d)", name, proc.pid)
            proc.terminate()
    for name, proc in _children.items():
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
    log.info("All processes stopped")


def handle_signal(signum, frame):
    log.info("Signal %d received, shutting down …", signum)
    stop_all()
    sys.exit(0)


def main():
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    wait_for_kafka()

    try:
        setup_topics()
    except Exception as e:
        log.warning("Topic setup failed (topics may already exist): %s", e)

    time.sleep(3)

    for name, cmd in PROCESSES:
        _children[name] = start_process(name, cmd)

    while not _stopping:
        time.sleep(5)
        for name, cmd in PROCESSES:
            proc = _children.get(name)
            if proc is None:
                continue
            ret = proc.poll()
            if ret is not None:
                log.warning("%s exited (code=%d), restarting in %ds …", name, ret, RESTART_DELAY)
                time.sleep(RESTART_DELAY)
                _children[name] = start_process(name, cmd)


if __name__ == "__main__":
    main()
