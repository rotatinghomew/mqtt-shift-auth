"""检查合成观测、来源边界及本地传输。"""
import json
from pathlib import Path
from http.server import ThreadingHTTPServer
from threading import Thread
import unittest
from urllib.error import HTTPError
from urllib.request import urlopen

from source_reader import Source, handler_for


ROOT = Path(__file__).resolve().parents[1]
DATA = Source(ROOT / "inputs")


class ObservationsTest(unittest.TestCase):
    def test_available_sources(self):
        self.assertEqual(DATA.names(), EXPECTED_SOURCES)
        for source in EXPECTED_SOURCES:
            records = DATA.fetch(source)
            self.assertTrue(records, source)
            self.assertTrue(all(isinstance(row, dict) for row in records.values()), source)

    def test_project_references(self):
        for a, ar, af, b, br, bf, agree in FACTS:
            with self.subTest(reference=(a, ar, af, b, br, bf)):
                first = DATA.fetch(a, ar)[af]
                second = DATA.fetch(b, br)[bf]
                self.assertEqual(first == second, agree)

        for origin, record, field, candidates in REFERENCES:
            for target in DATA.fetch(origin, record)[field]:
                with self.subTest(origin=origin, target=target):
                    present = [name for name in candidates if target in DATA.fetch(name)]
                    self.assertEqual(len(present), 1)

    def test_loopback_delivers_raw_record(self):
        name, reference = ENDPOINT_SAMPLE
        with ThreadingHTTPServer(("127.0.0.1", 0), handler_for(DATA)) as server:
            thread = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
            thread.start()
            base = f"http://127.0.0.1:{server.server_port}/observations"
            try:
                with urlopen(base + f"/{name}/{reference}", timeout=3) as response:
                    self.assertEqual(response.status, 200)
                    self.assertEqual(json.load(response), DATA.fetch(name, reference))
                with self.assertRaises(HTTPError) as caught:
                    urlopen(base + f"/{name}/missing-record", timeout=3)
                with caught.exception as error:
                    self.assertEqual(error.code, 404)
            finally:
                server.shutdown()
                thread.join(timeout=3)


EXPECTED_SOURCES = ['deliveries', 'devices', 'messages', 'subscriptions']
ENDPOINT_SAMPLE = ('devices', 'sensor-old')
FACTS = [
    ('devices', 'sensor-old', 'client_name', 'devices', 'sensor-new', 'client_name', True),
    ('devices', 'sensor-old', 'tenant', 'devices', 'sensor-new', 'tenant', False),
    ('devices', 'watch-next', 'instance', 'subscriptions', 'second', 'instance', True),
    ('deliveries', 'maint-send', 'packet_id', 'deliveries', 'qual-ack', 'packet_id', True),
    ('deliveries', 'maint-send', 'group', 'deliveries', 'qual-ack', 'group', False),
    ('deliveries', 'maint-send', 'connection', 'deliveries', 'maint-ack', 'connection', True),
    ('messages', 'latest-red', 'topic', 'messages', 'latest-blue', 'topic', True),
]
REFERENCES = [
]
