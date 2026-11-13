import hashlib
import json
import unittest

from field_observatory.client import Client
from field_observatory.decoding import parse_page, records as decode_records
from field_observatory.origin import load_scenario, serve


class SourceContractTests(unittest.TestCase):
    def test_domain_observations_preserve_source_distinctions(self):
        scenario = load_scenario()
        records = scenario["source_records"]
        with serve(scenario) as origin:
            client = Client(origin.base_url)
            manifest = records[0]
            self.assertEqual(manifest["generation"], 2)
            for chunk in manifest["chunks"]:
                response = client.fetch(chunk["path"])
                self.assertEqual(hashlib.sha256(response.body).hexdigest(), chunk["sha256"])
            self.assertNotEqual(records[0]["generation"], records[1]["generation"])

    def test_every_record_has_the_declared_source_identity(self):
        scenario = load_scenario()
        for record in scenario["source_records"]:
            for field in scenario["identity_fields"]:
                self.assertIn(field, record)
                self.assertNotEqual(str(record[field]).strip(), "")


if __name__ == "__main__":
    unittest.main()
