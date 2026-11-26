import json
import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import collect  # noqa: E402
from test_collect_main import ready, prefect_post  # noqa: E402


def card(validator, issues=(), valid=True, name="model__x"):
    val = {"spec": "model-card", "version": "1", "valid": valid,
           "validator": validator, "issues": list(issues)}
    return {"name": name, "extras": [
        {"key": "other", "value": "x"},
        {"key": "model_card_validation", "value": json.dumps(val)}]}


def github_get(path):
    if "/tags" in path:
        return [{"name": "latest"}, {"name": "v0.7.1"}, {"name": "v0.7.0"}]
    return [{"name": "v1.schema.json"}, {"name": "checks.py"}, {"name": "examples"}]


def api_get(path):
    return {"fdo": {"versions": ["1"]}, "model-card": {"versions": ["1"]}}


def api_post(path, body=None):
    return {"valid": False, "validator": "episerve-specs 0.7.0"}


def ckan_get(path):
    return {"result": {"count": 1, "results": [
        card("episerve-specs 0.7.1", [{"severity": "warning", "message": "secret text",
                                       "path": "a"}])]}}


class ParseTests(unittest.TestCase):
    def test_parse_release_forms(self):
        for t in ("v0.7.1", "0.7.1", "episerve-specs 0.7.1"):
            self.assertEqual(collect.parse_release(t), (0, 7, 1))
        self.assertIsNone(collect.parse_release("latest"))
        self.assertEqual(collect.release_str((0, 7, 1)), "0.7.1")

    def test_release_from_validator_string(self):
        self.assertEqual(collect.parse_release("episerve-specs 0.7.0"), (0, 7, 0))
        self.assertEqual(collect.parse_release("0.7.0"), (0, 7, 0))
        self.assertIsNone(collect.parse_release("something else 1.2"))
        self.assertIsNone(collect.parse_release(None))

    def test_latest_release_ignores_non_release_tags(self):
        tags = ["latest", "v0.10.0", "v0.9.1", "v1.0.0-rc1", "docs"]
        self.assertEqual(collect.latest_release(tags), "0.10.0")
        self.assertIsNone(collect.latest_release(["latest"]))

    def test_schema_versions_sorted(self):
        names = ["v10.schema.json", "v2.schema.json", "README.md", "checks.py"]
        self.assertEqual(collect.schema_versions(names), ["2", "10"])

    def test_catalog_counts_cards_without_validation(self):
        pk = [card("episerve-specs 0.7.1", [{"severity": "warning"}]),
              card("episerve-specs 0.7.0"),
              {"name": "m3", "extras": [{"key": "x", "value": "y"}]},
              {"name": "m4", "extras": [{"key": "model_card_validation",
                                         "value": "{not json"}]}]
        self.assertEqual(collect.catalog_summary(pk),
                         {"releases": ["0.7.0", "0.7.1"], "valid": 2,
                          "total": 4, "warnings": 1})


class CollectTests(unittest.TestCase):
    def test_collect_specs_shape(self):
        s = collect.collect_specs(github_get, api_get, api_post, ckan_get)
        gh = {"release": "0.7.1", "versions": ["1"]}
        api = {"release": "0.7.0", "versions": ["1"]}
        self.assertEqual(s, {"latest": "0.7.1", "rows": [
            {"spec": "model-card", "github": gh, "api": api,
             "catalog": {"releases": ["0.7.1"], "valid": 1, "total": 1, "warnings": 1}},
            {"spec": "fdo", "github": gh, "api": api,
             "catalog": {"recorded": False}}]})

    def test_collect_specs_github_down(self):
        def down(path):
            raise OSError("secret-host")
        s = collect.collect_specs(down, api_get, api_post, ckan_get)
        self.assertIsNone(s["latest"])
        for row in s["rows"]:
            self.assertIsNone(row["github"])
            self.assertEqual(row["api"]["release"], "0.7.0")
        self.assertEqual(s["rows"][0]["catalog"]["total"], 1)

    def test_collect_specs_no_issue_text_leaks(self):
        text = json.dumps(collect.collect_specs(github_get, api_get, api_post, ckan_get))
        self.assertNotIn("secret text", text)
        self.assertNotIn("model__x", text)

    def test_main_writes_specs(self):
        src = collect.Sources(
            k8s_get=ready, prefect_post=prefect_post, alerts_get=lambda: [],
            previous_get=lambda: None, check_url=lambda u: True,
            github_get=github_get, api_get=api_get, api_post=api_post,
            ckan_get=ckan_get)
        with tempfile.TemporaryDirectory() as d:
            out = os.path.join(d, "s.json")
            self.assertEqual(collect.main({"OUTPUT": out}, src), 0)
            with open(out) as f:
                data = json.load(f)
        self.assertEqual(data["specs"]["latest"], "0.7.1")


if __name__ == "__main__":
    unittest.main()
