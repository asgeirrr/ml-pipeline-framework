import unittest
from mlpipeline.pipeline import ConfigParser, Component


class TestConfigParser(unittest.TestCase):
    def test_read_config(self):
        path = "data/test_0.yaml"
        config_parser = ConfigParser()
        config = config_parser._read_config(path)
        config_expected = {
            "pipeline": {
                "name": "test pipeline",
                "inputs": ["a"],
                "outputs": ["b.c"],
                "components": {
                    "b": {
                        "runner": "B",
                        "inputs": ["a"],
                        "outputs": ["c"]
                    }
                }
            }
        }
        self.assertEqual(config, config_expected)

    def test_parse_components(self):
        components = {
            "test_component": {
                "runner": "TestComponent",
                "inputs": ["a"],
                "outputs": ["b"]
            },
            "another_test_component": {
                "runner": "AnotherTestComponent",
                "inputs": ["test_component.b"],
                "outputs": ["c"]
            }
        }
        expected_output = {
            "test_component": TestComponent("test_component", {
                "runner": "TestComponent",
                "inputs": ["a"],
                "outputs": ["b"]
            }),
            "another_test_component": AnotherTestComponent("another_test_component", {
                "runner": "AnotherTestComponent",
                "inputs": ["test_component.b"],
                "outputs": ["c"]
            }),
        }
        config_parser = ConfigParser()
        output_components = config_parser._parse_components(components, "tests.test_config_parser")
        self.assertEqual(output_components, expected_output)

    def test_verify_inputs(self):
        config_parser = ConfigParser()
        self.assertFalse(config_parser._verify_inputs(set(["a.test"])))
        self.assertTrue(config_parser._verify_inputs(set(["a", "b"])))

    def test_parse_config(self):
        config_parser = ConfigParser()
        with self.assertRaises(RuntimeError):
            config_parser.parse_config("data/missing.yaml", "mlpipeline.custom")


class TestComponent(Component):
    pass


class AnotherTestComponent(Component):
    pass
