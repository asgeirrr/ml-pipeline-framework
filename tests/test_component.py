import unittest
from mlpipeline.pipeline import Component


class TestComponent(unittest.TestCase):
    def test_get_dependencies(self):
        component = Component("test_component", {"inputs": ["test_0", "test_component_1.test_1"], "outputs": []})
        expected_deps = set(["inputs", "test_component_1"])
        self.assertEqual(component._get_dependencies(), expected_deps)

        component = Component("test_component", {"inputs": [], "outputs": []})
        expected_deps = set([])
        self.assertEqual(component._get_dependencies(), expected_deps)

    def test_extract_inputs(self):
        component = Component("test_component", {"inputs": ["test_0", "test_component_1.test_1"], "outputs": []})
        result = {
            "test_0": 0,
            "test_component_1": {
                "test_1": 1,
                "test_2": 2
            }
        }
        self.assertEqual(component._extract_inputs(result), {"test_1": 1, "test_0": 0})
