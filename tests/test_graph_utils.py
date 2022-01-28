import unittest
from mlpipeline.pipeline import Component, GraphUtils


class TestGraphUtils(unittest.TestCase):
    def test_create_graph(self):
        components = {
            "a": Component("a", {"inputs": ["input0", "input1"], "outputs": ["output0"]}),
            "b": Component("b", {"inputs": ["input0", "input1"], "outputs": ["output1"]}),
            "c": Component("c", {"inputs": ["a.output0", "b.output1"], "outputs": ["output1"]}),
        }
        graph_utils = GraphUtils()
        expected_graph = {"inputs": {"b", "a"}, "b": {"c"}, "a": {"c"}, "c": {"outputs"}, "outputs": set()}
        output_graph = graph_utils._create_graph(components, set(["input0", "input1"]), set(["c.output1"]))
        self.assertEqual(expected_graph, output_graph)

        components = {
            "a": Component("a", {"inputs": [], "outputs": ["output0"]}),
            "c": Component("c", {"inputs": ["a.output0"], "outputs": ["output1"]}),
        }
        expected_graph = {"a": {"c"}, "c": {"outputs"}, "outputs": set()}
        output_graph = graph_utils._create_graph(components, set(), set(["c.output1"]))
        self.assertEqual(expected_graph, output_graph)

    def test_get_topological_order(self):
        graph = {
            "inputs": set(["a", "b"]),
            "a": set(["c"]),
            "b": set(["c"]),
            "c": set(["outputs"]),
            "outputs": set([])
        }
        graph_utils = GraphUtils()
        running_order = graph_utils._get_topological_order(graph)
        expected_running_order_reduced = [None, None, "c"]
        self.assertEqual(expected_running_order_reduced[2], running_order[2])

        graph = {
            "inputs": set(["a", "b"]),
            "a": set(["b"]),
            "b": set(["c"]),
            "c": set(["a"]),
            "outputs": set([])
        }
        with self.assertRaises(RuntimeError):
            graph_utils._get_topological_order(graph)

    def test_verify_edge(self):
        graph_utils = GraphUtils()
        components = {
            "a": Component("a", {"inputs": [], "outputs": ["output0"]}),
            "c": Component("c", {"inputs": ["a.output0"], "outputs": ["output1"]}),
        }

        self.assertTrue(graph_utils._verify_edge(components, set(), set(), "a", "c"))

        components = {
            "a": Component("a", {"inputs": [], "outputs": ["output0"]}),
            "c": Component("c", {"inputs": ["a.output1"], "outputs": ["output1"]}),
        }
        self.assertFalse(graph_utils._verify_edge(components, set(), set(), "a", "c"))

        components = {
            "a": Component("a", {"inputs": ["input0"], "outputs": ["output0"]})
        }
        self.assertTrue(graph_utils._verify_edge(components, set(["input0", "input1"]), set(), "inputs", "a"))

        components = {
            "a": Component("a", {"inputs": ["input2"], "outputs": ["output0"]})
        }
        self.assertFalse(graph_utils._verify_edge(components, set(["input0", "input1"]), set(), "inputs", "a"))
