import unittest
from mlpipeline.pipeline import PipelineBuilder, Component, Pipeline
from mlpipeline.custom import ImagePreprocessor, OCRModel2, ExtractionModel


class TestPipelineBuilder(unittest.TestCase):
    def test_read_config(self):
        path = "data/test_0.yaml"
        pipeline_builder = PipelineBuilder("")
        config = pipeline_builder._read_config(path)
        config_expected = {
            'pipeline': {
                'name': 'test pipeline',
                'inputs': ['a'],
                'outputs': ['b.c'],
                'components': {
                    'b': {
                        'runner': 'B',
                        'inputs': ['a'],
                        'outputs': ['c']
                    }
                }
            }
        }
        self.assertEqual(config, config_expected)

    def test_parse_components(self):
        components = {
            'test_component': {
                'runner': 'TestComponent',
                'inputs': ['a'],
                'outputs': ['b']
            },
            'another_test_component': {
                'runner': 'AnotherTestComponent',
                'inputs': ['test_component.b'],
                'outputs': ['c']
            }
        }
        expected_output = {
            'test_component': TestComponent('test_component', {
                'runner': 'TestComponent',
                'inputs': ['a'],
                'outputs': ['b']
            }),
            'another_test_component': AnotherTestComponent('another_test_component', {
                'runner': 'AnotherTestComponent',
                'inputs': ['test_component.b'],
                'outputs': ['c']
            }),
        }
        pipeline_builder = PipelineBuilder("tests.test_pipeline_builder")
        output_components = pipeline_builder._parse_components(components)
        self.assertEqual(output_components, expected_output)

    def test_create_graph(self):
        components = {
            "a": Component("a", {"inputs": ["input0", "input1"], "outputs": ["output0"]}),
            "b": Component("b", {"inputs": ["input0", "input1"], "outputs": ["output1"]}),
            "c": Component("c", {"inputs": ["a.output0", "b.output1"], "outputs": ["output1"]}),
        }
        pipeline_builder = PipelineBuilder("")
        expected_graph = {'inputs': {'b', 'a'}, 'b': {'c'}, 'a': {'c'}, 'c': {'outputs'}, 'outputs': set()}
        output_graph = pipeline_builder._create_graph(components, set(["input0", "input1"]), set(["c.output1"]))
        self.assertEqual(expected_graph, output_graph)

        components = {
            "a": Component("a", {"inputs": [], "outputs": ["output0"]}),
            "c": Component("c", {"inputs": ["a.output0"], "outputs": ["output1"]}),
        }
        expected_graph = {'a': {'c'}, 'c': {'outputs'}, 'outputs': set()}
        output_graph = pipeline_builder._create_graph(components, set(), set(["c.output1"]))
        self.assertEqual(expected_graph, output_graph)

    def test_get_running_order(self):
        graph = {
            'inputs': set(['a', 'b']),
            'a': set(['c']),
            'b': set(['c']),
            'c': set(['outputs']),
            'outputs': set([])
        }
        pipeline_builder = PipelineBuilder("")
        running_order = pipeline_builder._get_running_order(graph)
        expected_running_order_reduced = ['inputs', None, None, 'c', 'outputs']
        self.assertEqual(expected_running_order_reduced[0], running_order[0])
        self.assertEqual(expected_running_order_reduced[3], running_order[3])
        self.assertEqual(expected_running_order_reduced[4], running_order[4])

        graph = {
            'inputs': set(['a', 'b']),
            'a': set(['b']),
            'b': set(['c']),
            'c': set(['a']),
            'outputs': set([])
        }
        with self.assertRaises(RuntimeError):
            pipeline_builder._get_running_order(graph)

    def test_verify_edge(self):
        pipeline_builder = PipelineBuilder("")
        components = {
            "a": Component("a", {"inputs": [], "outputs": ["output0"]}),
            "c": Component("c", {"inputs": ["a.output0"], "outputs": ["output1"]}),
        }

        self.assertTrue(pipeline_builder._verify_edge(components, set(), set(), "a", "c"))

        components = {
            "a": Component("a", {"inputs": [], "outputs": ["output0"]}),
            "c": Component("c", {"inputs": ["a.output1"], "outputs": ["output1"]}),
        }
        self.assertFalse(pipeline_builder._verify_edge(components, set(), set(), "a", "c"))

        components = {
            "a": Component("a", {"inputs": ["input0"], "outputs": ["output0"]})
        }
        self.assertTrue(pipeline_builder._verify_edge(components, set(["input0", "input1"]), set(), "inputs", "a"))

        components = {
            "a": Component("a", {"inputs": ["input2"], "outputs": ["output0"]})
        }
        self.assertFalse(pipeline_builder._verify_edge(components, set(["input0", "input1"]), set(), "inputs", "a"))

    def test_build_pipeline(self):
        path = "data/pipeline_0.yaml"
        pipeline_builder = PipelineBuilder("mlpipeline.custom")
        pipeline = pipeline_builder.build_pipeline(path)
        name = "My linear ML pipeline."
        inputs = set(["document_id", "page_num"])
        outputs = set(["extractor.extractions"])
        components = {
            "image_preprocessing": ImagePreprocessor("image_preprocessing", {
                "inputs": ["document_id", "page_num"],
                "outputs": ["page_id"]
            }),
            "image_ocr": OCRModel2("image_ocr", {
                "inputs": ["image_preprocessing.page_id"],
                "outputs": ["page_id"]
            }),
            "extractor": ExtractionModel("extractor", {
                "inputs": ["image_ocr.page_id"],
                "outputs": ["extractions"]
            })
        }
        running_order = ["inputs", "ImagePreprocessor", "OCRModel2", "ExtractionModel", "outputs"]
        expected_pipeline = Pipeline(name, inputs, outputs, components, running_order)
        self.assertEqual(expected_pipeline, pipeline)


class TestComponent(Component):
    pass


class AnotherTestComponent(Component):
    pass
