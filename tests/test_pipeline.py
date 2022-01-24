import unittest
from mlpipeline.pipeline import Pipeline, PipelineBuilder


class TestPipeline(unittest.TestCase):
    def test_execute(self):
        path = "data/pipeline_1.yaml"
        pipeline_builder = PipelineBuilder("mlpipeline.custom")
        pipeline = pipeline_builder.build_pipeline(path)
        self.assertEqual(set(pipeline.execute({"input_0": 0, "input_1": 1}).keys()), set(["test_processor_3.output_3"]))

    def test_get_running_order(self):
        pipeline = Pipeline("test", set(), set(), dict(), [])
        self.assertEqual(pipeline._get_running_order(["inputs", "test_0", "test_1", "outputs"]), ["test_0", "test_1"])

        pipeline = Pipeline("test", set(), set(), dict(), [])
        self.assertEqual(pipeline._get_running_order(["test_0", "test_1", "outputs"]), ["test_0", "test_1"])

    def test_verify_inputs(self):
        pipeline = Pipeline("test", set(["a", "b"]), set(), dict(), [])
        inputs = {"a": 0, "b": 1}
        self.assertTrue(pipeline._verify_inputs(inputs))

        inputs = {"a": 1}
        self.assertFalse(pipeline._verify_inputs(inputs))

    def test_extract_outputs(self):
        pipeline = Pipeline("test", set(), set(["a.0", "b.1"]), dict(), [])
        result = {"input_0": 0, "input_1": 1, "a": {"0": 0, "1": 2}, "b": {"1": 1}}
        self.assertEqual(pipeline._extract_outputs(result), {"a.0": 0, "b.1": 1})

        pipeline = Pipeline("test", set(), set(), dict(), [])
        result = {"input_0": 0, "input_1": 1, "a": {"0": 0, "1": 2}, "b": {"1": 1}}
        self.assertEqual(pipeline._extract_outputs(result), {})
