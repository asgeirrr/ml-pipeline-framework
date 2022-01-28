import unittest
from mlpipeline.pipeline import PipelineBuilder, Pipeline
from mlpipeline.custom import ImagePreprocessor, OCRModel2, ExtractionModel


class TestPipelineBuilder(unittest.TestCase):
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
