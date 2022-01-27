import logging
import numpy as np
from mlpipeline.pipeline import Component

##
L = logging.getLogger(__name__)
##


class ImagePreprocessor(Component):
    RANDOM_VALUES = ["A", "B", "C"]

    def process(self, inputs: dict) -> dict:
        outputs = {}
        for output_key in self.outputs:
            outputs[output_key] = np.random.choice(self.RANDOM_VALUES)
        return outputs


class OCRModel2(Component):
    """Passes input to output
    """

    def process(self, inputs: dict) -> dict:
        outputs = {}
        for output_key in self.outputs:
            outputs[output_key] = inputs[output_key]
        return outputs


class ExtractionModel(Component):
    RANDOM_VALUES = [1, 2, 3]

    def process(self, inputs: dict) -> dict:
        outputs = {}
        for output_key in self.outputs:
            outputs[output_key] = "A" + str(np.random.choice(self.RANDOM_VALUES))
        return outputs


class EmptyComponent(Component):
    def process(self, inputs: dict) -> dict:
        return {}
