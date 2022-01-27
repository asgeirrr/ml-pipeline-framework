import logging
from ..utils import get_pipeline_message

##
L = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
##


class Pipeline:
    """
    Executes components in topological order and obtains the result

    ...

    Methods
    -------
    execute(input: dict)
        obtains result
    """

    def __init__(self, name: str, inputs: set, outputs: set, components: dict, running_order: list):
        """
        Parameters
        ----------
        name : str
            Human readable name of the pipeline
        inputs : set
            input keys
        output : set
            output keys
        components : dict
            all components component_id -> component
        running_order : list
            component ids in topological order
        """

        self.name = name
        self.inputs = inputs
        self.outputs = outputs
        self.components = components
        self.running_order = running_order

    def _verify_inputs(self, inputs: dict) -> bool:
        """Verifies the input from cli if it matches the input expected in pipeline

        Parameters
        ----------
        inputs : dict
            inputs from cli

        Returns
        -------
        bool
            if correct
        """

        inputs_keys = set(inputs.keys())
        if len(inputs_keys & self.inputs) != len(self.inputs):
            return False

        return True

    def execute(self, inputs: dict) -> dict:
        """Executes components one by one in topological order

        Parameters
        ----------
        inputs : dict
            inputs from cli

        Returns
        -------
        dict
            outputs

        Raises
        ------
        RuntimeError
            If inputs passed from cli don"t match the pipeline inputs
        """

        if not self._verify_inputs(inputs):
            L.error("Incorrect inputs from command line, shutting down")
            raise RuntimeError("Inputs passed from command line don't match specified inputs")

        result = {}
        result.update(inputs)
        L.info("Starting the {}".format(self.name))
        L.info(get_pipeline_message(inputs, "inputs"))

        for component_id in self.running_order:
            component_result = self.components[component_id].execute(result)
            result[component_id] = component_result

        outputs = self._extract_outputs(result)
        L.info(get_pipeline_message(outputs, "outputs"))
        return outputs

    def _extract_outputs(self, result: dict) -> dict:
        """Extracts outputs from result dictionary

        Parameters
        ----------
        result : dict
            All inputs an outputs from processing

        Returns
        -------
        dict
            output results
        """

        outputs = {}
        for output_key in self.outputs:
            output_key_split = output_key.split(".")
            if len(output_key_split) == 1:
                outputs[output_key] = result[output_key]
            else:
                outputs[output_key] = result[output_key_split[0]][output_key_split[1]]
        return outputs

    def __str__(self):
        return """
            name: {}
            inputs: {}
            outputs: {}
            components: {}
        """.format(self.name, self.inputs, self.outputs, self.components)

    def __eq__(self, other):
        if not isinstance(other, Pipeline):
            return NotImplemented

        return self.name == other.name and self.inputs == other.inputs and self.outputs == other.outputs and self.components == other.components
