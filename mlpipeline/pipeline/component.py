import logging
import time
from ..utils import get_keys_values

##
L = logging.getLogger(__name__)
L.setLevel(logging.DEBUG)
logging.basicConfig(level=logging.INFO)
L.info('Started')
##


class Component:
    '''
    Generic component class. Should be exetended
    ...

    Methods
    -------
    execute(result: dict)
        computes outputs of the processing of components

    process(inputs: dict)
        component logic, should be defined
    '''

    def __init__(self, component_id: str, component_definition: dict):
        '''
        Parameters
        ----------
        component_id : str
            name of the component from config
        component_definition : dict
            raw info about the component from config
        '''

        self.name = component_id
        self.inputs = set(component_definition["inputs"])
        self.outputs = set(component_definition["outputs"])
        self.dependencies = self._get_dependencies()

    def _get_dependencies(self) -> set:
        '''Parses inputs and extracts dependencies

        Returns
        -------
        set
            dependant component ids

        Raises
        ------
        RuntimeError
            If inputs passed don't match the pattern <component_id>.<input_id>
        '''

        dependencies = set()
        for input_ in self.inputs:
            split_input = input_.split(".")
            if len(split_input) == 1:
                dependencies.add("inputs")
            elif len(split_input) == 2:
                dependencies.add(split_input[0])
            else:
                raise RuntimeError("Incorrect input {}, should be <component_id>.<input_name>".format(input_))
        return dependencies

    def execute(self, result: dict) -> dict:
        '''Executes pipeline components one by one in topological order.

        Parameters
        ----------
        result : dict
            All inputs an outputs from processing

        Returns
        -------
        dict
            output results
        '''

        inputs = self._extract_inputs(result)
        outputs = self.process(inputs)
        L.info(self._get_message(inputs, outputs))
        return outputs

    def _extract_inputs(self, result: dict) -> dict:
        '''Extracts inputs from the previous computation in a proper format

        Parameters
        ----------
        result : dict
            All inputs an outputs from processing

        Returns
        -------
        dict
            inputs
        '''

        inputs = {}
        for input_key in self.inputs:
            input_key_split = input_key.split(".")
            if len(input_key_split) == 1:
                inputs[input_key_split[0]] = result[input_key_split[0]]
            else:
                inputs[input_key_split[1]] = result[input_key_split[0]][input_key_split[1]]

        return inputs

    def process(self, inputs: dict):
        '''To be defined
        '''

        pass

    def _get_message(self, inputs: dict, outputs: dict) -> str:
        '''Log message formatting
        '''

        message = "{}: {} - {}: inputs - {} : outputs - {}".format(
            int(time.time()), self.name, self.__class__.__name__,
            get_keys_values(inputs), get_keys_values(outputs)
        )
        return message

    def __str__(self):
        return "name: {}, inputs: {}, outputs: {}".format(self.name, self.inputs, self.outputs)

    def __eq__(self, other):
        if not isinstance(other, Component):
            return NotImplemented

        return self.name == other.name and self.inputs == other.inputs and self.outputs == other.outputs and self.dependencies == other.dependencies

    @classmethod
    def construct(cls, name: str, definition: dict):
        return cls(name, definition)
