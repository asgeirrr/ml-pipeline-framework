import logging
import yaml
import importlib

from .pipeline import Pipeline
from .component import Component

##
L = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
##


class PipelineBuilder:
    '''
    Reads config file and builds the pipeline.

    ...

    Methods
    -------
    build_pipeline(config_path: str)
        Creates pipeline from given config
    '''

    def __init__(self, components_module: str):
        '''
        Parameters
        ----------
        components_module : str
            A path where components are defined
        '''

        self._components_module = components_module

    def _read_config(self, config_path: str) -> dict:
        ''' Reads config yaml file and creates a dictionary

        Parameters
        ----------
        config_path : str
            A path to configuration of pipeline and components

        Returns
        -------
        dict
            config
        '''

        with open(config_path) as fp:
            return yaml.safe_load(fp)

    def _parse_components(self, components_definition: dict) -> dict:
        '''Creates componenents from configuration.

        Parameters
        ----------
        components_definition : dict
            A dictionary with raw components info from configuration

        Returns
        -------
        dict
            component_id -> component
        '''

        components = {}
        for component_name in components_definition:
            components[component_name] = self._build_component(component_name, components_definition[component_name])

        return components

    def _build_component(self, component_name: str, component_definition: dict) -> Component:
        '''Creates single omponenent from its' configuration.

        Parameters
        ----------
        component_name : str
            Name of the component
        component_definition : dict
            A dictionary with raw component info from configuration

        Returns
        -------
        Component
            a component object
        '''

        module = importlib.import_module(self._components_module)
        component_class = getattr(module, component_definition["runner"])
        return component_class.construct(component_name, component_definition)

    def _create_graph(self, components: dict, inputs: set, outputs: set) -> dict:
        '''Creates the dependency graph between components, where node is component id and edge represents the link between them.

        Parameters
        ----------
        components : dict
            The components info

        inputs: set
            The set of pipeline inputs
        outputs: set
            The set of pipeline outputs

        Returns
        -------
        dict
            graph representation of components

        Raises
        ------
        RuntimeError
            If the inputs of one component don't match with the output of the component it depends on
            OR inputs and outputs in incorrect format
        '''

        graph = {}
        for component_id in components:
            dependencies = components[component_id].dependencies
            if component_id not in graph:
                graph[component_id] = set()

            for dependency in dependencies:
                if dependency not in graph:
                    graph[dependency] = set()

                if self._verify_edge(components, inputs, outputs, dependency, component_id):
                    graph[dependency].add(component_id)
                else:
                    L.error("Incorrect graph, shutting down")
                    raise RuntimeError("Incorrect link from {} to {}, inputs don't match the outputs".format(dependency, component_id))

        if len(outputs) > 0:
            graph["outputs"] = set()

        for output in outputs:
            output_split = output.split(".")
            if len(output_split) == 1:
                if output_split not in inputs:
                    L.error("Incorrect outputs, shutting down")
                    raise RuntimeError("Incorrect outputs {}".format(outputs))

                graph["inputs"].add("outputs")

            elif len(output_split) == 2:
                if self._verify_edge(components, inputs, outputs, output_split[0], "outputs"):
                    graph[output_split[0]].add("outputs")
                else:
                    L.error("Incorrect outputs, shutting down")
                    raise RuntimeError("Incorrect outputs {}".format(outputs))

            else:
                L.error("Incorrect inputs, shutting down")
                raise RuntimeError("Incorrect input {}, should be <component_id>.<input_name>".format(output))

        return graph

    def _get_running_order(self, graph: dict) -> list:
        '''Gets the dependency graph topologically sorted and creates an order of components to be executed in optimal order.
        Checks if the graph contains a cycle (if output of one component is referenced somewhere in its' deps)

        Parameters
        ----------
        graph : dict
            Components dependency graph

        Returns
        -------
        list
            running order of the graph

        Raises
        ------
        RuntimeError
            If the graph contains a cycle
        '''

        visited = set()
        running_order = []
        for component_id in graph:
            if component_id not in visited:
                self._get_running_order_util(component_id, graph, visited, running_order)

        if not self._check_cycle(running_order[:], graph):
            running_order.reverse()
        else:
            L.error("Incorrect graph, shutting down")
            raise RuntimeError("The component pipeline contains cycle")

        return running_order

    def _get_running_order_util(self, component_id: str, graph: dict, visited: set, running_order: list):
        '''Helper function for _get_running_order
        '''

        visited.add(component_id)

        for neighbor in graph[component_id]:
            if neighbor not in visited:
                self._get_running_order_util(neighbor, graph, visited, running_order)

        running_order.append(component_id)

    def _check_cycle(self, running_order_reversed: list, graph: dict) -> bool:
        '''Checks if the dependency graph contains a cycle

        Parameters
        ----------
        running_order_reversed : list
            Stack of visited nodes
        graph : dict
            Components dependency graph

        Returns
        -------
        bool
            if contains a cycle
        '''

        top_order = dict()
        index = 0
        while (len(running_order_reversed) != 0):
            top_order[running_order_reversed[-1]] = index
            index += 1
            running_order_reversed.pop()

        for component_id in graph:
            for neighbor in graph[component_id]:
                first = 0 if component_id not in top_order else top_order[component_id]
                second = 0 if neighbor not in top_order else top_order[neighbor]
                if (first > second):
                    return True

        return False

    def _verify_edge(self, components: dict, inputs: set, outputs: set, from_id: str, to_id: str) -> bool:
        '''Checks if an edge is correct (input of one component matches the output of its dependency)

        Parameters
        ----------
        components : dict
            All components
        inputs : set
            Pipeline inputs
        outputs : set
            Pipeline outputs
        from_id : str
            Id of previous component
        to_id : str
            Id of next component

        Returns
        -------
        bool
            if correct
        '''

        if from_id == 'inputs':
            outputs_from = inputs
        else:
            if from_id not in components:
                return False

            outputs_from = components[from_id].outputs

        if to_id == 'outputs':
            inputs_to = outputs
        else:
            if to_id not in components:
                return False

            inputs_to = components[to_id].inputs

        for inpt in inputs_to:
            split_inpt = inpt.split(".")
            if (len(split_inpt) == 1) and (split_inpt[0] not in outputs_from):
                return False

            if (len(split_inpt) == 2) and (split_inpt[0] == from_id) and (split_inpt[1] not in outputs_from):
                return False

        return True

    def _verify_inputs(self, inputs: set) -> bool:
        '''Checks if pipeline inputs don't contain references to a component

        Parameters
        ----------
        inputs : set
            Pipeline inputs

        Returns
        -------
        bool
            if correct
        '''

        for input_ in inputs:
            if len(input_.split(".")) > 1:
                return False
        return True

    def build_pipeline(self, config_path: str) -> Pipeline:
        '''Builds the pipeline from configuration

        Parameters
        ----------
        config_path : str
            Path to config

        Returns
        -------
        Pipeline
            executable pipeline
        '''

        config = self._read_config(config_path)["pipeline"]
        name = config["name"]
        inputs = set(config.get("inputs", []))
        if not self._verify_inputs(inputs):
            L.error("Incorrect inputs, shutting down")
            raise RuntimeError("Incorrect inputs")

        outputs = set(config.get("outputs", []))
        if "components" not in config:
            L.error("No components, shutting down")
            raise RuntimeError("There are no components specified in config")

        components_definintion = config["components"]
        components = self._parse_components(components_definintion)
        graph = self._create_graph(components, inputs, outputs)
        running_order = self._get_running_order(graph)
        return Pipeline(name, inputs, outputs, components, running_order)
