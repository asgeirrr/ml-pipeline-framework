import importlib
import logging
import typing
import yaml
from .component import Component

##
L = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
##


class ConfigParser:
    """
    Reads config file and parses componets.

    ...

    Methods
    -------
    parse_config(config_path: str, components_module: str)
        returns name, inputs, outsputs, components for the pipeline
    """

    def _read_config(self, config_path: str) -> dict:
        """ Reads config yaml file and creates a dictionary

        Parameters
        ----------
        config_path : str
            A path to configuration of pipeline and components

        Returns
        -------
        dict
            config
        """

        with open(config_path) as fp:
            return yaml.safe_load(fp)

    def _parse_components(self, components_definition: dict, components_module: str) -> dict:
        """Creates componenents from configuration.

        Parameters
        ----------
        components_definition : dict
            A dictionary with raw components info from configuration

        Returns
        -------
        dict
            component_id -> component
        """

        components = {}
        for component_name in components_definition:
            components[component_name] = self._build_component(component_name, components_definition[component_name], components_module)

        return components

    def _build_component(self, component_name: str, component_definition: dict, components_module: str) -> Component:
        """Creates single component from its" configuration.

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
        """

        module = importlib.import_module(components_module)
        component_class = getattr(module, component_definition["runner"])
        return component_class.construct(component_name, component_definition)

    def _verify_inputs(self, inputs: set) -> bool:
        """Checks if pipeline inputs don"t contain references to a component

        Parameters
        ----------
        inputs : set
            Pipeline inputs

        Returns
        -------
        bool
            if correct
        """

        for input_ in inputs:
            if len(input_.split(".")) > 1:
                return False
        return True

    def parse_config(self, config_path: str, components_module: str) -> typing.Tuple[str, set, set, dict]:
        """Parses the config

        Parameters
        ----------
        config_path : str
            Path to yaml config
        components_module : str
            Module where user defined components are stored

        Returns
        -------
        str
            name
        set
            inputs
        set
            outputs
        dict
            componets
        """

        config = self._read_config(config_path)["pipeline"]
        if "name" not in config:
            L.error("No pipeline name, shutting down")
            raise RuntimeError("There are no pipeline name specified in config")

        name = config["name"]
        inputs = config.get("inputs")
        if not inputs:
            inputs = []

        inputs = set(inputs)

        outputs = config.get("outputs")
        if not outputs:
            outputs = []

        outputs = set(outputs)

        if "components" not in config:
            L.error("No components, shutting down")
            raise RuntimeError("There are no components specified in config")

        components_definintion = config["components"]
        components = self._parse_components(components_definintion, components_module)

        if not self._verify_inputs(inputs):
            L.error("Incorrect inputs, shutting down")
            raise RuntimeError("Incorrect inputs")

        return name, inputs, outputs, components
