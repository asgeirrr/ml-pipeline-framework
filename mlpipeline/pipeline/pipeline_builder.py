import logging

from .pipeline import Pipeline
from .config_parser import ConfigParser
from .graph_utils import GraphUtils

##
L = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
##


class PipelineBuilder:
    """
    Reads config file and builds the pipeline.

    ...

    Methods
    -------
    build_pipeline(config_path: str)
        Creates pipeline from given config
    """

    def __init__(self, components_module: str):
        """
        Parameters
        ----------
        components_module : str
            A path where components are defined
        """

        self._components_module = components_module

    def build_pipeline(self, config_path: str) -> Pipeline:
        """Builds the pipeline from configuration

        Parameters
        ----------
        config_path : str
            Path to config

        Returns
        -------
        Pipeline
            executable pipeline
        """
        parser = ConfigParser()
        name, inputs, outputs, components = parser.parse_config(config_path, self._components_module)

        graph_utils = GraphUtils()
        running_order = graph_utils.get_running_order(components, inputs, outputs)

        return Pipeline(name, inputs, outputs, components, running_order)
