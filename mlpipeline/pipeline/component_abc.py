import abc


class ComponentABC(abc.ABC):
    """
    Generic component class. Should be exetended
    """

    def __init__(self, component_id: str, component_definition: dict):
        """
        Parameters
        ----------
        component_id : str
            name of the component from config
        component_definition : dict
            raw info about the component from config
        """

        self.name = component_id

        inputs = component_definition.get("inputs")
        if not inputs:
            inputs = []
        self.inputs = set(inputs)

        outputs = component_definition.get("outputs")
        if not outputs:
            outputs = []
        self.outputs = set(outputs)

    @abc.abstractmethod
    def execute(self, result):
        raise NotImplementedError()

    @abc.abstractmethod
    def process(self, inputs):
        raise NotImplementedError()

    @classmethod
    def construct(cls, name: str, definition: dict):
        return cls(name, definition)
