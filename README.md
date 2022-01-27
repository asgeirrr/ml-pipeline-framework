# Project structure

All code is stored in `mlpipeline` folder, which contains the main runner `pipeline_cli.py`, `pipeline`, `custom` and `utils` folders. `pipeline` folder contains the main components, which are `pipeline.py`, `component.py` and `pipeline_builder.py`.

`custom` folder is for user defined components. `utils` for any helper functions.

`doc` - documentation, `tests` for unittests, `data` for yaml configs/

# Important assumptions

 - Configurated pipeline must have a name and at least one component. Otherwise the runner will raise RuntimeError.
 - The pipeline and components might have missing inputs and outputs
 - The component shouldn't have circular references (the running order shouldn't contain a cycle)

# Workflow

`pipeline_cli` reads command line arguments, where config file must be present and inputs are optional. `pipeline_builder.py`
reads the config file, parses it into components objects, verifies inputs, outputs and links between components before any of processing is executed (as it might be expensive). It creates dependency graph and creates the execution order of components. `pipeline.py` object is the result of its computation. Then the `pipeline` object executes components in running order and outputs the result.

The `data/` folder contains legit (`pipeline_0,1,2`) configurations and faulty ones.

# Installation/running

Installation

```python3 setup.py install```

Execution

```pipeline_cli --file "data/pipeline_0.yaml" --inputs document_id=D0 page_num=0```

Testing

```python3 -m unittest tests/*.py```
