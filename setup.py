from setuptools import setup

setup(
    name="mlpipelineframework",
    version="0.0.0",
    author="Rita Argirova",
    author_email="argirova.rita@gmail.com",
    url="https://github.com/ty-norm/ml-pipeline-framework",
    description="ML pipeline parser and executor",
    packages=["tests", "mlpipeline", "mlpipeline.custom", "mlpipeline.pipeline", "mlpipeline.utils"],
    entry_points={
        "console_scripts": [
            "pipeline_cli = mlpipeline.pipeline_cli:main"
        ]
    },
    install_requires=[
        "pyyaml",
        "numpy",
        "importlib"
    ],
)
