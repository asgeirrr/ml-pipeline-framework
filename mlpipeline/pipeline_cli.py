import argparse
from .pipeline import PipelineBuilder


def main():
    parser = argparse.ArgumentParser(description='Should parse config file, read the input and run the pipeline')
    parser.add_argument('--file', type=str, required=True, help='Config file path')
    parser.add_argument('--inputs', type=str, help='Input parameters', nargs="+")
    args = parser.parse_args()

    inputs = {}
    for arg in args.inputs:
        key, value = arg.split('=')
        inputs[key] = value

    config_path = args.file
    pipeline_builder = PipelineBuilder("mlpipeline.custom")
    pipeline = pipeline_builder.build_pipeline(config_path)
    pipeline.execute(inputs)
