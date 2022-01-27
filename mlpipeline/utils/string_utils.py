import time


def get_keys_values(keys_values: dict) -> str:
    message = ""
    for k, v in keys_values.items():
        message += k + "=" + str(v) + " "
    return message


def get_component_message(name, class_name, inputs: dict, outputs: dict) -> str:
    """Log message formatting
    """

    message = "{}: {} - {}: inputs - {} : outputs - {}".format(
        int(time.time()), name, class_name,
        get_keys_values(inputs), get_keys_values(outputs)
    )
    return message


def get_pipeline_message(keys_values: dict, prefix: str) -> str:
    """Log line formatter
    """

    return "{}: pipeline : {} - {}".format(int(time.time()), prefix, get_keys_values(keys_values))
