def get_keys_values(keys_values: dict) -> str:
    message = ""
    for k, v in keys_values.items():
        message += k + "=" + str(v) + " "
    return message
