import yaml


def load_config(path: str) -> dict:
    with open(path, "r") as f:
        config = yaml.safe_load(f)

    # minimal validation
    if "source" not in config:
        raise ValueError("Missing 'source' in config")
    if "target" not in config:
        raise ValueError("Missing 'target' in config")

    return config