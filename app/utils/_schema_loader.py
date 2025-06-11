import yaml

def load_schema(path: str) -> dict:
    """
    Load a schema from a YAML file.

    Args:
        path: The path to the YAML file.

    Returns:
        A dictionary containing the schema.
    """
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)