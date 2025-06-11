def validate_data(df, schema):
    """
    Validate the data against the schema.

    Args:
        df: The dataframe to validate.
        schema: The schema to validate against.

    Returns:
        A list of errors.
    """
    errors = []

    expected_columns = [f["name"] for f in schema["schema"]["fields"]]
    required_columns = [f["name"] for f in schema["schema"]["fields"] if f.get("required")]

    for col in required_columns:
        if col not in df.columns:
            errors.append(f"Campo obrigatório ausente: {col}")

    for col in df.columns:
        if col not in expected_columns:
            errors.append(f"Campo não esperado: {col}")

    return errors