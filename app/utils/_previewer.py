import duckdb
import io

def preview_file(file, separator, header_row):
    """	
    Preview a file using DuckDB and return a dataframe and file information.

    Args:
        file: The uploaded file object.
        separator: The separator used in the file.
        header_row: The row number of the header.

    Returns:
        A tuple containing the dataframe and file information.
    """
    file.seek(0)
    file_bytes = file.read()

    con = duckdb.connect(database=':memory:')
    con.execute("INSTALL httpfs; LOAD httpfs;")

    # DuckDB espera um stream de texto
    csv_stream = io.StringIO(file_bytes.decode("utf-8"))

    rel = con.read_csv(
        csv_stream,
        header=True,
        sep=separator,
        skiprows=header_row
    )

    df = rel.limit(10).df()

    file_info = {
        "colunas_detectadas": rel.columns,
        "total_linhas_estimado": rel.count("1").fetchone()[0],
        "colunas_com_tipos": {col: str(typ) for col, typ in zip(rel.columns, rel.types)}
    }

    return df, file_info