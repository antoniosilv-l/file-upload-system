import duckdb
import io
import pandas as pd
from ._normalizer import normalize_dataframe_columns

def get_excel_sheet_names(file):
    """
    Get the names of all sheets in an Excel file.
    
    Args:
        file: The uploaded Excel file object.
        
    Returns:
        List of sheet names.
    """
    try:
        file.seek(0)
        xl_file = pd.ExcelFile(file)
        return xl_file.sheet_names
    except Exception:
        return []

def preview_file(file, separator=",", header_row=0, sheet_name=None, full_data=False):
    """	
    Preview a file (CSV or Excel) and return a dataframe and file information.

    Args:
        file: The uploaded file object.
        separator: The separator used in CSV files.
        header_row: The row number of the header.
        sheet_name: The name of the Excel sheet to read (for Excel files only).
        full_data: If True, returns the complete dataframe instead of preview.

    Returns:
        A tuple containing the dataframe and file information.
    """
    file.seek(0)
    file_extension = file.name.lower().split('.')[-1]
    
    if file_extension in ['xlsx', 'xls']:
        # Handle Excel files
        try:
            if full_data:
                # Read complete file
                df = pd.read_excel(file, sheet_name=sheet_name, header=header_row)
                total_rows = len(df)
            else:
                # Read preview only
                df = pd.read_excel(
                    file,
                    sheet_name=sheet_name,
                    header=header_row,
                    nrows=100  # Limit to 100 rows for preview
                )
                
                # Get total row count
                file.seek(0)
                df_full = pd.read_excel(file, sheet_name=sheet_name, header=header_row)
                total_rows = len(df_full)
            
        except Exception as e:
            raise Exception(f"Erro ao ler planilha Excel: {e}")
    
    else:
        # Handle CSV files
        try:
            file.seek(0)
            file_bytes = file.read()

            con = duckdb.connect(database=':memory:')
            con.execute("INSTALL httpfs; LOAD httpfs;")

            # DuckDB expects a text stream
            csv_stream = io.StringIO(file_bytes.decode("utf-8"))

            rel = con.read_csv(
                csv_stream,
                header=True,
                sep=separator,
                skiprows=header_row
            )

            if full_data:
                # Read complete file
                df = rel.df()
                total_rows = len(df)
            else:
                # Read preview only
                df = rel.limit(100).df()
                total_rows = rel.count("1").fetchone()[0]
            
        except UnicodeDecodeError:
            # Try different encodings
            for encoding in ['latin1', 'cp1252', 'iso-8859-1']:
                try:
                    file.seek(0)
                    file_bytes = file.read()
                    csv_stream = io.StringIO(file_bytes.decode(encoding))
                    
                    con = duckdb.connect(database=':memory:')
                    con.execute("INSTALL httpfs; LOAD httpfs;")
                    
                    rel = con.read_csv(
                        csv_stream,
                        header=True,
                        sep=separator,
                        skiprows=header_row
                    )

                    if full_data:
                        # Read complete file
                        df = rel.df()
                        total_rows = len(df)
                    else:
                        # Read preview only
                        df = rel.limit(100).df()
                        total_rows = rel.count("1").fetchone()[0]
                    break
                except:
                    continue
            else:
                raise Exception("Não foi possível decodificar o arquivo CSV. Verifique a codificação.")
        
        except Exception as e:
            raise Exception(f"Erro ao ler arquivo CSV: {e}")
    
    # Normalize column names
    df_normalized, column_mapping = normalize_dataframe_columns(df)

    file_info = {
        "tipo_arquivo": file_extension.upper(),
        "colunas_detectadas": list(df.columns),
        "colunas_normalizadas": list(df_normalized.columns),
        "mapeamento_colunas": column_mapping,
        "total_linhas_estimado": total_rows,
        "colunas_com_tipos": {str(col): str(dtype) for col, dtype in df.dtypes.items()}
    }

    return df_normalized, file_info