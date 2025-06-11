import re
import unicodedata

def normalize_column_name(column_name):
    """
    Normalize column name by:
    - Converting to lowercase
    - Removing accents and special characters
    - Replacing spaces and special chars with underscores
    - Removing consecutive underscores
    
    Args:
        column_name: The original column name
        
    Returns:
        Normalized column name
    """
    # Remove accents and normalize unicode
    normalized = unicodedata.normalize('NFKD', column_name)
    normalized = ''.join(c for c in normalized if not unicodedata.combining(c))
    
    # Convert to lowercase
    normalized = normalized.lower()
    
    # Replace spaces and special characters with underscores
    normalized = re.sub(r'[^a-z0-9]+', '_', normalized)
    
    # Remove leading/trailing underscores and consecutive underscores
    normalized = re.sub(r'^_+|_+$', '', normalized)
    normalized = re.sub(r'_+', '_', normalized)
    
    return normalized

def normalize_dataframe_columns(df):
    """
    Normalize all column names in a dataframe.
    
    Args:
        df: The dataframe to normalize
        
    Returns:
        Dataframe with normalized column names
    """
    df_copy = df.copy()
    column_mapping = {col: normalize_column_name(col) for col in df_copy.columns}
    df_copy = df_copy.rename(columns=column_mapping)
    return df_copy, column_mapping 