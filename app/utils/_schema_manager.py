import os
import glob
from ._schema_loader import load_schema

def get_available_schemas():
    """
    Get all available schema files and organize them by category.
    
    Returns:
        Dictionary with available schemas organized by subject/sub-subject
    """
    schema_files = glob.glob("schema/*.yaml") + glob.glob("schema/*.yml")
    
    schemas = {}
    for schema_file in schema_files:
        try:
            schema = load_schema(schema_file)
            filename = os.path.basename(schema_file)
            
            # Extract schema info
            schema_info = {
                'file': schema_file,
                'filename': filename,
                'table_name': schema.get('schema', {}).get('table_name', filename.replace('.yaml', '').replace('.yml', '')),
                'description': schema.get('description', f"Schema para {filename}"),
                'fields_count': len(schema.get('schema', {}).get('fields', []))
            }
            
            schemas[filename] = schema_info
            
        except Exception as e:
            print(f"Erro ao carregar schema {schema_file}: {e}")
    
    return schemas

def get_schemas_by_category(assunto, sub_assunto):
    """
    Get schemas that might be relevant for the given subject/sub-subject.
    
    Args:
        assunto: The main subject
        sub_assunto: The sub-subject
        
    Returns:
        List of relevant schema options
    """
    all_schemas = get_available_schemas()
    
    # Schema mapping based on subject/sub-subject
    schema_mapping = {
        "usuarios": {
            "cadastro": ["user.yaml", "usuarios.yaml", "clientes.yaml"],
        },
        "locais": {
            "aeroportos": ["aeroportos.yaml", "locations.yaml", "places.yaml"],
        },
        "produtos": {
            "catalogo": ["products.yaml", "produtos.yaml"],
            "estoque": ["inventory.yaml", "estoque.yaml"],
        },
        "vendas": {
            "transacoes": ["sales.yaml", "vendas.yaml", "transactions.yaml"],
            "leads": ["leads.yaml", "prospects.yaml"],
        }
    }
    
    # Normalize keys for lookup
    assunto_key = assunto.lower().replace(' ', '_')
    sub_assunto_key = sub_assunto.lower().replace(' ', '_')
    
    # Get relevant schemas for this category
    relevant_schemas = []
    
    if assunto_key in schema_mapping and sub_assunto_key in schema_mapping[assunto_key]:
        schema_files = schema_mapping[assunto_key][sub_assunto_key]
        for schema_file in schema_files:
            if schema_file in all_schemas:
                relevant_schemas.append(all_schemas[schema_file])
    
    # If no specific schemas found, return all available schemas
    if not relevant_schemas:
        relevant_schemas = list(all_schemas.values())
    
    return relevant_schemas

def format_schema_option(schema_info):
    """
    Format schema information for display in dropdown.
    
    Args:
        schema_info: Schema information dictionary
        
    Returns:
        Formatted string for dropdown display
    """
    return f"{schema_info['table_name']} ({schema_info['fields_count']} campos) - {schema_info['filename']}" 