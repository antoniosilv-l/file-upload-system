import yaml
import os
from pathlib import Path

class SchemaManager:
    """Gerencia os schemas YAML para validação de dados"""
    
    def __init__(self, schema_dir="schema"):
        self.schema_dir = Path(schema_dir)
        
    def list_schemas(self):
        """Lista todos os arquivos de schema disponíveis"""
        if not self.schema_dir.exists():
            return []
            
        schemas = []
        for file_path in self.schema_dir.glob("*.yaml"):
            schemas.append(file_path.name)
            
        return sorted(schemas)
    
    def load_schema(self, schema_name):
        """Carrega um schema específico"""
        try:
            schema_path = self.schema_dir / schema_name
            
            if not schema_path.exists():
                raise FileNotFoundError(f"Schema não encontrado: {schema_name}")
                
            with open(schema_path, 'r', encoding='utf-8') as file:
                schema_data = yaml.safe_load(file)
                
            return schema_data
            
        except Exception as e:
            print(f"Erro ao carregar schema {schema_name}: {str(e)}")
            return None
    
    def get_schema_info(self, schema_name):
        """Retorna informações básicas sobre um schema"""
        schema_data = self.load_schema(schema_name)
        
        if not schema_data:
            return None
            
        info = {
            'name': schema_name,
            'description': schema_data.get('description', 'Sem descrição'),
            'table_name': schema_data.get('schema', {}).get('table_name', 'N/A'),
            'fields_count': len(schema_data.get('schema', {}).get('fields', [])),
            'required_fields': len([
                field for field in schema_data.get('schema', {}).get('fields', [])
                if field.get('required', False)
            ])
        }
        
        return info
    
    def validate_schema_structure(self, schema_data):
        """Valida se um schema tem a estrutura correta"""
        if not isinstance(schema_data, dict):
            return False, "Schema deve ser um dicionário"
            
        if 'schema' not in schema_data:
            return False, "Schema deve conter a chave 'schema'"
            
        schema_content = schema_data['schema']
        
        if 'fields' not in schema_content:
            return False, "Schema deve conter 'fields'"
            
        if not isinstance(schema_content['fields'], list):
            return False, "'fields' deve ser uma lista"
            
        # Validar cada campo
        for i, field in enumerate(schema_content['fields']):
            if not isinstance(field, dict):
                return False, f"Campo {i} deve ser um dicionário"
                
            if 'name' not in field:
                return False, f"Campo {i} deve ter 'name'"
                
            if 'type' not in field:
                return False, f"Campo {i} deve ter 'type'"
                
        return True, "Schema válido" 