import streamlit as st
import duckdb
import yaml
import os
from typing import Dict, List, Any, Optional
from datetime import datetime
import re


class ValidationService:
    """Serviço de validação de dados usando DuckDB"""
    
    def __init__(self):
        self.conn = duckdb.connect(":memory:")
        self.schema_cache = {}
    
    def get_subjects_and_subsubjects(self) -> Dict[str, List[str]]:
        """Obtém lista de assuntos e sub-assuntos dos schemas YAML"""
        subjects_data = {}
        
        # Corrigir o path para o diretório de schemas
        current_dir = os.path.dirname(os.path.abspath(__file__))  # app/services
        project_root = os.path.dirname(os.path.dirname(current_dir))  # raiz do projeto
        schema_dir = os.path.join(project_root, 'schema')
        
        if not os.path.exists(schema_dir):
            return subjects_data
        
        try:
            for filename in os.listdir(schema_dir):
                if filename.endswith('.yaml') or filename.endswith('.yml'):
                    file_path = os.path.join(schema_dir, filename)
                    
                    with open(file_path, 'r', encoding='utf-8') as file:
                        schema = yaml.safe_load(file)
                        
                        subject = schema.get('subject', 'Indefinido')
                        sub_subject = schema.get('sub_subject', filename.replace('.yaml', '').replace('.yml', ''))
                        
                        if subject not in subjects_data:
                            subjects_data[subject] = []
                        
                        if sub_subject not in subjects_data[subject]:
                            subjects_data[subject].append(sub_subject)
        
        except Exception as e:
            st.error(f"Erro ao carregar schemas: {e}")
        
        return subjects_data
    
    def load_schema_by_subject_subsubject(self, subject: str, sub_subject: str) -> Optional[Dict]:
        """Carrega schema baseado no assunto e sub-assunto"""
        cache_key = f"{subject}_{sub_subject}"
        
        if cache_key in self.schema_cache:
            return self.schema_cache[cache_key]
        
        # Corrigir o path para o diretório de schemas
        current_dir = os.path.dirname(os.path.abspath(__file__))  # app/services
        project_root = os.path.dirname(os.path.dirname(current_dir))  # raiz do projeto
        schema_dir = os.path.join(project_root, 'schema')
        
        if not os.path.exists(schema_dir):
            return None
        
        try:
            for filename in os.listdir(schema_dir):
                if filename.endswith('.yaml') or filename.endswith('.yml'):
                    file_path = os.path.join(schema_dir, filename)
                    
                    with open(file_path, 'r', encoding='utf-8') as file:
                        schema = yaml.safe_load(file)
                        
                        if (schema.get('subject') == subject and 
                            schema.get('sub_subject') == sub_subject):
                            
                            self.schema_cache[cache_key] = schema
                            return schema
        
        except Exception as e:
            st.error(f"Erro ao carregar schema: {e}")
        
        return None
    
    def validate_data(self, data_dict: List[Dict], schema: Dict) -> List[str]:
        """
        Valida dados contra um schema YAML usando validação Python
        
        Args:
            data_dict: Lista de dicionários com dados para validar
            schema: Schema YAML carregado
            
        Returns:
            List[str]: Lista de erros encontrados
        """
        errors = []
        
        if not data_dict or not schema:
            return ["Dados ou schema inválido"]
        
        try:
            # Obter colunas dos dados
            columns = list(data_dict[0].keys()) if data_dict else []
            
            # Obter campos do schema
            schema_fields = schema.get('schema', {}).get('fields', [])
            required_fields = [field['name'] for field in schema_fields if field.get('required', False)]
            
            # Validar campos obrigatórios
            for field_name in required_fields:
                if field_name not in columns:
                    errors.append(f"Campo obrigatório ausente: {field_name}")
                else:
                    # Verificar se há valores nulos/vazios nas linhas
                    null_count = 0
                    for i, row in enumerate(data_dict):
                        value = row.get(field_name)
                        if value is None or str(value).strip() == '':
                            null_count += 1
                    
                    if null_count > 0:
                        errors.append(f"Campo obrigatório '{field_name}' possui {null_count} valor(es) vazio(s)")
            
            # Validar tipos de dados
            for field in schema_fields:
                field_name = field['name']
                field_type = field.get('type', 'string')
                
                if field_name in columns:
                    field_errors = self._validate_field_type(field_name, field_type, data_dict)
                    errors.extend(field_errors)
            
            return errors
            
        except Exception as e:
            return [f"Erro durante validação: {str(e)}"]
    
    def _validate_field_type(self, field_name: str, field_type: str, data_dict: List[Dict]) -> List[str]:
        """Valida tipo de dados de um campo específico"""
        errors = []
        
        try:
            for i, row in enumerate(data_dict):
                value = row.get(field_name)
                
                if value is None or value == '':
                    continue  # Valores vazios já são tratados na validação de obrigatórios
                
                if field_type in ['int', 'integer']:
                    if not self._is_integer(value):
                        errors.append(f"Linha {i+1}: '{field_name}' deve ser um número inteiro (valor: {value})")
                
                elif field_type in ['float', 'number', 'double']:
                    if not self._is_number(value):
                        errors.append(f"Linha {i+1}: '{field_name}' deve ser um número (valor: {value})")
                
                elif field_type in ['date']:
                    if not self._is_date(value):
                        errors.append(f"Linha {i+1}: '{field_name}' deve ser uma data válida (valor: {value})")
                
                elif field_type in ['datetime', 'timestamp']:
                    if not self._is_datetime(value):
                        errors.append(f"Linha {i+1}: '{field_name}' deve ser uma data/hora válida (valor: {value})")
                
                elif field_type in ['bool', 'boolean']:
                    if not self._is_boolean(value):
                        errors.append(f"Linha {i+1}: '{field_name}' deve ser um valor booleano (valor: {value})")
                
                # field_type 'string' não precisa validação específica
                
        except Exception as e:
            errors.append(f"Erro ao validar campo '{field_name}': {str(e)}")
        
        return errors
    
    def _is_integer(self, value) -> bool:
        """Verifica se valor é um inteiro válido"""
        if value is None or (isinstance(value, str) and value.strip() == ''):
            return False
        
        if isinstance(value, bool):
            return False
        
        try:
            int(value)
            return True
        except (ValueError, TypeError):
            return False
    
    def _is_number(self, value) -> bool:
        """Verifica se valor é um número válido"""
        if value is None or (isinstance(value, str) and value.strip() == ''):
            return False
        
        if isinstance(value, bool):
            return False
        
        try:
            float(value)
            return True
        except (ValueError, TypeError):
            return False
    
    def _is_date(self, value) -> bool:
        """Verifica se valor é uma data válida"""
        if value is None or (isinstance(value, str) and value.strip() == ''):
            return False
        
        if isinstance(value, datetime):
            return True
        
        try:
            # Tentar diferentes formatos de data
            date_formats = [
                '%Y-%m-%d',
                '%d/%m/%Y',
                '%m/%d/%Y',
                '%Y/%m/%d',
                '%d-%m-%Y',
                '%m-%d-%Y'
            ]
            
            for fmt in date_formats:
                try:
                    datetime.strptime(str(value), fmt)
                    return True
                except ValueError:
                    continue
            
            return False
        except Exception:
            return False
    
    def _is_datetime(self, value) -> bool:
        """Verifica se valor é uma data/hora válida"""
        if value is None or (isinstance(value, str) and value.strip() == ''):
            return False
        
        if isinstance(value, datetime):
            return True
        
        try:
            # Tentar diferentes formatos de datetime
            datetime_formats = [
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%d %H:%M',
                '%d/%m/%Y %H:%M:%S',
                '%d/%m/%Y %H:%M',
                '%Y-%m-%dT%H:%M:%S',
                '%Y-%m-%dT%H:%M:%SZ'
            ]
            
            for fmt in datetime_formats:
                try:
                    datetime.strptime(str(value), fmt)
                    return True
                except ValueError:
                    continue
            
            return False
        except Exception:
            return False
    
    def _is_boolean(self, value) -> bool:
        """Verifica se valor é um booleano válido"""
        if value is None or (isinstance(value, str) and value.strip() == ''):
            return False
        
        if isinstance(value, bool):
            return True
        
        # Verificar strings que representam booleanos
        if isinstance(value, str):
            return value.lower() in ['true', 'false', '1', '0', 'yes', 'no', 'sim', 'não']
        
        # Verificar números que representam booleanos
        if isinstance(value, (int, float)):
            return value in [0, 1]
        
        return False


# Funções de conveniência para compatibilidade
def validate_data(df, schema_data):
    """Função wrapper para compatibilidade"""
    service = ValidationService()
    return service.validate_data(df, schema_data)


def get_subjects_and_subsubjects():
    """Função wrapper para compatibilidade"""
    service = ValidationService()
    return service.get_subjects_and_subsubjects()


def load_schema_by_subject_subsubject(subject, sub_subject):
    """Função wrapper para compatibilidade"""
    service = ValidationService()
    return service.load_schema_by_subject_subsubject(subject, sub_subject) 