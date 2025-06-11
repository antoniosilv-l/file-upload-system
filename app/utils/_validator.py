import pandas as pd
import numpy as np
from datetime import datetime
import re

class DataValidator:
    """Valida dados de acordo com schemas definidos"""
    
    def __init__(self):
        self.type_validators = {
            'str': self._validate_string,
            'int': self._validate_integer,
            'float': self._validate_float,
            'date': self._validate_date,
            'email': self._validate_email,
            'phone': self._validate_phone,
            'bool': self._validate_boolean
        }
    
    def validate_data(self, df, schema_data):
        """
        Valida um DataFrame de acordo com um schema
        
        Args:
            df (pd.DataFrame): DataFrame para validar
            schema_data (dict): Schema YAML carregado
            
        Returns:
            tuple: (is_valid, validation_report)
        """
        if not schema_data or 'schema' not in schema_data:
            return False, {"schema": ["Schema inválido ou não encontrado"]}
        
        schema = schema_data['schema']
        fields = schema.get('fields', [])
        
        validation_report = {}
        is_valid = True
        
        # Verificar se todas as colunas obrigatórias existem
        required_columns = [field['name'] for field in fields if field.get('required', False)]
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            validation_report['missing_columns'] = [f"Colunas obrigatórias ausentes: {', '.join(missing_columns)}"]
            is_valid = False
        
        # Validar cada campo
        for field in fields:
            field_name = field['name']
            field_type = field.get('type', 'str')
            required = field.get('required', False)
            
            if field_name not in df.columns:
                if required:
                    continue  # Já reportado acima
                else:
                    continue  # Campo opcional não presente
            
            column_errors = []
            
            # Verificar valores nulos em campos obrigatórios
            if required:
                null_count = df[field_name].isna().sum()
                if null_count > 0:
                    column_errors.append(f"{null_count} valores nulos em campo obrigatório")
            
            # Validar tipo de dados
            non_null_data = df[field_name].dropna()
            if len(non_null_data) > 0:
                type_errors = self._validate_column_type(non_null_data, field_type)
                column_errors.extend(type_errors)
            
            # Validações específicas do campo
            if 'min_length' in field:
                length_errors = self._validate_min_length(non_null_data, field['min_length'])
                column_errors.extend(length_errors)
                
            if 'max_length' in field:
                length_errors = self._validate_max_length(non_null_data, field['max_length'])
                column_errors.extend(length_errors)
                
            if 'min_value' in field:
                value_errors = self._validate_min_value(non_null_data, field['min_value'])
                column_errors.extend(value_errors)
                
            if 'max_value' in field:
                value_errors = self._validate_max_value(non_null_data, field['max_value'])
                column_errors.extend(value_errors)
                
            if 'allowed_values' in field:
                value_errors = self._validate_allowed_values(non_null_data, field['allowed_values'])
                column_errors.extend(value_errors)
            
            if column_errors:
                validation_report[field_name] = column_errors
                is_valid = False
        
        return is_valid, validation_report
    
    def _validate_column_type(self, series, expected_type):
        """Valida o tipo de dados de uma coluna"""
        errors = []
        
        if expected_type not in self.type_validators:
            return [f"Tipo '{expected_type}' não suportado"]
        
        validator = self.type_validators[expected_type]
        invalid_count = 0
        
        for value in series:
            if not validator(value):
                invalid_count += 1
        
        if invalid_count > 0:
            errors.append(f"{invalid_count} valores com tipo inválido (esperado: {expected_type})")
        
        return errors
    
    def _validate_string(self, value):
        """Valida se o valor é uma string válida"""
        return isinstance(value, (str, int, float)) and str(value).strip() != ''
    
    def _validate_integer(self, value):
        """Valida se o valor é um inteiro válido"""
        try:
            if pd.isna(value):
                return True
            if isinstance(value, bool):
                return False
            int(value)
            return True
        except (ValueError, TypeError):
            return False
    
    def _validate_float(self, value):
        """Valida se o valor é um float válido"""
        try:
            if pd.isna(value):
                return True
            if isinstance(value, bool):
                return False
            float(value)
            return True
        except (ValueError, TypeError):
            return False
    
    def _validate_date(self, value):
        """Valida se o valor é uma data válida"""
        if pd.isna(value):
            return True
            
        try:
            if isinstance(value, datetime):
                return True
            pd.to_datetime(value)
            return True
        except:
            return False
    
    def _validate_email(self, value):
        """Valida se o valor é um email válido"""
        if pd.isna(value):
            return True
            
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(email_pattern, str(value)))
    
    def _validate_phone(self, value):
        """Valida se o valor é um telefone válido"""
        if pd.isna(value):
            return True
            
        # Remove caracteres não numéricos
        phone = re.sub(r'[^\d]', '', str(value))
        # Aceita telefones com 10 ou 11 dígitos
        return len(phone) in [10, 11] and phone.isdigit()
    
    def _validate_boolean(self, value):
        """Valida se o valor é um booleano válido"""
        if pd.isna(value):
            return True
            
        if isinstance(value, bool):
            return True
            
        str_value = str(value).lower().strip()
        return str_value in ['true', 'false', '1', '0', 'yes', 'no', 'sim', 'não']
    
    def _validate_min_length(self, series, min_length):
        """Valida comprimento mínimo de strings"""
        errors = []
        invalid_count = 0
        
        for value in series:
            if len(str(value)) < min_length:
                invalid_count += 1
        
        if invalid_count > 0:
            errors.append(f"{invalid_count} valores com comprimento menor que {min_length}")
        
        return errors
    
    def _validate_max_length(self, series, max_length):
        """Valida comprimento máximo de strings"""
        errors = []
        invalid_count = 0
        
        for value in series:
            if len(str(value)) > max_length:
                invalid_count += 1
        
        if invalid_count > 0:
            errors.append(f"{invalid_count} valores com comprimento maior que {max_length}")
        
        return errors
    
    def _validate_min_value(self, series, min_value):
        """Valida valor mínimo para campos numéricos"""
        errors = []
        invalid_count = 0
        
        for value in series:
            try:
                if float(value) < min_value:
                    invalid_count += 1
            except (ValueError, TypeError):
                continue
        
        if invalid_count > 0:
            errors.append(f"{invalid_count} valores menores que {min_value}")
        
        return errors
    
    def _validate_max_value(self, series, max_value):
        """Valida valor máximo para campos numéricos"""
        errors = []
        invalid_count = 0
        
        for value in series:
            try:
                if float(value) > max_value:
                    invalid_count += 1
            except (ValueError, TypeError):
                continue
        
        if invalid_count > 0:
            errors.append(f"{invalid_count} valores maiores que {max_value}")
        
        return errors
    
    def _validate_allowed_values(self, series, allowed_values):
        """Valida se os valores estão na lista de valores permitidos"""
        errors = []
        invalid_count = 0
        
        for value in series:
            if str(value) not in [str(v) for v in allowed_values]:
                invalid_count += 1
        
        if invalid_count > 0:
            errors.append(f"{invalid_count} valores não estão na lista permitida: {allowed_values}")
        
        return errors 