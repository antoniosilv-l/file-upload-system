import re
import pandas as pd
import unicodedata

class ColumnNormalizer:
    """Classe para normalização de nomes de colunas"""
    
    def __init__(self):
        self.normalization_rules = {
            # Regras específicas para caracteres especiais comuns
            'tipo (r$)': 'tipo_rs',
            'tipo(r$)': 'tipo_rs',
            'valor (r$)': 'valor_rs',
            'valor(r$)': 'valor_rs',
            'preço (r$)': 'preco_rs',
            'preço(r$)': 'preco_rs',
            'custo (r$)': 'custo_rs',
            'custo(r$)': 'custo_rs',
        }
    
    def normalize_column_name(self, column_name):
        """
        Normaliza um nome de coluna individual
        
        Args:
            column_name (str): Nome da coluna original
            
        Returns:
            str: Nome da coluna normalizado
        """
        if not isinstance(column_name, str):
            column_name = str(column_name)
        
        # Converter para minúsculas
        normalized = column_name.lower().strip()
        
        # Aplicar regras específicas primeiro
        for original, replacement in self.normalization_rules.items():
            if original in normalized:
                normalized = normalized.replace(original, replacement)
        
        # Remover acentos
        normalized = self._remove_accents(normalized)
        
        # Substituir espaços e caracteres especiais por underscore
        normalized = re.sub(r'[^\w]', '_', normalized)
        
        # Remover underscores múltiplos
        normalized = re.sub(r'_+', '_', normalized)
        
        # Remover underscore no início e fim
        normalized = normalized.strip('_')
        
        # Garantir que não comece com número
        if normalized and normalized[0].isdigit():
            normalized = 'col_' + normalized
        
        # Garantir que não seja vazio
        if not normalized:
            normalized = 'unnamed_column'
        
        return normalized
    
    def _remove_accents(self, text):
        """Remove acentos de um texto"""
        return ''.join(
            char for char in unicodedata.normalize('NFD', text)
            if unicodedata.category(char) != 'Mn'
        )
    
    def normalize_dataframe(self, df):
        """
        Normaliza todos os nomes de colunas de um DataFrame
        
        Args:
            df (pd.DataFrame): DataFrame original
            
        Returns:
            pd.DataFrame: DataFrame com colunas normalizadas
        """
        if not isinstance(df, pd.DataFrame):
            raise ValueError("Input deve ser um pandas DataFrame")
        
        # Criar cópia do DataFrame
        df_normalized = df.copy()
        
        # Mapear colunas originais para normalizadas
        column_mapping = {}
        normalized_columns = []
        
        for original_col in df.columns:
            normalized_col = self.normalize_column_name(original_col)
            
            # Verificar se já existe uma coluna com esse nome normalizado
            counter = 1
            base_normalized = normalized_col
            while normalized_col in normalized_columns:
                normalized_col = f"{base_normalized}_{counter}"
                counter += 1
            
            column_mapping[original_col] = normalized_col
            normalized_columns.append(normalized_col)
        
        # Renomear as colunas
        df_normalized.columns = normalized_columns
        
        return df_normalized
    
    def get_column_mapping(self, df):
        """
        Retorna o mapeamento entre colunas originais e normalizadas
        
        Args:
            df (pd.DataFrame): DataFrame original
            
        Returns:
            dict: Mapeamento {coluna_original: coluna_normalizada}
        """
        mapping = {}
        normalized_columns = []
        
        for original_col in df.columns:
            normalized_col = self.normalize_column_name(original_col)
            
            # Verificar duplicatas
            counter = 1
            base_normalized = normalized_col
            while normalized_col in normalized_columns:
                normalized_col = f"{base_normalized}_{counter}"
                counter += 1
            
            mapping[original_col] = normalized_col
            normalized_columns.append(normalized_col)
        
        return mapping
    
    def add_custom_rule(self, original, replacement):
        """
        Adiciona uma regra customizada de normalização
        
        Args:
            original (str): Texto original para buscar
            replacement (str): Texto de substituição
        """
        self.normalization_rules[original.lower()] = replacement.lower()
    
    def remove_custom_rule(self, original):
        """
        Remove uma regra customizada
        
        Args:
            original (str): Texto original da regra a remover
        """
        key = original.lower()
        if key in self.normalization_rules:
            del self.normalization_rules[key]
    
    def get_normalization_rules(self):
        """Retorna as regras de normalização atuais"""
        return self.normalization_rules.copy()
    
    def preview_normalization(self, columns):
        """
        Faz um preview da normalização sem aplicar
        
        Args:
            columns (list): Lista de nomes de colunas
            
        Returns:
            dict: Mapeamento de preview {original: normalizado}
        """
        preview = {}
        normalized_columns = []
        
        for col in columns:
            normalized = self.normalize_column_name(col)
            
            # Verificar duplicatas
            counter = 1
            base_normalized = normalized
            while normalized in normalized_columns:
                normalized = f"{base_normalized}_{counter}"
                counter += 1
            
            preview[col] = normalized
            normalized_columns.append(normalized)
        
        return preview 