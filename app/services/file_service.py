import streamlit as st
import duckdb
from typing import Dict, List, Any, Tuple, Optional
from io import BytesIO
import re
import unicodedata
import os


class FileProcessingService:
    """Serviço para processamento de arquivos CSV/Excel usando DuckDB"""
    
    def __init__(self):
        self.conn = duckdb.connect(":memory:")
        self.supported_formats = ['csv', 'xlsx', 'xls']
        self.normalization_rules = {
            'tipo (r$)': 'tipo_rs',
            'tipo(r$)': 'tipo_rs',
            'valor (r$)': 'valor_rs',
            'valor(r$)': 'valor_rs',
            'preço (r$)': 'preco_rs',
            'preço(r$)': 'preco_rs',
            'custo (r$)': 'custo_rs',
            'custo(r$)': 'custo_rs',
        }
    
    def get_excel_sheets(self, uploaded_file) -> List[str]:
        """Retorna lista de abas de uma planilha Excel"""
        try:
            # Salvar arquivo temporariamente para DuckDB
            temp_path = f"/tmp/temp_excel_{uploaded_file.name}"
            with open(temp_path, "wb") as f:
                f.write(uploaded_file.getvalue())
            
            # Usar DuckDB para ler metadados do Excel
            try:
                # Tentar diferentes abas para detectar quais existem
                sheets = []
                for i in range(10):  # Tentar até 10 abas
                    try:
                        result = self.conn.execute(f"SELECT * FROM read_excel('{temp_path}', sheet={i}) LIMIT 1").fetchall()
                        sheets.append(f"Sheet{i+1}")
                    except:
                        break
                
                # Se não encontrou nenhuma aba, tentar com nomes padrão
                if not sheets:
                    common_names = ['Sheet1', 'Planilha1', 'Plan1', 'Data', 'Dados']
                    for name in common_names:
                        try:
                            result = self.conn.execute(f"SELECT * FROM read_excel('{temp_path}', sheet='{name}') LIMIT 1").fetchall()
                            sheets.append(name)
                            break
                        except:
                            continue
                
                return sheets if sheets else ['Sheet1']
                
            except Exception as e:
                # Fallback: retornar nome padrão
                return ['Sheet1']
                
        except Exception as e:
            st.error(f"Erro ao ler planilha Excel: {e}")
            return []
    
    def preview_file(self, uploaded_file, sep: str = ",", header_row: int = 0, sheet_name: str = None) -> Tuple[Any, Dict]:
        """
        Faz preview de arquivo CSV ou Excel usando DuckDB
        
        Returns:
            tuple: (dataframe_dict, file_info)
        """
        file_extension = uploaded_file.name.split('.')[-1].lower()
        
        try:
            if file_extension == 'csv':
                return self._preview_csv_with_duckdb(uploaded_file, sep, header_row)
            elif file_extension in ['xlsx', 'xls']:
                return self._preview_excel_with_duckdb(uploaded_file, sheet_name, header_row)
            else:
                raise ValueError(f"Formato de arquivo não suportado: {file_extension}")
                
        except Exception as e:
            raise Exception(f"Erro ao fazer preview do arquivo: {str(e)}")
    
    def _preview_csv_with_duckdb(self, uploaded_file, sep: str, header_row: int):
        """Preview CSV usando DuckDB e retorna lista de dicionários"""
        try:
            # Detectar separador se for "auto"
            if sep == "auto":
                sep = self._detect_csv_separator(uploaded_file)
            
            # Salvar arquivo temporariamente
            temp_path = f"/tmp/temp_{uploaded_file.name}"
            with open(temp_path, "wb") as f:
                f.write(uploaded_file.getvalue())
            
            # Ler com DuckDB
            skip_rows = header_row if header_row > 0 else 0
            
            result = self.conn.execute(f"""
                SELECT * FROM read_csv('{temp_path}', 
                                     delim='{sep}', 
                                     header=true,
                                     skip={skip_rows})
                LIMIT 50
            """).fetchall()
            
            # Obter nomes das colunas
            columns = [desc[0] for desc in self.conn.description]
            
            # Converter para lista de dicionários
            data = []
            for row in result:
                row_dict = dict(zip(columns, row))
                data.append(row_dict)
            
            # Query para contagem total de linhas
            count_result = self.conn.execute(f"""
                SELECT COUNT(*) FROM read_csv('{temp_path}', 
                                            delim='{sep}', 
                                            header=true,
                                            skip={skip_rows})
            """).fetchall()
            
            total_rows = count_result[0][0] if count_result else len(data)
            
            # Limpar arquivo temporário
            if os.path.exists(temp_path):
                os.remove(temp_path)
            
            # Obter tipos das colunas
            column_types = {}
            if data:
                for column in columns:
                    # Inferir tipo baseado nos primeiros valores não-nulos
                    sample_values = [row[column] for row in data[:10] if row.get(column) is not None]
                    if sample_values:
                        column_types[column] = self._infer_column_type(sample_values)
                    else:
                        column_types[column] = 'object'
            
            file_info = {
                'formato': 'csv',
                'colunas_detectadas': len(columns),
                'total_linhas_estimado': total_rows,
                'colunas_com_tipos': column_types,
                'separador_usado': sep
            }
            
            return data, file_info
            
        except Exception as e:
            st.error(f"Erro ao processar CSV: {e}")
            return [], {'formato': 'csv', 'erro': str(e)}
    
    def _preview_excel_with_duckdb(self, uploaded_file, sheet_name: str, header_row: int):
        """Preview Excel usando DuckDB e retorna lista de dicionários"""
        try:
            # Salvar arquivo temporariamente
            temp_path = f"/tmp/temp_{uploaded_file.name}"
            with open(temp_path, "wb") as f:
                f.write(uploaded_file.getvalue())
            
            # Construir query DuckDB para Excel
            skip_rows = header_row if header_row > 0 else 0
            
            if sheet_name:
                result = self.conn.execute(f"""
                    SELECT * FROM read_excel('{temp_path}', 
                                           sheet='{sheet_name}',
                                           header=true,
                                           skip={skip_rows})
                    LIMIT 50
                """).fetchall()
                
                # Query para contagem total
                count_result = self.conn.execute(f"""
                    SELECT COUNT(*) FROM read_excel('{temp_path}', 
                                                  sheet='{sheet_name}',
                                                  header=true,
                                                  skip={skip_rows})
                """).fetchall()
            else:
                result = self.conn.execute(f"""
                    SELECT * FROM read_excel('{temp_path}', 
                                           header=true,
                                           skip={skip_rows})
                    LIMIT 50
                """).fetchall()
                
                # Query para contagem total
                count_result = self.conn.execute(f"""
                    SELECT COUNT(*) FROM read_excel('{temp_path}', 
                                                  header=true,
                                                  skip={skip_rows})
                """).fetchall()
            
            # Obter nomes das colunas
            columns = [desc[0] for desc in self.conn.description]
            
            # Converter para lista de dicionários
            data = []
            for row in result:
                row_dict = dict(zip(columns, row))
                data.append(row_dict)
            
            total_rows = count_result[0][0] if count_result else len(data)
            
            # Limpar arquivo temporário
            if os.path.exists(temp_path):
                os.remove(temp_path)
            
            # Obter tipos das colunas
            column_types = {}
            if data:
                for column in columns:
                    # Inferir tipo baseado nos primeiros valores não-nulos
                    sample_values = [row[column] for row in data[:10] if row.get(column) is not None]
                    if sample_values:
                        column_types[column] = self._infer_column_type(sample_values)
                    else:
                        column_types[column] = 'object'
            
            file_info = {
                'formato': 'excel',
                'colunas_detectadas': len(columns),
                'total_linhas_estimado': total_rows,
                'colunas_com_tipos': column_types,
                'aba_usada': sheet_name
            }
            
            return data, file_info
            
        except Exception as e:
            st.error(f"Erro ao processar Excel: {e}")
            return [], {'formato': 'excel', 'erro': str(e)}
    
    def _detect_csv_separator(self, uploaded_file) -> str:
        """Detecta automaticamente o separador do CSV"""
        # Ler primeiras linhas para detectar separador
        sample = uploaded_file.read(1024).decode('utf-8', errors='ignore')
        uploaded_file.seek(0)  # Reset file pointer
        
        separators = [',', ';', '\t', '|']
        separator_counts = {}
        
        for sep in separators:
            count = sample.count(sep)
            if count > 0:
                separator_counts[sep] = count
        
        if separator_counts:
            return max(separator_counts, key=separator_counts.get)
        else:
            return ','  # Default
    
    def normalize_columns(self, data_dict: List[Dict]) -> List[Dict]:
        """
        Normaliza nomes de colunas removendo caracteres especiais
        
        Args:
            data_dict: Lista de dicionários com dados
            
        Returns:
            Lista de dicionários com colunas normalizadas
        """
        if not isinstance(data_dict, list):
            raise ValueError("Input deve ser uma lista de dicionários")
        
        if not data_dict:
            return data_dict
        
        # Criar mapeamento de normalização
        original_columns = list(data_dict[0].keys())
        normalized_mapping = {}
        
        for col in original_columns:
            # Remover acentos e caracteres especiais
            normalized = col.lower()
            normalized = normalized.replace(' ', '_')
            normalized = ''.join(c if c.isalnum() or c == '_' else '' for c in normalized)
            normalized_mapping[col] = normalized
        
        # Aplicar normalização
        normalized_data = []
        for row in data_dict:
            normalized_row = {}
            for old_col, new_col in normalized_mapping.items():
                normalized_row[new_col] = row.get(old_col)
            normalized_data.append(normalized_row)
        
        return normalized_data
    
    def normalize_column_names(self, df):
        """
        Normaliza nomes de colunas do DataFrame
        
        Args:
            df (pd.DataFrame): DataFrame original
            
        Returns:
            pd.DataFrame: DataFrame com colunas normalizadas
        """
        if not isinstance(df, list):
            raise ValueError("Input deve ser uma lista de dicionários")
        
        df_normalized = df.copy()
        normalized_columns = []
        
        for original_col in df.columns:
            normalized_col = self._normalize_single_column(original_col)
            
            # Evitar duplicatas
            counter = 1
            base_normalized = normalized_col
            while normalized_col in normalized_columns:
                normalized_col = f"{base_normalized}_{counter}"
                counter += 1
            
            normalized_columns.append(normalized_col)
        
        df_normalized.columns = normalized_columns
        return df_normalized
    
    def _normalize_single_column(self, column_name):
        """Normaliza um nome de coluna individual"""
        if not isinstance(column_name, str):
            column_name = str(column_name)
        
        # Converter para minúsculas
        normalized = column_name.lower().strip()
        
        # Aplicar regras específicas
        for original, replacement in self.normalization_rules.items():
            if original in normalized:
                normalized = normalized.replace(original, replacement)
        
        # Remover acentos
        normalized = self._remove_accents(normalized)
        
        # Substituir caracteres especiais por underscore
        normalized = re.sub(r'[^\w]', '_', normalized)
        
        # Remover underscores múltiplos
        normalized = re.sub(r'_+', '_', normalized)
        
        # Limpar início e fim
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
    
    def get_column_mapping_preview(self, columns):
        """
        Faz preview da normalização de colunas
        
        Args:
            columns (list): Lista de nomes de colunas
            
        Returns:
            dict: Mapeamento {original: normalizado}
        """
        preview = {}
        normalized_columns = []
        
        for col in columns:
            normalized = self._normalize_single_column(col)
            
            # Verificar duplicatas
            counter = 1
            base_normalized = normalized
            while normalized in normalized_columns:
                normalized = f"{base_normalized}_{counter}"
                counter += 1
            
            preview[col] = normalized
            normalized_columns.append(normalized)
        
        return preview
    
    def add_normalization_rule(self, original, replacement):
        """Adiciona regra customizada de normalização"""
        self.normalization_rules[original.lower()] = replacement.lower()
    
    def get_file_info(self, uploaded_file):
        """Retorna informações básicas sobre o arquivo"""
        info = {
            'name': uploaded_file.name,
            'size': uploaded_file.size,
            'type': uploaded_file.type,
            'extension': uploaded_file.name.split('.')[-1].lower()
        }
        
        # Tamanho formatado
        if info['size'] < 1024:
            info['size_formatted'] = f"{info['size']} bytes"
        elif info['size'] < 1024 * 1024:
            info['size_formatted'] = f"{info['size'] / 1024:.1f} KB"
        else:
            info['size_formatted'] = f"{info['size'] / (1024 * 1024):.1f} MB"
        
        return info
    
    def validate_file_size(self, uploaded_file, max_size_mb=50):
        """Valida tamanho do arquivo"""
        max_size_bytes = max_size_mb * 1024 * 1024
        
        if uploaded_file.size > max_size_bytes:
            st.error(f"Arquivo muito grande! Tamanho máximo: {max_size_mb}MB")
            return False
        
        return True
    
    def _infer_column_type(self, sample_values):
        """Infere o tipo de dados de uma coluna baseado em valores de amostra"""
        import re
        
        if not sample_values:
            return 'object'
        
        # Converter todos para string para análise
        str_values = [str(v) for v in sample_values if v is not None]
        
        if not str_values:
            return 'object'
        
        # Verificar se todos são números inteiros
        all_int = True
        for val in str_values:
            if not re.match(r'^-?\d+$', val.strip()):
                all_int = False
                break
        
        if all_int:
            return 'int64'
        
        # Verificar se todos são números decimais
        all_float = True
        for val in str_values:
            if not re.match(r'^-?\d*\.?\d+([eE][+-]?\d+)?$', val.strip().replace(',', '.')):
                all_float = False
                break
        
        if all_float:
            return 'float64'
        
        # Verificar se são booleanos
        bool_values = {'true', 'false', '1', '0', 'yes', 'no', 'sim', 'não', 'verdadeiro', 'falso'}
        all_bool = True
        for val in str_values:
            if val.strip().lower() not in bool_values:
                all_bool = False
                break
        
        if all_bool:
            return 'bool'
        
        # Verificar se são datas
        date_patterns = [
            r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
            r'\d{2}/\d{2}/\d{4}',  # DD/MM/YYYY
            r'\d{2}-\d{2}-\d{4}',  # DD-MM-YYYY
        ]
        
        all_date = True
        for val in str_values:
            is_date = any(re.match(pattern, val.strip()) for pattern in date_patterns)
            if not is_date:
                all_date = False
                break
        
        if all_date:
            return 'datetime64'
        
        # Default para string
        return 'object'


# Funções de conveniência para compatibilidade
def preview_file(uploaded_file, sep=',', header_row=0, sheet_name=None):
    """Função wrapper para compatibilidade"""
    service = FileProcessingService()
    return service.preview_file(uploaded_file, sep, header_row, sheet_name)


def get_excel_sheets(uploaded_file):
    """Função wrapper para compatibilidade"""
    service = FileProcessingService()
    return service.get_excel_sheets(uploaded_file) 