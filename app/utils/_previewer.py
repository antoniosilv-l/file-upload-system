import pandas as pd
import streamlit as st
from io import StringIO, BytesIO

class DataPreviewer:
    """Classe para preview de dados de arquivos CSV e Excel"""
    
    def __init__(self):
        self.supported_formats = ['csv', 'xlsx', 'xls']
    
    def preview_file(self, uploaded_file):
        """
        Faz preview de um arquivo uploaded
        
        Args:
            uploaded_file: Arquivo enviado via Streamlit
            
        Returns:
            pd.DataFrame: DataFrame com os dados do arquivo
        """
        try:
            file_extension = uploaded_file.name.split('.')[-1].lower()
            
            if file_extension not in self.supported_formats:
                st.error(f"Formato não suportado: {file_extension}")
                return None
            
            if file_extension == 'csv':
                return self._preview_csv(uploaded_file)
            elif file_extension in ['xlsx', 'xls']:
                return self._preview_excel(uploaded_file)
                
        except Exception as e:
            st.error(f"Erro ao processar arquivo: {str(e)}")
            return None
    
    def _preview_csv(self, uploaded_file):
        """Preview específico para arquivos CSV"""
        try:
            # Reset file pointer
            uploaded_file.seek(0)
            
            # Tentar diferentes encodings
            encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
            
            for encoding in encodings:
                try:
                    uploaded_file.seek(0)
                    content = uploaded_file.read().decode(encoding)
                    
                    # Detectar separador
                    separators = [',', ';', '\t', '|']
                    best_separator = ','
                    max_columns = 0
                    
                    for sep in separators:
                        try:
                            df_test = pd.read_csv(StringIO(content), sep=sep, nrows=5)
                            if len(df_test.columns) > max_columns:
                                max_columns = len(df_test.columns)
                                best_separator = sep
                        except:
                            continue
                    
                    # Ler o arquivo com o melhor separador
                    df = pd.read_csv(StringIO(content), sep=best_separator)
                    
                    if len(df) > 0:
                        st.success(f"✅ Arquivo CSV carregado com encoding {encoding} e separador '{best_separator}'")
                        return df
                        
                except UnicodeDecodeError:
                    continue
                except Exception as e:
                    st.warning(f"Erro com encoding {encoding}: {str(e)}")
                    continue
            
            st.error("Não foi possível determinar o encoding do arquivo CSV")
            return None
            
        except Exception as e:
            st.error(f"Erro ao processar CSV: {str(e)}")
            return None
    
    def _preview_excel(self, uploaded_file):
        """Preview específico para arquivos Excel"""
        try:
            # Reset file pointer
            uploaded_file.seek(0)
            
            # Ler as abas disponíveis
            excel_file = pd.ExcelFile(uploaded_file)
            sheet_names = excel_file.sheet_names
            
            if len(sheet_names) == 1:
                # Só uma aba, carregar diretamente
                df = pd.read_excel(uploaded_file, sheet_name=sheet_names[0])
                st.success(f"✅ Arquivo Excel carregado (aba: {sheet_names[0]})")
                return df
            else:
                # Múltiplas abas, deixar usuário escolher
                st.info(f"📄 Arquivo Excel com {len(sheet_names)} abas encontradas")
                
                selected_sheet = st.selectbox(
                    "Selecione a aba para processar:",
                    options=sheet_names,
                    key="excel_sheet_selector"
                )
                
                if selected_sheet:
                    uploaded_file.seek(0)
                    df = pd.read_excel(uploaded_file, sheet_name=selected_sheet)
                    st.success(f"✅ Aba '{selected_sheet}' carregada")
                    return df
                else:
                    return None
                    
        except Exception as e:
            st.error(f"Erro ao processar Excel: {str(e)}")
            return None
    
    def get_file_info(self, uploaded_file):
        """
        Retorna informações básicas sobre o arquivo
        
        Args:
            uploaded_file: Arquivo enviado via Streamlit
            
        Returns:
            dict: Informações do arquivo
        """
        info = {
            'name': uploaded_file.name,
            'size': uploaded_file.size,
            'type': uploaded_file.type,
            'extension': uploaded_file.name.split('.')[-1].lower()
        }
        
        # Adicionar tamanho em formato legível
        if info['size'] < 1024:
            info['size_formatted'] = f"{info['size']} bytes"
        elif info['size'] < 1024 * 1024:
            info['size_formatted'] = f"{info['size'] / 1024:.1f} KB"
        else:
            info['size_formatted'] = f"{info['size'] / (1024 * 1024):.1f} MB"
        
        return info
    
    def validate_file_size(self, uploaded_file, max_size_mb=50):
        """
        Valida o tamanho do arquivo
        
        Args:
            uploaded_file: Arquivo enviado
            max_size_mb: Tamanho máximo em MB
            
        Returns:
            bool: True se válido, False caso contrário
        """
        max_size_bytes = max_size_mb * 1024 * 1024
        
        if uploaded_file.size > max_size_bytes:
            st.error(f"Arquivo muito grande! Tamanho máximo: {max_size_mb}MB")
            return False
        
        return True 