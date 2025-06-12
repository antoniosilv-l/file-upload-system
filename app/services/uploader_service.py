import streamlit as st
import duckdb
import boto3
from typing import Tuple, Optional, Dict, Any, List
from datetime import datetime
import os
import io
from botocore.exceptions import ClientError, NoCredentialsError
from app.config.s3_config import s3_config


class S3UploaderService:
    """Serviço de upload para S3 usando DuckDB"""
    
    def __init__(self):
        self.conn = duckdb.connect(":memory:")
        self.s3_client = None
        self._initialize_s3_client()
    
    def _initialize_s3_client(self):
        """Inicializa cliente S3"""
        try:
            if s3_config.is_configured():
                credentials = s3_config.get_credentials()
                self.s3_client = boto3.client(
                    's3',
                    aws_access_key_id=credentials['aws_access_key_id'],
                    aws_secret_access_key=credentials['aws_secret_access_key'],
                    region_name=credentials['region_name']
                )
        except Exception as e:
            st.error(f"Erro ao inicializar cliente S3: {e}")
    
    def is_available(self) -> bool:
        """Verifica se o serviço S3 está disponível"""
        return self.s3_client is not None and s3_config.is_configured()
    
    def upload_file(self, uploaded_file, schema: Dict, subject: str, sub_subject: str) -> Tuple[bool, str]:
        """
        Faz upload de arquivo para S3 após processamento com DuckDB
        
        Args:
            uploaded_file: Arquivo enviado via Streamlit
            schema: Schema YAML de validação
            subject: Assunto do arquivo
            sub_subject: Sub-assunto do arquivo
            
        Returns:
            tuple: (sucesso, mensagem)
        """
        if not self.is_available():
            return False, "Serviço S3 não disponível"
        
        try:
            # Converter arquivo para lista de dicionários usando DuckDB
            data = self._load_file_with_duckdb(uploaded_file)
            
            if data is None:
                return False, "Erro ao processar arquivo"
            
            # Gerar path de destino
            s3_key = self._generate_s3_path(uploaded_file.name, subject, sub_subject)
            
            # Converter para CSV e fazer upload
            csv_content = self._dataframe_to_csv(data)
            
            # Upload para S3
            credentials = s3_config.get_credentials()
            self.s3_client.put_object(
                Bucket=credentials['bucket_name'],
                Key=s3_key,
                Body=csv_content.encode('utf-8'),
                ContentType='text/csv',
                Metadata={
                    'subject': subject,
                    'sub_subject': sub_subject,
                    'original_filename': uploaded_file.name,
                    'upload_date': datetime.now().isoformat(),
                    'schema_used': schema.get('description', 'N/A')
                }
            )
            
            return True, f"Upload realizado com sucesso: {s3_key}"
            
        except Exception as e:
            return False, f"Erro durante upload: {str(e)}"
    
    def _load_file_with_duckdb(self, uploaded_file) -> Optional[List[Dict]]:
        """Carrega arquivo usando DuckDB e retorna lista de dicionários"""
        file_extension = uploaded_file.name.split('.')[-1].lower()
        
        try:
            # Salvar arquivo temporariamente
            temp_path = f"/tmp/temp_upload_{uploaded_file.name}"
            with open(temp_path, "wb") as f:
                f.write(uploaded_file.getvalue())
            
            if file_extension == 'csv':
                # Auto-detectar separador
                sample = uploaded_file.read(1024).decode('utf-8', errors='ignore')
                uploaded_file.seek(0)
                
                separators = [',', ';', '\t', '|']
                separator_counts = {sep: sample.count(sep) for sep in separators}
                best_sep = max(separator_counts, key=separator_counts.get) if separator_counts else ','
                
                result = self.conn.execute(f"""
                    SELECT * FROM read_csv('{temp_path}', 
                                         delim='{best_sep}', 
                                         header=true)
                """).fetchall()
                
                # Obter nomes das colunas
                columns = [desc[0] for desc in self.conn.description]
                
                # Converter para lista de dicionários
                data = []
                for row in result:
                    row_dict = dict(zip(columns, row))
                    data.append(row_dict)
                
            elif file_extension in ['xlsx', 'xls']:
                result = self.conn.execute(f"""
                    SELECT * FROM read_excel('{temp_path}', header=true)
                """).fetchall()
                
                # Obter nomes das colunas
                columns = [desc[0] for desc in self.conn.description]
                
                # Converter para lista de dicionários
                data = []
                for row in result:
                    row_dict = dict(zip(columns, row))
                    data.append(row_dict)
            else:
                return None
            
            # Limpar arquivo temporário
            if os.path.exists(temp_path):
                os.remove(temp_path)
            
            return data
            
        except Exception as e:
            st.error(f"Erro ao carregar arquivo: {e}")
            return None
    
    def _dataframe_to_csv(self, data: List[Dict]) -> str:
        """Converte lista de dicionários para CSV string"""
        try:
            if not data:
                return ""
            
            # Registrar dados no DuckDB
            self.conn.register('temp_data', data)
            
            # Exportar como CSV
            csv_result = self.conn.execute("COPY temp_data TO '/tmp/export.csv' (HEADER, DELIMITER ',')").fetchall()
            
            # Ler o arquivo gerado
            with open('/tmp/export.csv', 'r', encoding='utf-8') as f:
                csv_content = f.read()
            
            # Limpar arquivo temporário
            if os.path.exists('/tmp/export.csv'):
                os.remove('/tmp/export.csv')
            
            return csv_content
            
        except Exception as e:
            # Fallback: criar CSV manualmente
            if not data:
                return ""
            
            import csv
            import io
            
            output = io.StringIO()
            writer = csv.DictWriter(output, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
            
            return output.getvalue()
    
    def _generate_s3_path(self, filename: str, subject: str, sub_subject: str) -> str:
        """Gera path estruturado para S3"""
        # Normalizar filename
        base_name = filename.rsplit('.', 1)[0]
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        normalized_filename = f"{base_name}_{timestamp}.csv"
        
        # Estrutura: sistema/assunto/sub_assunto/YYYY-MM-DD/arquivo.csv
        date_folder = datetime.now().strftime("%Y-%m-%d")
        return f"sistema/{subject}/{sub_subject}/{date_folder}/{normalized_filename}"
    
    def get_upload_path_preview(self, filename: str, subject: str, sub_subject: str) -> str:
        """Retorna preview do path onde o arquivo será salvo"""
        return self._generate_s3_path(filename, subject, sub_subject)
    
    def list_uploaded_files(self, prefix: str = "sistema/", limit: int = 100) -> List[Dict]:
        """Lista arquivos enviados no S3"""
        if not self.is_available():
            return []
        
        try:
            credentials = s3_config.get_credentials()
            response = self.s3_client.list_objects_v2(
                Bucket=credentials['bucket_name'],
                Prefix=prefix,
                MaxKeys=limit
            )
            
            files = []
            for obj in response.get('Contents', []):
                file_info = {
                    'key': obj['Key'],
                    'size': obj['Size'],
                    'last_modified': obj['LastModified'],
                    'filename': obj['Key'].split('/')[-1]
                }
                
                # Tentar obter metadata
                try:
                    metadata_response = self.s3_client.head_object(
                        Bucket=credentials['bucket_name'],
                        Key=obj['Key']
                    )
                    file_info['metadata'] = metadata_response.get('Metadata', {})
                except:
                    file_info['metadata'] = {}
                
                files.append(file_info)
            
            return files
            
        except Exception as e:
            st.error(f"Erro ao listar arquivos: {e}")
            return []


# Função de conveniência para compatibilidade
def upload_file_to_s3(uploaded_file, schema_data, subject, sub_subject):
    """Função wrapper para compatibilidade com código existente"""
    service = S3UploaderService()
    return service.upload_file(uploaded_file, schema_data, subject, sub_subject)


def get_upload_path_preview(subject, sub_subject, filename):
    """Função wrapper para preview do caminho"""
    service = S3UploaderService()
    return service.get_upload_path_preview(filename, subject, sub_subject) 