import boto3
import pandas as pd
from datetime import datetime
import io
import os
from botocore.exceptions import ClientError, NoCredentialsError

class DataUploader:
    """Classe para upload de dados para AWS S3"""
    
    def __init__(self, aws_access_key, aws_secret_key, bucket_name, region_name='us-east-1'):
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key
        self.bucket_name = bucket_name
        self.region_name = region_name
        self.s3_client = None
        
        try:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key,
                region_name=region_name
            )
        except Exception as e:
            print(f"Erro ao conectar com S3: {str(e)}")
    
    def upload_to_s3(self, df, original_filename, schema_data):
        """
        Upload DataFrame para S3
        
        Args:
            df (pd.DataFrame): DataFrame para upload
            original_filename (str): Nome do arquivo original
            schema_data (dict): Schema usado na validação
            
        Returns:
            tuple: (success, message)
        """
        try:
            if self.s3_client is None:
                return False, "Cliente S3 não configurado"
            
            # Gerar nome do arquivo com timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            base_name = os.path.splitext(original_filename)[0]
            
            # Estrutura de pastas por data
            folder_date = datetime.now().strftime("%Y/%m/%d")
            s3_key = f"uploads/{folder_date}/{base_name}_{timestamp}.csv"
            
            # Converter DataFrame para CSV
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False, encoding='utf-8')
            csv_content = csv_buffer.getvalue()
            
            # Metadata do arquivo
            metadata = {
                'original_filename': original_filename,
                'upload_timestamp': datetime.now().isoformat(),
                'schema_used': schema_data.get('schema', {}).get('table_name', 'unknown'),
                'row_count': str(len(df)),
                'column_count': str(len(df.columns)),
                'file_size_bytes': str(len(csv_content.encode('utf-8')))
            }
            
            # Upload para S3
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=csv_content.encode('utf-8'),
                ContentType='text/csv',
                Metadata=metadata
            )
            
            return True, f"Arquivo enviado com sucesso: {s3_key}"
            
        except NoCredentialsError:
            return False, "Credenciais AWS não encontradas ou inválidas"
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchBucket':
                return False, f"Bucket '{self.bucket_name}' não encontrado"
            elif error_code == 'AccessDenied':
                return False, "Acesso negado. Verifique as permissões do bucket"
            else:
                return False, f"Erro AWS: {e.response['Error']['Message']}"
        except Exception as e:
            return False, f"Erro inesperado: {str(e)}"
    
    def test_connection(self):
        """
        Testa a conexão com S3
        
        Returns:
            tuple: (success, message)
        """
        try:
            if self.s3_client is None:
                return False, "Cliente S3 não configurado"
            
            # Tentar listar objetos no bucket (apenas verificar acesso)
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            return True, "Conexão com S3 estabelecida com sucesso"
            
        except NoCredentialsError:
            return False, "Credenciais AWS não encontradas ou inválidas"
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                return False, f"Bucket '{self.bucket_name}' não encontrado"
            elif error_code == '403':
                return False, "Acesso negado. Verifique as permissões"
            else:
                return False, f"Erro AWS: {e.response['Error']['Message']}"
        except Exception as e:
            return False, f"Erro de conexão: {str(e)}"
    
    def list_uploads(self, prefix="uploads/", max_files=100):
        """
        Lista arquivos enviados no bucket
        
        Args:
            prefix (str): Prefixo para filtrar arquivos
            max_files (int): Número máximo de arquivos a retornar
            
        Returns:
            list: Lista de dicionários com informações dos arquivos
        """
        try:
            if self.s3_client is None:
                return []
            
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix,
                MaxKeys=max_files
            )
            
            files = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    # Buscar metadata do arquivo
                    try:
                        head_response = self.s3_client.head_object(
                            Bucket=self.bucket_name,
                            Key=obj['Key']
                        )
                        metadata = head_response.get('Metadata', {})
                    except:
                        metadata = {}
                    
                    file_info = {
                        'key': obj['Key'],
                        'filename': obj['Key'].split('/')[-1],
                        'size': obj['Size'],
                        'last_modified': obj['LastModified'],
                        'original_filename': metadata.get('original_filename', 'N/A'),
                        'schema_used': metadata.get('schema_used', 'N/A'),
                        'row_count': metadata.get('row_count', 'N/A'),
                        'column_count': metadata.get('column_count', 'N/A')
                    }
                    files.append(file_info)
            
            # Ordenar por data de modificação (mais recente primeiro)
            files.sort(key=lambda x: x['last_modified'], reverse=True)
            return files
            
        except Exception as e:
            print(f"Erro ao listar uploads: {str(e)}")
            return []
    
    def download_file(self, s3_key):
        """
        Download de um arquivo do S3
        
        Args:
            s3_key (str): Chave do arquivo no S3
            
        Returns:
            tuple: (success, content/error_message)
        """
        try:
            if self.s3_client is None:
                return False, "Cliente S3 não configurado"
            
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=s3_key
            )
            
            content = response['Body'].read().decode('utf-8')
            return True, content
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchKey':
                return False, "Arquivo não encontrado"
            else:
                return False, f"Erro AWS: {e.response['Error']['Message']}"
        except Exception as e:
            return False, f"Erro ao baixar arquivo: {str(e)}"
    
    def delete_file(self, s3_key):
        """
        Deleta um arquivo do S3
        
        Args:
            s3_key (str): Chave do arquivo no S3
            
        Returns:
            tuple: (success, message)
        """
        try:
            if self.s3_client is None:
                return False, "Cliente S3 não configurado"
            
            self.s3_client.delete_object(
                Bucket=self.bucket_name,
                Key=s3_key
            )
            
            return True, f"Arquivo deletado com sucesso: {s3_key}"
            
        except ClientError as e:
            return False, f"Erro AWS: {e.response['Error']['Message']}"
        except Exception as e:
            return False, f"Erro ao deletar arquivo: {str(e)}" 