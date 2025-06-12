import boto3
import duckdb
from datetime import datetime
from botocore.exceptions import ClientError, NoCredentialsError
from typing import Dict, List, Any

class S3HistoryManager:
    """Gerencia o histórico de uploads no S3 usando DuckDB"""
    
    def __init__(self, aws_access_key, aws_secret_key, bucket_name, region_name='us-east-1'):
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key
        self.bucket_name = bucket_name
        self.region_name = region_name
        self.s3_client = None
        self.conn = duckdb.connect(":memory:")
        
        try:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key,
                region_name=region_name
            )
        except Exception as e:
            print(f"Erro ao conectar com S3: {str(e)}")
    
    def get_upload_history(self, prefix="uploads/", limit=50) -> List[Dict]:
        """
        Recupera o histórico de uploads usando DuckDB
        
        Args:
            prefix (str): Prefixo para filtrar arquivos
            limit (int): Número máximo de registros
            
        Returns:
            List[Dict]: Lista de dicionários com histórico de uploads
        """
        try:
            if self.s3_client is None:
                return []
            
            # Listar objetos no S3
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix,
                MaxKeys=limit
            )
            
            if 'Contents' not in response:
                return []
            
            # Coletar dados dos arquivos
            files_data = []
            for obj in response['Contents']:
                try:
                    # Buscar metadata
                    head_response = self.s3_client.head_object(
                        Bucket=self.bucket_name,
                        Key=obj['Key']
                    )
                    metadata = head_response.get('Metadata', {})
                    
                    file_info = {
                        'file_key': obj['Key'],
                        'filename': obj['Key'].split('/')[-1],
                        'original_filename': metadata.get('original_filename', 'N/A'),
                        'upload_date': obj['LastModified'].isoformat(),
                        'file_size_bytes': obj['Size'],
                        'file_size_mb': round(obj['Size'] / (1024 * 1024), 2),
                        'schema_used': metadata.get('schema_used', 'N/A'),
                        'row_count': int(metadata.get('row_count', 0)) if metadata.get('row_count', '0').isdigit() else 0,
                        'column_count': int(metadata.get('column_count', 0)) if metadata.get('column_count', '0').isdigit() else 0,
                        'upload_timestamp': metadata.get('upload_timestamp', obj['LastModified'].isoformat())
                    }
                    files_data.append(file_info)
                    
                except Exception as e:
                    # Se der erro ao buscar metadata, ainda inclui informações básicas
                    file_info = {
                        'file_key': obj['Key'],
                        'filename': obj['Key'].split('/')[-1],
                        'original_filename': 'N/A',
                        'upload_date': obj['LastModified'].isoformat(),
                        'file_size_bytes': obj['Size'],
                        'file_size_mb': round(obj['Size'] / (1024 * 1024), 2),
                        'schema_used': 'N/A',
                        'row_count': 0,
                        'column_count': 0,
                        'upload_timestamp': obj['LastModified'].isoformat()
                    }
                    files_data.append(file_info)
            
            if not files_data:
                return []
            
            # Usar DuckDB para processar os dados
            self.conn.register('files_temp', files_data)
            
            # Query SQL para processar e ordenar os dados
            query = """
            SELECT 
                filename,
                original_filename,
                upload_date,
                file_size_mb,
                schema_used,
                row_count,
                column_count,
                file_key
            FROM files_temp
            ORDER BY upload_date DESC
            LIMIT ?
            """
            
            result = self.conn.execute(query, [limit]).fetchall()
            
            # Converter resultado para lista de dicionários
            columns = ['filename', 'original_filename', 'upload_date', 'file_size_mb', 
                      'schema_used', 'row_count', 'column_count', 'file_key']
            
            result_list = []
            for row in result:
                row_dict = dict(zip(columns, row))
                result_list.append(row_dict)
            
            return result_list
            
        except Exception as e:
            print(f"Erro ao recuperar histórico: {str(e)}")
            return []
    
    def get_upload_statistics(self) -> Dict[str, Any]:
        """
        Retorna estatísticas dos uploads usando DuckDB
        
        Returns:
            dict: Estatísticas dos uploads
        """
        try:
            # Primeiro, obter todos os dados
            history_list = self.get_upload_history(limit=1000)  # Buscar mais registros para estatísticas
            
            if not history_list:
                return {
                    'total_files': 0,
                    'total_size_mb': 0,
                    'total_rows': 0,
                    'avg_file_size_mb': 0,
                    'files_today': 0,
                    'schemas_used': []
                }
            
            # Usar DuckDB para calcular estatísticas
            self.conn.register('history', history_list)
            
            # Estatísticas gerais
            stats_query = """
            SELECT 
                COUNT(*) as total_files,
                ROUND(SUM(CAST(file_size_mb AS DOUBLE)), 2) as total_size_mb,
                SUM(CAST(row_count AS INTEGER)) as total_rows,
                ROUND(AVG(CAST(file_size_mb AS DOUBLE)), 2) as avg_file_size_mb
            FROM history
            """
            
            stats_result = self.conn.execute(stats_query).fetchone()
            
            # Schemas mais usados
            schemas_query = """
            SELECT 
                schema_used,
                COUNT(*) as count
            FROM history
            WHERE schema_used != 'N/A'
            GROUP BY schema_used
            ORDER BY count DESC
            LIMIT 5
            """
            
            schemas_result = self.conn.execute(schemas_query).fetchall()
            
            statistics = {
                'total_files': int(stats_result[0]) if stats_result[0] else 0,
                'total_size_mb': float(stats_result[1]) if stats_result[1] else 0.0,
                'total_rows': int(stats_result[2]) if stats_result[2] else 0,
                'avg_file_size_mb': float(stats_result[3]) if stats_result[3] else 0.0,
                'files_today': 0,  # Simplificado - pode ser implementado depois se necessário
                'schemas_used': [{'schema': row[0], 'count': row[1]} for row in schemas_result]
            }
            
            return statistics
            
        except Exception as e:
            print(f"Erro ao calcular estatísticas: {str(e)}")
            return {
                'total_files': 0,
                'total_size_mb': 0,
                'total_rows': 0,
                'avg_file_size_mb': 0,
                'files_today': 0,
                'schemas_used': []
            }
    
    def get_uploads_by_date(self, days=30) -> List[Dict]:
        """
        Obtém uploads agrupados por data
        
        Args:
            days (int): Número de dias para incluir
            
        Returns:
            List[Dict]: Lista com uploads por data
        """
        try:
            history_list = self.get_upload_history(limit=1000)
            
            if not history_list:
                return []
            
            self.conn.register('history', history_list)
            
            # Query para agrupar por data
            query = """
            SELECT 
                DATE(upload_date) as upload_day,
                COUNT(*) as files_count,
                ROUND(SUM(CAST(file_size_mb AS DOUBLE)), 2) as total_size_mb
            FROM history
            WHERE upload_date >= CURRENT_DATE - INTERVAL ? DAYS
            GROUP BY DATE(upload_date)
            ORDER BY upload_day DESC
            """
            
            result = self.conn.execute(query, [days]).fetchall()
            
            return [
                {
                    'upload_day': row[0],
                    'files_count': row[1],
                    'total_size_mb': row[2]
                }
                for row in result
            ]
            
        except Exception as e:
            print(f"Erro ao obter uploads por data: {str(e)}")
            return []
    
    def search_files(self, search_term: str, limit=20) -> List[Dict]:
        """
        Busca arquivos por termo
        
        Args:
            search_term (str): Termo de busca
            limit (int): Limite de resultados
            
        Returns:
            List[Dict]: Lista de arquivos encontrados
        """
        try:
            history_list = self.get_upload_history(limit=500)
            
            if not history_list:
                return []
            
            self.conn.register('history', history_list)
            
            # Query de busca
            query = """
            SELECT 
                filename,
                original_filename,
                upload_date,
                file_size_mb,
                schema_used,
                file_key
            FROM history
            WHERE 
                LOWER(filename) LIKE LOWER(?) OR 
                LOWER(original_filename) LIKE LOWER(?) OR
                LOWER(schema_used) LIKE LOWER(?)
            ORDER BY upload_date DESC
            LIMIT ?
            """
            
            search_pattern = f"%{search_term}%"
            result = self.conn.execute(query, [search_pattern, search_pattern, search_pattern, limit]).fetchall()
            
            columns = ['filename', 'original_filename', 'upload_date', 'file_size_mb', 'schema_used', 'file_key']
            
            return [dict(zip(columns, row)) for row in result]
            
        except Exception as e:
            print(f"Erro ao buscar arquivos: {str(e)}")
            return []
    
    def test_connection(self) -> tuple:
        """
        Testa conexão com S3
        
        Returns:
            tuple: (success, message)
        """
        try:
            if self.s3_client is None:
                return False, "Cliente S3 não inicializado"
            
            # Tentar listar objetos como teste
            self.s3_client.list_objects_v2(Bucket=self.bucket_name, MaxKeys=1)
            return True, "Conexão estabelecida com sucesso"
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchBucket':
                return False, f"Bucket '{self.bucket_name}' não encontrado"
            elif error_code in ['AccessDenied', 'Forbidden']:
                return False, "Acesso negado. Verifique as permissões"
            else:
                return False, f"Erro AWS: {e.response['Error']['Message']}"
        except Exception as e:
            return False, f"Erro de conexão: {str(e)}" 