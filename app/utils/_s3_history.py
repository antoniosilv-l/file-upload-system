import boto3
import pandas as pd
import duckdb
from datetime import datetime
from botocore.exceptions import ClientError, NoCredentialsError

class S3HistoryManager:
    """Gerencia o histórico de uploads no S3 usando DuckDB"""
    
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
    
    def get_upload_history(self, prefix="uploads/", limit=50):
        """
        Recupera o histórico de uploads usando DuckDB
        
        Args:
            prefix (str): Prefixo para filtrar arquivos
            limit (int): Número máximo de registros
            
        Returns:
            pd.DataFrame: DataFrame com histórico de uploads
        """
        try:
            if self.s3_client is None:
                return pd.DataFrame()
            
            # Listar objetos no S3
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix,
                MaxKeys=limit
            )
            
            if 'Contents' not in response:
                return pd.DataFrame()
            
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
                        'upload_date': obj['LastModified'],
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
                        'upload_date': obj['LastModified'],
                        'file_size_bytes': obj['Size'],
                        'file_size_mb': round(obj['Size'] / (1024 * 1024), 2),
                        'schema_used': 'N/A',
                        'row_count': 0,
                        'column_count': 0,
                        'upload_timestamp': obj['LastModified'].isoformat()
                    }
                    files_data.append(file_info)
            
            if not files_data:
                return pd.DataFrame()
            
            # Usar DuckDB para processar os dados
            conn = duckdb.connect()
            
            # Criar DataFrame temporário
            df_temp = pd.DataFrame(files_data)
            
            # Registrar DataFrame no DuckDB
            conn.register('files_temp', df_temp)
            
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
                file_key,
                DATE_TRUNC('day', upload_date) as upload_day
            FROM files_temp
            ORDER BY upload_date DESC
            LIMIT ?
            """
            
            result_df = conn.execute(query, [limit]).fetchdf()
            conn.close()
            
            return result_df
            
        except Exception as e:
            print(f"Erro ao recuperar histórico: {str(e)}")
            return pd.DataFrame()
    
    def get_upload_statistics(self):
        """
        Retorna estatísticas dos uploads usando DuckDB
        
        Returns:
            dict: Estatísticas dos uploads
        """
        try:
            # Primeiro, obter todos os dados
            history_df = self.get_upload_history(limit=1000)  # Buscar mais registros para estatísticas
            
            if history_df.empty:
                return {
                    'total_files': 0,
                    'total_size_mb': 0,
                    'total_rows': 0,
                    'avg_file_size_mb': 0,
                    'files_today': 0,
                    'schemas_used': []
                }
            
            # Usar DuckDB para calcular estatísticas
            conn = duckdb.connect()
            conn.register('history', history_df)
            
            # Estatísticas gerais
            stats_query = """
            SELECT 
                COUNT(*) as total_files,
                ROUND(SUM(file_size_mb), 2) as total_size_mb,
                SUM(row_count) as total_rows,
                ROUND(AVG(file_size_mb), 2) as avg_file_size_mb,
                COUNT(CASE WHEN DATE(upload_date) = CURRENT_DATE THEN 1 END) as files_today
            FROM history
            """
            
            stats_result = conn.execute(stats_query).fetchone()
            
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
            
            schemas_result = conn.execute(schemas_query).fetchall()
            
            conn.close()
            
            statistics = {
                'total_files': int(stats_result[0]) if stats_result[0] else 0,
                'total_size_mb': float(stats_result[1]) if stats_result[1] else 0.0,
                'total_rows': int(stats_result[2]) if stats_result[2] else 0,
                'avg_file_size_mb': float(stats_result[3]) if stats_result[3] else 0.0,
                'files_today': int(stats_result[4]) if stats_result[4] else 0,
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
    
    def get_uploads_by_date(self, days=30):
        """
        Retorna uploads agrupados por data usando DuckDB
        
        Args:
            days (int): Número de dias para analisar
            
        Returns:
            pd.DataFrame: DataFrame com uploads por data
        """
        try:
            history_df = self.get_upload_history(limit=1000)
            
            if history_df.empty:
                return pd.DataFrame()
            
            conn = duckdb.connect()
            conn.register('history', history_df)
            
            query = """
            SELECT 
                DATE(upload_date) as date,
                COUNT(*) as files_count,
                ROUND(SUM(file_size_mb), 2) as total_size_mb,
                SUM(row_count) as total_rows
            FROM history
            WHERE upload_date >= CURRENT_DATE - INTERVAL ? DAYS
            GROUP BY DATE(upload_date)
            ORDER BY date DESC
            """
            
            result_df = conn.execute(query, [days]).fetchdf()
            conn.close()
            
            return result_df
            
        except Exception as e:
            print(f"Erro ao agrupar uploads por data: {str(e)}")
            return pd.DataFrame()
    
    def search_files(self, search_term, limit=20):
        """
        Busca arquivos por nome usando DuckDB
        
        Args:
            search_term (str): Termo de busca
            limit (int): Limite de resultados
            
        Returns:
            pd.DataFrame: DataFrame com resultados da busca
        """
        try:
            history_df = self.get_upload_history(limit=500)
            
            if history_df.empty:
                return pd.DataFrame()
            
            conn = duckdb.connect()
            conn.register('history', history_df)
            
            query = """
            SELECT *
            FROM history
            WHERE 
                LOWER(filename) LIKE LOWER(?) OR
                LOWER(original_filename) LIKE LOWER(?) OR
                LOWER(schema_used) LIKE LOWER(?)
            ORDER BY upload_date DESC
            LIMIT ?
            """
            
            search_pattern = f"%{search_term}%"
            result_df = conn.execute(query, [search_pattern, search_pattern, search_pattern, limit]).fetchdf()
            conn.close()
            
            return result_df
            
        except Exception as e:
            print(f"Erro na busca: {str(e)}")
            return pd.DataFrame()
    
    def test_connection(self):
        """
        Testa a conexão com S3
        
        Returns:
            bool: True se conectado, False caso contrário
        """
        try:
            if self.s3_client is None:
                return False
            
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            return True
            
        except Exception:
            return False 