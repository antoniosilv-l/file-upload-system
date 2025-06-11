import boto3
import duckdb
import time
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from app.config.s3_config import s3_config
import json
import streamlit as st


class AthenaService:
    """Serviço para integração com AWS Athena usando DuckDB"""
    
    def __init__(self):
        self.athena_client = None
        self.s3_client = None
        self.result_bucket = None
        self.result_prefix = "athena-results/"
        self.conn = duckdb.connect(":memory:")
        self._initialize_clients()
    
    def _initialize_clients(self):
        """Inicializa clientes AWS"""
        try:
            if s3_config.is_configured():
                credentials = s3_config.get_credentials()
                
                self.athena_client = boto3.client(
                    'athena',
                    aws_access_key_id=credentials['aws_access_key_id'],
                    aws_secret_access_key=credentials['aws_secret_access_key'],
                    region_name=credentials['region_name']
                )
                
                self.s3_client = boto3.client(
                    's3',
                    aws_access_key_id=credentials['aws_access_key_id'],
                    aws_secret_access_key=credentials['aws_secret_access_key'],
                    region_name=credentials['region_name']
                )
                
                # Usar mesmo bucket para resultados
                self.result_bucket = credentials['bucket_name']
                
        except Exception as e:
            print(f"Erro ao inicializar clientes Athena: {str(e)}")
    
    def is_available(self) -> bool:
        """Verifica se o serviço está disponível"""
        return self.athena_client is not None and self.s3_client is not None
    
    def get_databases(self) -> List[str]:
        """Lista databases disponíveis no Athena"""
        try:
            if not self.is_available():
                return []
            
            response = self.athena_client.list_databases(CatalogName='AwsDataCatalog')
            databases = [db['Name'] for db in response['DatabaseList']]
            return sorted(databases)
            
        except Exception as e:
            print(f"Erro ao listar databases: {str(e)}")
            return []
    
    def get_tables(self, database: str) -> List[str]:
        """Lista tabelas de um database específico"""
        try:
            if not self.is_available() or not database:
                return []
            
            response = self.athena_client.list_table_metadata(
                CatalogName='AwsDataCatalog',
                DatabaseName=database
            )
            
            tables = [table['Name'] for table in response['TableMetadataList']]
            return sorted(tables)
            
        except Exception as e:
            print(f"Erro ao listar tabelas: {str(e)}")
            return []
    
    def get_table_schema(self, database: str, table: str) -> Optional[Dict]:
        """Obtém schema de uma tabela específica"""
        try:
            if not self.is_available() or not database or not table:
                return None
            
            response = self.athena_client.get_table_metadata(
                CatalogName='AwsDataCatalog',
                DatabaseName=database,
                TableName=table
            )
            
            table_metadata = response['TableMetadata']
            
            # Extrair informações do schema
            columns = []
            for column in table_metadata.get('Columns', []):
                columns.append({
                    'name': column['Name'],
                    'type': column['Type'],
                    'comment': column.get('Comment', '')
                })
            
            schema_info = {
                'database': database,
                'table': table,
                'columns': columns,
                'location': table_metadata.get('Parameters', {}).get('location', ''),
                'input_format': table_metadata.get('Parameters', {}).get('inputformat', ''),
                'output_format': table_metadata.get('Parameters', {}).get('outputformat', ''),
                'serialization_lib': table_metadata.get('Parameters', {}).get('serialization.lib', ''),
                'partition_keys': [
                    {'name': pk['Name'], 'type': pk['Type']} 
                    for pk in table_metadata.get('PartitionKeys', [])
                ],
                'table_type': table_metadata.get('TableType', 'EXTERNAL_TABLE'),
                'creation_time': table_metadata.get('CreateTime', datetime.now())
            }
            
            return schema_info
            
        except Exception as e:
            print(f"Erro ao obter schema da tabela: {str(e)}")
            return None
    
    def execute_query(self, query: str, database: str = None) -> Tuple[bool, str, Optional[str]]:
        """
        Executa query no Athena
        
        Returns:
            tuple: (success, message, execution_id)
        """
        try:
            if not self.is_available():
                return False, "Serviço Athena não disponível", None
            
            # Configuração da query
            query_config = {
                'QueryString': query,
                'ResultConfiguration': {
                    'OutputLocation': f's3://{self.result_bucket}/{self.result_prefix}'
                }
            }
            
            if database:
                query_config['QueryExecutionContext'] = {'Database': database}
            
            # Executar query
            response = self.athena_client.start_query_execution(**query_config)
            execution_id = response['QueryExecutionId']
            
            # Aguardar conclusão
            while True:
                response = self.athena_client.get_query_execution(QueryExecutionId=execution_id)
                status = response['QueryExecution']['Status']['State']
                
                if status in ['SUCCEEDED']:
                    return True, f"Query executada com sucesso. ID: {execution_id}", execution_id
                elif status in ['FAILED', 'CANCELLED']:
                    error_message = response['QueryExecution']['Status'].get('StateChangeReason', 'Erro desconhecido')
                    return False, f"Query falhou: {error_message}", execution_id
                
                time.sleep(1)  # Aguardar 1 segundo antes de verificar novamente
                
        except Exception as e:
            return False, f"Erro ao executar query: {str(e)}", None
    
    def get_query_results(self, execution_id: str) -> Optional[List[Dict]]:
        """Obtém resultados de uma query executada como lista de dicionários"""
        try:
            if not self.is_available():
                return None
            
            response = self.athena_client.get_query_results(QueryExecutionId=execution_id)
            
            # Extrair dados
            rows = response['ResultSet']['Rows']
            if not rows:
                return []
            
            # Primeira linha são os headers
            headers = [col['VarCharValue'] for col in rows[0]['Data']]
            
            # Dados das linhas
            data = []
            for row in rows[1:]:  # Pular header
                row_data = {}
                for i, col in enumerate(row['Data']):
                    if i < len(headers):
                        row_data[headers[i]] = col.get('VarCharValue', '')
                data.append(row_data)
            
            return data
            
        except Exception as e:
            print(f"Erro ao obter resultados: {str(e)}")
            return None
    
    def can_modify_table(self, schema_info: Dict) -> Tuple[bool, str]:
        """
        Verifica se uma tabela pode ser modificada baseado no schema
        
        Returns:
            tuple: (can_modify, reason)
        """
        if not schema_info:
            return False, "Schema não disponível"
        
        table_type = schema_info.get('table_type', '').upper()
        location = schema_info.get('location', '')
        
        # Verificar se é tabela externa no S3
        if table_type != 'EXTERNAL_TABLE':
            return False, f"Tipo de tabela '{table_type}' não suportado para modificação"
        
        # Verificar se tem localização S3
        if not location.startswith('s3://'):
            return False, "Tabela deve estar localizada no S3 para modificação"
        
        # Verificar formato suportado
        input_format = schema_info.get('input_format', '').lower()
        serialization_lib = schema_info.get('serialization_lib', '').lower()
        
        supported_formats = [
            'textinputformat',
            'org.apache.hadoop.mapred.textinputformat',
            'parquet',
            'org.apache.hadoop.hive.ql.io.parquet'
        ]
        
        supported_serialization = [
            'lazysimpleserdeutil',
            'org.apache.hadoop.hive.serde2.lazy.lazysimpleserdeutil',
            'parquet',
            'org.apache.hadoop.hive.ql.io.parquet.serde.parquethiveserde'
        ]
        
        format_supported = any(fmt in input_format for fmt in supported_formats)
        serde_supported = any(serde in serialization_lib for serde in supported_serialization)
        
        if not (format_supported or serde_supported):
            return False, f"Formato não suportado: {input_format}, {serialization_lib}"
        
        return True, "Tabela pode ser modificada"
    
    def preview_table_data(self, database: str, table: str, limit: int = 10) -> Optional[List[Dict]]:
        """Faz preview dos dados de uma tabela"""
        try:
            query = f"SELECT * FROM {database}.{table} LIMIT {limit}"
            success, message, execution_id = self.execute_query(query, database)
            
            if success and execution_id:
                return self.get_query_results(execution_id)
            else:
                print(f"Erro no preview: {message}")
                return None
                
        except Exception as e:
            print(f"Erro ao fazer preview: {str(e)}")
            return None
    
    def create_changes_log_table(self, database: str, target_table: str = None) -> Tuple[bool, str]:
        """
        Cria tabela de log de alterações se não existir - usando formato STRUCT
        
        Args:
            database: Nome do database
            target_table: Nome da tabela alvo (ignorado - schema é genérico)
            
        Returns:
            tuple: (success, message)
        """
        try:
            table_name = "data_changes_log"
            
            # Verificar se tabela já existe
            tables = self.get_tables(database)
            if table_name in tables:
                return True, f"Tabela de log {table_name} já existe"
            
            # DDL para tabela de log com campos STRUCT para suportar múltiplas tabelas
            ddl = f"""
CREATE EXTERNAL TABLE {database}.{table_name} (
  `id` string,
  `table_name` string,
  `database_name` string,
  `operation` string,
  `row_identifier` string,
  `before` struct<
    id:string,
    first_name:string,
    last_name:string,
    email:string,
    gender:string,
    ip_address:string,
    is_active:string,
    data:string
  >,
  `after` struct<
    id:string,
    first_name:string,
    last_name:string,
    email:string,
    gender:string,
    ip_address:string,
    is_active:string,
    data:string
  >,
  `updated_by` string,
  `updated_at` string,
  `session_id` string,
  `change_reason` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'field.delim' = '|',
  'collection.delim' = ',',
  'mapkey.delim' = ':',
  'skip.header.line.count' = '1'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://{self.result_bucket}/data-changes-log/{database}/'
TBLPROPERTIES (
  'has_encrypted_data'='false',
  'classification'='csv'
)
"""
            
            success, message, execution_id = self.execute_query(ddl.strip(), database)
            
            if success:
                return True, f"Tabela de log {table_name} criada com sucesso (schema STRUCT para múltiplas tabelas)"
            else:
                return False, f"Erro ao criar tabela de log: {message}"
                
        except Exception as e:
            return False, f"Erro ao criar tabela de log: {str(e)}"
    
    def save_table_changes(
        self,
        database: str,
        table_name: str,
        original_data: List[Dict],
        edited_data: List[Dict],
        user_name: str = "sistema"
    ) -> Tuple[bool, str]:
        """
        Salva alterações na tabela de log em vez de modificar a tabela principal
        
        Args:
            database: Nome do database
            table_name: Nome da tabela
            original_data: Dados originais como lista de dicionários
            edited_data: Dados editados como lista de dicionários
            user_name: Nome do usuário que fez as alterações
            
        Returns:
            tuple: (success, message)
        """
        try:
            # Criar tabela de log se não existir
            log_success, log_message = self.create_changes_log_table(database, table_name)
            if not log_success:
                return False, f"Erro ao preparar tabela de log: {log_message}"
            
            # Gerar logs de alterações
            changes_data = self._generate_change_logs(
                database, table_name, original_data, edited_data, user_name
            )
            
            if not changes_data:
                return True, "Nenhuma alteração detectada"
            
            # Salvar logs no S3
            success, message = self._save_changes_to_s3(database, changes_data)
            
            if success:
                return True, f"{len(changes_data)} alteração(ões) registrada(s) no log"
            else:
                return False, f"Erro ao salvar log: {message}"
                
        except Exception as e:
            return False, f"Erro ao salvar alterações: {str(e)}"
    
    def _generate_change_logs(
        self,
        database: str,
        table_name: str,
        original_data: List[Dict],
        edited_data: List[Dict],
        user_name: str
    ) -> List[Dict]:
        """Gera registros de log das alterações - apenas UPDATEs de dados existentes"""
        import hashlib
        import json
        from datetime import datetime
        import uuid
        
        changes = []
        session_id = str(uuid.uuid4())[:8]
        timestamp = datetime.now().isoformat()
        
        # Identificar colunas chave
        id_column = None
        if original_data:
            # Procurar por colunas que parecem ser IDs
            for col in original_data[0].keys():
                if any(keyword in col.lower() for keyword in ['id', 'key', 'pk']):
                    id_column = col
                    break
            
            # Se não encontrar, usar primeira coluna
            if id_column is None:
                id_column = list(original_data[0].keys())[0] if original_data[0] else None
        
        # Validar que temos a mesma quantidade de registros (apenas edição, não inserção/deleção)
        if len(original_data) != len(edited_data):
            print(f"Aviso: Quantidade de registros diferente. Original: {len(original_data)}, Editado: {len(edited_data)}")
            # Limitar ao menor tamanho para evitar INSERTs/DELETEs
            max_size = min(len(original_data), len(edited_data))
            original_data = original_data[:max_size]
            edited_data = edited_data[:max_size]
        
        # Converter listas em dicionários indexados mantendo ordem original para comparação 1:1
        original_dict = {}
        for i, row in enumerate(original_data):
            # Usar posição da lista para evitar problemas com IDs duplicados/ausentes
            key = i
            original_dict[str(key)] = row
        
        edited_dict = {}
        for i, row in enumerate(edited_data):
            # Usar posição da lista para garantir comparação correta linha por linha
            key = i
            edited_dict[str(key)] = row
        
        # Detectar apenas mudanças em registros existentes (UPDATEs)
        for key in edited_dict:
            if key in original_dict:
                # Comparar linhas existentes
                original_row = original_dict[key]
                edited_row = edited_dict[key]
                
                # Normalizar dados para comparação (convertir None para string vazia, etc.)
                def normalize_row(row):
                    return {k: str(v) if v is not None else '' for k, v in row.items()}
                
                normalized_original = normalize_row(original_row)
                normalized_edited = normalize_row(edited_row)
                
                # Verificar se houve mudança real nos dados
                if normalized_original != normalized_edited:
                    # Usar ID da linha se existir para identificação, senão usar posição
                    row_id = edited_row.get(id_column, key) if id_column else key
                    
                    # Gerar ID único para a mudança
                    change_id = hashlib.md5(
                        f"{database}_{table_name}_{row_id}_{timestamp}".encode()
                    ).hexdigest()
                    
                    change_record = {
                        'id': change_id,
                        'table_name': table_name,
                        'database_name': database,
                        'operation': 'UPDATE',
                        'row_identifier': str(row_id),
                        'before': json.dumps(original_row, default=str),
                        'after': json.dumps(edited_row, default=str),
                        'updated_by': user_name,
                        'updated_at': timestamp,
                        'session_id': session_id,
                        'change_reason': 'Manual edit via Athena interface'
                    }
                    changes.append(change_record)
        
        # NÃO detectar INSERTs ou DELETEs - apenas UPDATEs de dados existentes
        # Isso evita logs de inserção desnecessários quando se está apenas editando dados
        
        return changes
    
    def _save_changes_to_s3(self, database: str, changes_data: List[Dict]) -> Tuple[bool, str]:
        """Salva registros de mudanças no S3 em formato STRUCT"""
        try:
            from datetime import datetime
            
            if not changes_data:
                return False, "Nenhum dado de mudança para salvar"
            
            # Definir colunas esperadas
            columns = ['id', 'table_name', 'database_name', 'operation', 'row_identifier', 
                      'before', 'after', 'updated_by', 'updated_at', 'session_id', 'change_reason']
            
            # Converter para CSV manualmente com formato STRUCT
            csv_lines = []
            csv_lines.append('|'.join(columns))  # Header
            
            for change_record in changes_data:
                # Processar cada registro de mudança
                escaped_row = []
                for column in columns:
                    value = change_record.get(column, '')
                    
                    # Campos STRUCT especiais (before e after)
                    if column in ['before', 'after']:
                        struct_value = self._convert_json_to_struct(str(value))
                        escaped_row.append(struct_value)
                    else:
                        # Para outros campos, apenas escapar pipe
                        escaped_value = str(value).replace('|', '\\|')
                        escaped_row.append(escaped_value)
                
                csv_lines.append('|'.join(escaped_row))
            
            csv_content = '\n'.join(csv_lines)
            
            # Gerar nome do arquivo único
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            s3_key = f"data-changes-log/{database}/changes_{timestamp}.csv"
            
            # Upload para S3
            credentials = s3_config.get_credentials()
            self.s3_client.put_object(
                Bucket=credentials['bucket_name'],
                Key=s3_key,
                Body=csv_content.encode('utf-8'),
                ContentType='text/csv'
            )
            
            return True, f"Log salvo em s3://{credentials['bucket_name']}/{s3_key}"
            
        except Exception as e:
            return False, f"Erro ao salvar no S3: {str(e)}"
    
    def get_table_with_changes(self, database: str, table: str, limit: int = 100) -> Optional[List[Dict]]:
        """
        Obtém dados da tabela aplicando as alterações do log
        
        Args:
            database: Nome do database
            table: Nome da tabela
            limit: Limite de registros
            
        Returns:
            Lista de dicionários com alterações aplicadas OU dados originais se log não existir
        """
        try:
            # Carregar dados originais da tabela
            original_data = self.preview_table_data(database, table, limit)
            if original_data is None:
                return original_data
            
            # Verificar se tabela de log existe E está funcionando
            tables = self.get_tables(database)
            if "data_changes_log" not in tables:
                # Log não existe - retornar dados originais
                return original_data
            
            # Tentar carregar alterações do log para esta tabela
            try:
                log_query = f"""
                SELECT * FROM {database}.data_changes_log 
                WHERE table_name = '{table}' 
                AND database_name = '{database}'
                ORDER BY updated_at DESC
                LIMIT 100
                """
                
                log_success, log_message, log_execution_id = self.execute_query(log_query, database)
                
                if not log_success or not log_execution_id:
                    # Erro ao executar query do log - retornar dados originais
                    print(f"Erro ao consultar log, retornando dados originais: {log_message}")
                    return original_data
                
                log_data = self.get_query_results(log_execution_id)
                
                if not log_data:
                    # Sem alterações no log - retornar dados originais
                    return original_data
                
                # Verificar se o log tem estrutura correta
                required_columns = ['operation', 'row_identifier', 'before', 'after']
                if not all(col in log_data[0] for col in required_columns):
                    # Log com estrutura incorreta - retornar dados originais
                    print(f"Log com estrutura incorreta, retornando dados originais.")
                    return original_data
                
                # Aplicar alterações do log
                result_data = self._apply_changes_from_log(original_data, log_data)
                return result_data
                
            except Exception as log_error:
                # Qualquer erro com o log - retornar dados originais
                print(f"Erro ao processar log, retornando dados originais: {str(log_error)}")
                return original_data
            
        except Exception as e:
            print(f"Erro geral ao obter dados da tabela: {str(e)}")
            return None
    
    def _apply_changes_from_log(self, original_data: List[Dict], log_data: List[Dict]) -> List[Dict]:
        """Aplica alterações do log aos dados originais"""
        # Criar cópia dos dados originais para modificar
        result_data = [row.copy() for row in original_data]
        
        # Criar índice por ID para facilitar busca
        id_column = None
        if result_data:
            # Procurar coluna ID
            for col in result_data[0].keys():
                if any(keyword in col.lower() for keyword in ['id', 'key', 'pk']):
                    id_column = col
                    break
            if id_column is None:
                id_column = list(result_data[0].keys())[0]
        
        # Criar dicionário indexado para busca rápida
        result_dict = {}
        for i, row in enumerate(result_data):
            key = str(row.get(id_column, i)) if id_column else str(i)
            result_dict[key] = row
        
        # Processar alterações em ordem cronológica reversa (mais recente primeiro)
        for log_row in log_data:
            try:
                operation = log_row.get('operation', '')
                row_identifier = log_row.get('row_identifier', '')
                
                # Validar se campos obrigatórios existem
                if not operation or not row_identifier:
                    continue
                
                if operation == 'UPDATE':
                    # Aplicar atualização
                    try:
                        after_values_str = log_row.get('after', '')
                        if not after_values_str or after_values_str.strip() == '':
                            after_values_str = ''
                        
                        # Debug: verificar formato dos dados
                        print(f"Processing UPDATE for row {row_identifier}: {after_values_str[:100]}...")
                        
                        # Converter STRUCT para dicionário
                        after_values = self._convert_struct_to_dict(after_values_str)
                        
                        # Filtrar apenas campos que existem na tabela original
                        if row_identifier in result_dict and after_values:
                            original_keys = set(result_dict[row_identifier].keys())
                            filtered_values = {k: v for k, v in after_values.items() if k in original_keys and v != ''}
                            
                            print(f"Updating row {row_identifier} with: {filtered_values}")
                            result_dict[row_identifier].update(filtered_values)
                        
                    except Exception as e:
                        print(f"Erro ao processar STRUCT em UPDATE: {str(e)}")
                        continue
                
                elif operation == 'INSERT':
                    # Adicionar nova linha
                    try:
                        after_values_str = log_row.get('after', '')
                        if not after_values_str or after_values_str.strip() == '':
                            after_values_str = ''
                        
                        # Converter STRUCT para dicionário
                        after_values = self._convert_struct_to_dict(after_values_str)
                        
                        # Adicionar se não existe
                        if row_identifier not in result_dict and after_values:
                            result_dict[row_identifier] = after_values
                            
                    except Exception as e:
                        print(f"Erro ao processar STRUCT em INSERT: {str(e)}")
                        continue
                
                elif operation == 'DELETE':
                    # Remover linha
                    if row_identifier in result_dict:
                        del result_dict[row_identifier]
                
            except Exception as e:
                print(f"Erro ao aplicar alteração: {str(e)}")
                continue
        
        # Converter de volta para lista
        final_result = list(result_dict.values())
        print(f"Final result has {len(final_result)} rows")
        return final_result
    
    def _unescape_json_from_athena(self, json_str: str) -> str:
        """Remove escape duplo de JSON lido do Athena (compatibilidade com dados antigos)"""
        if not json_str or json_str in ['{}', '']:
            return '{}'
        
        # Se tiver escape duplo ("""""), converter para escape simples ("")
        if '""""' in json_str:
            # Reverter o escape duplo que foi feito incorretamente
            unescaped = json_str.replace('""""', '"')
            return unescaped
        
        return json_str

    def _escape_json_for_athena(self, json_str: str) -> str:
        """Limpa e formata JSON para ser compatível com Athena LazySimpleSerDe"""
        if not json_str or json_str in ['{}', '']:
            return '{}'
        
        try:
            # Parse e re-serialize para garantir JSON válido
            data = json.loads(json_str)
            
            # Re-serializar sem escape adicional
            clean_json = json.dumps(data, ensure_ascii=True, separators=(',', ':'))
            
            return clean_json
            
        except json.JSONDecodeError:
            # Se não conseguir parsear, retornar JSON vazio
            return '{}'

    def create_table_from_file(
        self, 
        database: str, 
        table_name: str, 
        s3_location: str, 
        columns: List[Dict], 
        file_format: str = 'csv'
    ) -> Tuple[bool, str]:
        """
        Cria ou substitui tabela baseada em arquivo S3
        
        Args:
            database: Nome do database
            table_name: Nome da tabela
            s3_location: Localização do arquivo no S3
            columns: Lista de colunas [{'name': str, 'type': str}]
            file_format: Formato do arquivo (csv, parquet)
        """
        try:
            # Primeiro, tentar dropar a tabela se existir
            drop_query = f"DROP TABLE IF EXISTS {database}.{table_name}"
            drop_success, drop_message, drop_execution_id = self.execute_query(drop_query, database)
            
            # Montar DDL baseado no formato
            if file_format.lower() == 'csv':
                # DDL para CSV com sintaxe correta do Athena
                column_definitions = []
                for col in columns:
                    column_definitions.append(f"`{col['name']}` {col['type']}")
                
                columns_ddl = ",\n  ".join(column_definitions)
                
                # Extrair apenas o diretório do S3 (sem o arquivo específico)
                import os
                s3_directory = os.path.dirname(s3_location) + '/'
                
                ddl = f"""
CREATE EXTERNAL TABLE {database}.{table_name} (
  {columns_ddl}
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'field.delim' = ',',
  'skip.header.line.count' = '1'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '{s3_directory}'
"""
            else:
                # DDL para Parquet
                column_definitions = []
                for col in columns:
                    column_definitions.append(f"`{col['name']}` {col['type']}")
                
                columns_ddl = ",\n  ".join(column_definitions)
                s3_directory = os.path.dirname(s3_location) + '/'
                
                ddl = f"""
CREATE EXTERNAL TABLE {database}.{table_name} (
  {columns_ddl}
)
STORED AS PARQUET
LOCATION '{s3_directory}'
"""
            
            success, message, execution_id = self.execute_query(ddl.strip(), database)
            
            if success:
                return True, f"Tabela {table_name} criada com sucesso"
            else:
                return False, f"Erro ao criar tabela: {message}"
                
        except Exception as e:
            return False, f"Erro ao criar tabela: {str(e)}"

    def recreate_changes_log_table(self, database: str, target_table: str = None) -> Tuple[bool, str]:
        """
        Remove e recria a tabela de log de alterações para corrigir problemas de estrutura
        
        Args:
            database: Nome do database
            target_table: Nome da tabela alvo para gerar schema STRUCT (opcional)
            
        Returns:
            tuple: (success, message)
        """
        try:
            table_name = "data_changes_log"
            
            # Primeiro dropar a tabela se existir
            drop_query = f"DROP TABLE IF EXISTS {database}.{table_name}"
            drop_success, drop_message, drop_execution_id = self.execute_query(drop_query, database)
            
            if not drop_success:
                return False, f"Erro ao dropar tabela existente: {drop_message}"
            
            # Limpar arquivos S3 antigos (opcional - pode falhar sem problemas)
            try:
                credentials = s3_config.get_credentials()
                prefix = f"data-changes-log/{database}/"
                
                # Listar objetos para deletar
                response = self.s3_client.list_objects_v2(
                    Bucket=credentials['bucket_name'],
                    Prefix=prefix
                )
                
                if 'Contents' in response:
                    objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
                    if objects_to_delete:
                        self.s3_client.delete_objects(
                            Bucket=credentials['bucket_name'],
                            Delete={'Objects': objects_to_delete}
                        )
                        
            except Exception as cleanup_error:
                print(f"Aviso: Erro ao limpar arquivos S3 antigos: {str(cleanup_error)}")
            
            # Recriar a tabela com estrutura correta
            create_success, create_message = self.create_changes_log_table(database, target_table)
            
            if create_success:
                return True, f"Tabela de log recriada com sucesso. Estrutura limpa."
            else:
                return False, f"Erro ao recriar tabela: {create_message}"
                
        except Exception as e:
            return False, f"Erro ao recriar tabela de log: {str(e)}"

    def _generate_struct_schema_from_table(self, database: str, table: str) -> str:
        """
        Gera schema STRUCT dinamicamente baseado nas colunas da tabela
        
        Args:
            database: Nome do database
            table: Nome da tabela
            
        Returns:
            String com schema STRUCT para usar no DDL
        """
        try:
            # Obter schema da tabela
            schema_info = self.get_table_schema(database, table)
            if not schema_info:
                # Fallback para estrutura genérica
                return "struct<data:string>"
            
            # Construir campos do STRUCT baseado nas colunas da tabela
            struct_fields = []
            for col in schema_info.get('columns', []):
                col_name = col['name']
                col_type = col['type'].lower()
                
                # Mapear tipos do Athena para STRUCT
                if 'int' in col_type or 'bigint' in col_type:
                    struct_type = 'bigint'
                elif 'double' in col_type or 'float' in col_type:
                    struct_type = 'double'
                elif 'boolean' in col_type:
                    struct_type = 'boolean'
                elif 'date' in col_type:
                    struct_type = 'date'
                elif 'timestamp' in col_type:
                    struct_type = 'timestamp'
                else:
                    struct_type = 'string'
                
                struct_fields.append(f"{col_name}:{struct_type}")
            
            if struct_fields:
                return f"struct<{','.join(struct_fields)}>"
            else:
                return "struct<data:string>"
                
        except Exception as e:
            print(f"Erro ao gerar schema STRUCT: {str(e)}")
            return "struct<data:string>"

    def _parse_struct_data(self, struct_str: str, schema_info: Dict) -> dict:
        """
        Parseia dados STRUCT do LazySimpleSerDe de volta para dicionário
        
        Args:
            struct_str: String com dados STRUCT
            schema_info: Informações do schema da tabela para mapear colunas
            
        Returns:
            Dicionário com dados parseados
        """
        if not struct_str or struct_str.strip() == "":
            return {}
        
        try:
            # Obter nomes das colunas em ordem alfabética
            columns = schema_info.get('columns', [])
            sorted_column_names = sorted([col['name'] for col in columns])
            
            # Dividir valores (cuidado com escaping)
            values = struct_str.split(',')
            
            # Mapear valores para colunas
            result = {}
            for i, col_name in enumerate(sorted_column_names):
                if i < len(values):
                    # Reverter escaping
                    value = values[i].replace("\\|", "|").replace("\\,", ",")
                    result[col_name] = value
                else:
                    result[col_name] = ""
            
            return result
            
        except Exception as e:
            print(f"Erro ao parsear STRUCT: {str(e)}")
            return {}

    def _convert_json_to_struct(self, json_str: str) -> str:
        """Converte JSON para formato STRUCT do Athena"""
        if not json_str or json_str in ['{}', '']:
            return ''  # STRUCT vazio sem vírgulas extras
        
        try:
            # Parse JSON
            data = json.loads(json_str)
            
            # Definir ordem dos campos do STRUCT
            struct_fields = ['id', 'first_name', 'last_name', 'email', 'gender', 'ip_address', 'is_active', 'data']
            
            # Extrair valores na ordem correta, substituindo None/ausentes por string vazia
            values = []
            for field in struct_fields:
                value = data.get(field, '')
                # Escapar vírgulas no valor (delimitador de STRUCT)
                escaped_value = str(value).replace(',', '\\,').replace('|', '\\|')
                values.append(escaped_value)
            
            # Retornar valores separados por vírgula (formato STRUCT)
            return ','.join(values)
            
        except json.JSONDecodeError:
            # Se não conseguir parsear, retornar STRUCT vazio
            return ''

    def _convert_struct_to_dict(self, struct_str: str) -> dict:
        """Converte string STRUCT de volta para dicionário"""
        if not struct_str or struct_str.strip() in [',', '{}', '', 'null']:
            return {}
        
        try:
            # Limpar vírgulas no início/fim da string
            cleaned_struct = struct_str.strip().strip(',').strip()
            
            # Verificar se é formato {key=value key2=value2} do Athena
            if cleaned_struct.startswith('{') and cleaned_struct.endswith('}'):
                return self._parse_athena_struct_format(cleaned_struct)
            
            # Formato original separado por vírgulas (fallback)
            return self._parse_comma_separated_struct(cleaned_struct)
            
        except Exception as e:
            print(f"Erro ao converter STRUCT: {str(e)}")
            return {}

    def _parse_athena_struct_format(self, struct_str: str) -> dict:
        """Parse formato STRUCT do Athena: {key=value key2=value2}"""
        # Remover chaves { }
        content = struct_str.strip()[1:-1].strip()
        
        if not content:
            return {}
        
        result = {}
        
        # Estratégia melhorada: usar regex para identificar padrões key=value
        import re
        
        # Encontrar todas as chaves conhecidas seguidas de =
        known_fields = ['id', 'first_name', 'last_name', 'email', 'gender', 'ip_address', 'is_active', 'data']
        
        # Criar padrão regex para capturar key=value
        pattern = r'(\w+)=([^=]*?)(?=\s+\w+=|$)'
        matches = re.findall(pattern, content)
        
        for key, value in matches:
            # Limpar valor (remover espaços extras e vírgulas no início/fim)
            cleaned_value = value.strip().strip(',').strip()
            result[key] = cleaned_value
        
        # Garantir que todos os campos esperados existam
        for field in known_fields:
            if field not in result:
                result[field] = ''
        
        return result

    def _parse_comma_separated_struct(self, struct_str: str) -> dict:
        """Parse formato STRUCT separado por vírgulas (formato antigo)"""
        # Dividir valores (cuidado avec escaping)
        values = []
        current_value = ''
        escaped = False
        
        for char in struct_str:
            if escaped:
                current_value += char
                escaped = False
            elif char == '\\':
                escaped = True
            elif char == ',':
                values.append(current_value)
                current_value = ''
            else:
                current_value += char
        
        # Adicionar último valor
        if current_value:
            values.append(current_value)
        
        # Mapear para campos conhecidos
        struct_fields = ['id', 'first_name', 'last_name', 'email', 'gender', 'ip_address', 'is_active', 'data']
        
        result = {}
        for i, field in enumerate(struct_fields):
            if i < len(values):
                # Reverter escaping
                value = values[i].replace('\\,', ',').replace('\\|', '|')
                result[field] = value if value else ''
            else:
                result[field] = ''
        
        return result

    def debug_struct_conversion(self, struct_data: str) -> dict:
        """Método de debug para conversão STRUCT"""
        print(f"Input STRUCT: {struct_data}")
        result = self._convert_struct_to_dict(struct_data)
        print(f"Converted dict: {result}")
        return result


# Função de conveniência
def get_athena_service() -> AthenaService:
    """Retorna instância do serviço Athena"""
    return AthenaService()

"""
EXEMPLO DE CONSULTAS STRUCT NO ATHENA:

1. Consultar campos específicos dos dados STRUCT:

SELECT 
    id,
    table_name,
    operation,
    row_identifier,
    before.id as id_before,
    before.first_name as first_name_before,
    before.email as email_before,
    after.id as id_after,
    after.first_name as first_name_after,
    after.email as email_after,
    updated_by,
    updated_at
FROM default.data_changes_log 
WHERE table_name = 'users'
ORDER BY updated_at DESC;

2. Filtrar por mudanças específicas:

SELECT 
    row_identifier,
    before.email as old_email,
    after.email as new_email,
    updated_at,
    updated_by
FROM default.data_changes_log 
WHERE table_name = 'users'
  AND before.email != after.email
ORDER BY updated_at DESC;

3. Histórico de um usuário específico:

SELECT 
    operation,
    before.first_name as name_before,
    after.first_name as name_after,
    before.is_active as active_before,
    after.is_active as active_after,
    updated_at,
    change_reason
FROM default.data_changes_log 
WHERE table_name = 'users'
  AND row_identifier = '2'
ORDER BY updated_at DESC;

4. Criar view para facilitar consultas:

CREATE VIEW user_changes_struct_view AS
SELECT 
    id as change_id,
    row_identifier as user_id,
    operation,
    before.first_name as first_name_before,
    after.first_name as first_name_after,
    before.last_name as last_name_before,
    after.last_name as last_name_after,
    before.email as email_before,
    after.email as email_after,
    before.is_active as active_before,
    after.is_active as active_after,
    updated_by,
    updated_at,
    session_id,
    change_reason
FROM default.data_changes_log 
WHERE table_name = 'users';

-- Usar a view:
SELECT * FROM user_changes_struct_view 
WHERE user_id = '2' 
ORDER BY updated_at DESC;

5. Análise de mudanças por período:

SELECT 
    DATE(updated_at) as change_date,
    COUNT(*) as total_changes,
    COUNT(DISTINCT row_identifier) as users_affected,
    COUNT(DISTINCT updated_by) as users_making_changes
FROM default.data_changes_log 
WHERE table_name = 'users'
  AND DATE(updated_at) >= DATE('2025-01-01')
GROUP BY DATE(updated_at)
ORDER BY change_date DESC;

6. Encontrar mudanças de status:

SELECT 
    row_identifier,
    before.is_active as was_active,
    after.is_active as now_active,
    updated_at,
    updated_by
FROM default.data_changes_log 
WHERE table_name = 'users'
  AND before.is_active != after.is_active
ORDER BY updated_at DESC;

""" 