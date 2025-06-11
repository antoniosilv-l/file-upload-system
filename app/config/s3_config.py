import os
from pathlib import Path
from typing import Optional

try:
    from dotenv import load_dotenv
    # Carregar .env se existir
    env_path = Path('.env')
    if env_path.exists():
        load_dotenv(env_path)
except ImportError:
    # python-dotenv não instalado, usar apenas variáveis de ambiente do sistema
    pass

class S3Config:
    """Configuração S3 centralizada com suporte a arquivo .env"""
    
    def __init__(self):
        # Carregar credenciais do .env ou variáveis de ambiente
        self.aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        self.bucket_name = os.getenv('S3_BUCKET_NAME')
        self.region_name = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
        

        
    def get_credentials(self) -> dict:
        """
        Retorna as credenciais S3 configuradas
        
        Returns:
            dict: Dicionário com credenciais S3
        """
        return {
            'aws_access_key_id': self.aws_access_key,
            'aws_secret_access_key': self.aws_secret_key,
            'bucket_name': self.bucket_name,
            'region_name': self.region_name
        }
    
    def is_configured(self) -> bool:
        """
        Verifica se as credenciais estão configuradas
        
        Returns:
            bool: True se configurado, False caso contrário
        """
        required_fields = [
            self.aws_access_key,
            self.aws_secret_key,
            self.bucket_name
        ]
        
        # Verificar se todos os campos obrigatórios estão preenchidos
        all_configured = all(field and field.strip() for field in required_fields)
        
        # Verificar se não são valores de exemplo
        no_examples = all(
            field and 'EXAMPLE' not in field.upper() and 'YOUR_' not in field.upper()
            for field in required_fields if field
        )
        
        return all_configured and no_examples
    
    def get_display_info(self) -> dict:
        """
        Retorna informações para exibição (sem expor credenciais)
        
        Returns:
            dict: Informações de display
        """
        access_key_preview = "***"
        if self.aws_access_key and len(self.aws_access_key) > 8:
            access_key_preview = f"{self.aws_access_key[:4]}...{self.aws_access_key[-4:]}"
        
        return {
            'bucket_name': self.bucket_name or 'Não configurado',
            'region_name': self.region_name or 'Não configurado',
            'configured': self.is_configured(),
            'access_key_preview': access_key_preview
        }
    
    def validate_credentials(self) -> tuple[bool, str]:
        """
        Valida as credenciais AWS
        
        Returns:
            tuple: (is_valid, message)
        """
        try:
            import boto3
            from botocore.exceptions import ClientError, NoCredentialsError
            
            if not self.is_configured():
                return False, "Credenciais não configuradas"
            
            # Testar conexão com S3
            s3_client = boto3.client(
                's3',
                aws_access_key_id=self.aws_access_key,
                aws_secret_access_key=self.aws_secret_key,
                region_name=self.region_name
            )
            
            # Verificar se bucket existe
            s3_client.head_bucket(Bucket=self.bucket_name)
            
            return True, "Credenciais válidas"
            
        except NoCredentialsError:
            return False, "Credenciais AWS não encontradas"
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                return False, f"Bucket '{self.bucket_name}' não encontrado"
            elif error_code == '403':
                return False, "Acesso negado. Verifique as permissões"
            else:
                return False, f"Erro AWS: {e.response['Error']['Message']}"
        except Exception as e:
            return False, f"Erro na validação: {str(e)}"

# Instância global da configuração
s3_config = S3Config() 