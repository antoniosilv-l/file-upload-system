"""
Serviço de autenticação com AWS Cognito e fallback local
"""

import boto3
import hashlib
import hmac
import base64
import os
from typing import Optional, Dict, Tuple, Any
from dataclasses import dataclass
from enum import Enum
import streamlit as st


class AuthStatus(Enum):
    SUCCESS = "success"
    INVALID_CREDENTIALS = "invalid_credentials"
    USER_NOT_FOUND = "user_not_found"
    ERROR = "error"
    COGNITO_UNAVAILABLE = "cognito_unavailable"


@dataclass
class AuthUser:
    """Informações do usuário autenticado"""
    username: str
    email: Optional[str] = None
    full_name: Optional[str] = None
    groups: list = None
    is_admin: bool = False
    auth_method: str = "local"  # "cognito" ou "local"
    
    def __post_init__(self):
        if self.groups is None:
            self.groups = []


@dataclass
class AuthResult:
    """Resultado da autenticação"""
    status: AuthStatus
    user: Optional[AuthUser] = None
    message: str = ""
    access_token: Optional[str] = None


class AuthenticationService:
    """Serviço de autenticação com Cognito e fallback local"""
    
    def __init__(self):
        # Configurações do Cognito
        self.region = os.getenv('AWS_REGION', 'us-east-1')
        self.user_pool_id = os.getenv('COGNITO_USER_POOL_ID')
        self.client_id = os.getenv('COGNITO_CLIENT_ID')
        self.client_secret = os.getenv('COGNITO_CLIENT_SECRET')
        
        # Usuário padrão local
        self.default_username = os.getenv('DEFAULT_ADMIN_USER', 'admin')
        self.default_password = os.getenv('DEFAULT_ADMIN_PASSWORD', 'admin')
        
        # Cliente Cognito
        self.cognito_client = None
        self._initialize_cognito()
    
    def _initialize_cognito(self):
        """Inicializa cliente Cognito se configurado"""
        try:
            if self.user_pool_id and self.client_id:
                self.cognito_client = boto3.client(
                    'cognito-idp',
                    region_name=self.region,
                    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
                )
        except Exception as e:
            print(f"Erro ao inicializar Cognito: {str(e)}")
            self.cognito_client = None
    
    def is_cognito_available(self) -> bool:
        """Verifica se Cognito está disponível"""
        return self.cognito_client is not None and self.user_pool_id and self.client_id
    
    def _calculate_secret_hash(self, username: str) -> str:
        """Calcula hash secreto para Cognito"""
        if not self.client_secret:
            return ""
        
        message = username + self.client_id
        secret_hash = hmac.new(
            self.client_secret.encode(),
            message.encode(),
            hashlib.sha256
        ).digest()
        return base64.b64encode(secret_hash).decode()
    
    def _authenticate_cognito(self, username: str, password: str) -> AuthResult:
        """Autentica via AWS Cognito"""
        try:
            auth_params = {
                'USERNAME': username,
                'PASSWORD': password
            }
            
            # Adicionar SECRET_HASH se necessário
            if self.client_secret:
                auth_params['SECRET_HASH'] = self._calculate_secret_hash(username)
            
            response = self.cognito_client.admin_initiate_auth(
                UserPoolId=self.user_pool_id,
                ClientId=self.client_id,
                AuthFlow='ADMIN_NO_SRP_AUTH',
                AuthParameters=auth_params
            )
            
            # Extrair token
            access_token = response['AuthenticationResult']['AccessToken']
            
            # Obter informações do usuário
            user_info = self.cognito_client.admin_get_user(
                UserPoolId=self.user_pool_id,
                Username=username
            )
            
            # Extrair atributos do usuário
            user_attributes = {attr['Name']: attr['Value'] for attr in user_info.get('UserAttributes', [])}
            user_groups = [group['GroupName'] for group in user_info.get('Groups', [])]
            
            # Verificar se é admin
            is_admin = 'admin' in [group.lower() for group in user_groups]
            
            user = AuthUser(
                username=username,
                email=user_attributes.get('email'),
                full_name=user_attributes.get('name', username),
                groups=user_groups,
                is_admin=is_admin,
                auth_method="cognito"
            )
            
            return AuthResult(
                status=AuthStatus.SUCCESS,
                user=user,
                message="Login realizado com sucesso via Cognito",
                access_token=access_token
            )
            
        except self.cognito_client.exceptions.NotAuthorizedException:
            return AuthResult(
                status=AuthStatus.INVALID_CREDENTIALS,
                message="Usuário ou senha inválidos"
            )
        except self.cognito_client.exceptions.UserNotFoundException:
            return AuthResult(
                status=AuthStatus.USER_NOT_FOUND,
                message="Usuário não encontrado"
            )
        except Exception as e:
            return AuthResult(
                status=AuthStatus.ERROR,
                message=f"Erro na autenticação Cognito: {str(e)}"
            )
    
    def _authenticate_local(self, username: str, password: str) -> AuthResult:
        """Autentica via usuário local padrão"""
        if username == self.default_username and password == self.default_password:
            user = AuthUser(
                username=username,
                email=f"{username}@local.system",
                full_name="Administrador Local",
                groups=["admin"],
                is_admin=True,
                auth_method="local"
            )
            
            return AuthResult(
                status=AuthStatus.SUCCESS,
                user=user,
                message="Login realizado com sucesso (usuário local)",
                access_token="local_admin_token"
            )
        else:
            return AuthResult(
                status=AuthStatus.INVALID_CREDENTIALS,
                message="Credenciais locais inválidas"
            )
    
    def authenticate(self, username: str, password: str) -> AuthResult:
        """Autentica usuário (Cognito primeiro, depois local)"""
        if not username or not password:
            return AuthResult(
                status=AuthStatus.INVALID_CREDENTIALS,
                message="Usuário e senha são obrigatórios"
            )
        
        # Tentar Cognito primeiro se disponível
        if self.is_cognito_available():
            result = self._authenticate_cognito(username, password)
            
            # Se Cognito falhar por credenciais, tentar local também
            if result.status == AuthStatus.INVALID_CREDENTIALS:
                local_result = self._authenticate_local(username, password)
                if local_result.status == AuthStatus.SUCCESS:
                    return local_result
            
            return result
        else:
            # Usar apenas autenticação local
            return self._authenticate_local(username, password)
    
    def logout(self, access_token: str = None) -> bool:
        """Faz logout do usuário"""
        try:
            if access_token and access_token != "local_admin_token" and self.cognito_client:
                self.cognito_client.global_sign_out(AccessToken=access_token)
            return True
        except Exception as e:
            print(f"Erro no logout: {str(e)}")
            return False
    
    def get_user_info(self, access_token: str) -> Optional[AuthUser]:
        """Obtém informações do usuário logado"""
        try:
            if access_token == "local_admin_token":
                return AuthUser(
                    username=self.default_username,
                    email=f"{self.default_username}@local.system",
                    full_name="Administrador Local",
                    groups=["admin"],
                    is_admin=True,
                    auth_method="local"
                )
            
            if self.cognito_client and access_token:
                response = self.cognito_client.get_user(AccessToken=access_token)
                
                user_attributes = {attr['Name']: attr['Value'] for attr in response.get('UserAttributes', [])}
                
                return AuthUser(
                    username=response['Username'],
                    email=user_attributes.get('email'),
                    full_name=user_attributes.get('name', response['Username']),
                    groups=[],  # Seria necessário fazer chamada adicional para grupos
                    is_admin=False,  # Seria necessário verificar grupos
                    auth_method="cognito"
                )
            
            return None
            
        except Exception as e:
            print(f"Erro ao obter informações do usuário: {str(e)}")
            return None
    
    def is_user_admin(self, user: AuthUser) -> bool:
        """Verifica se usuário é admin"""
        return user.is_admin if user else False
    
    def get_background_image_path(self, user: AuthUser) -> str:
        """Retorna caminho da imagem de fundo baseado no usuário"""
        if self.is_user_admin(user):
            # Admin pode ter imagem personalizada
            custom_path = os.getenv('ADMIN_BACKGROUND_IMAGE', 'assets/admin_background.jpg')
            if os.path.exists(custom_path):
                return custom_path
        
        # Imagem padrão
        return 'assets/default_background.jpg'


# Funções utilitárias para Streamlit
def init_session_state():
    """Inicializa estado da sessão"""
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False
    if 'user' not in st.session_state:
        st.session_state.user = None
    if 'access_token' not in st.session_state:
        st.session_state.access_token = None


def get_current_user() -> Optional[AuthUser]:
    """Retorna usuário atual da sessão"""
    return st.session_state.get('user')


def is_authenticated() -> bool:
    """Verifica se usuário está autenticado"""
    return st.session_state.get('authenticated', False)


def require_auth():
    """Decorator/função para exigir autenticação"""
    if not is_authenticated():
        st.switch_page("pages/login.py")
        st.stop()


def login_user(user: AuthUser, access_token: str):
    """Faz login do usuário na sessão"""
    st.session_state.authenticated = True
    st.session_state.user = user
    st.session_state.access_token = access_token


def logout_user():
    """Faz logout do usuário da sessão"""
    st.session_state.authenticated = False
    st.session_state.user = None
    st.session_state.access_token = None 