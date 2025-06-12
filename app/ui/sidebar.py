import streamlit as st
from app.services.auth_service import get_current_user, logout_user, AuthenticationService


class SidebarComponent:
    """Componente reutilizável para sidebar"""
    
    def __init__(self):
        self.auth_service = AuthenticationService()
    
    def render(self):
        """Renderiza a sidebar completa"""
        with st.sidebar:
            self._render_user_info()
            st.divider()
            return self._render_navigation_buttons()
    
    def _render_user_info(self):
        """Renderiza informações do usuário"""
        current_user = get_current_user()
        
        if current_user:
            # Container com informações do usuário
            st.markdown("""
            <style>
            .user-info {
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                padding: 1rem;
                border-radius: 10px;
                margin-bottom: 1rem;
                text-align: center;
            }
            .user-name {
                font-size: 1.1rem;
                font-weight: 600;
                margin-bottom: 0.3rem;
            }
            .user-details {
                font-size: 0.85rem;
                opacity: 0.9;
            }
            .admin-badge {
                background: rgba(255, 255, 255, 0.2);
                padding: 0.2rem 0.5rem;
                border-radius: 20px;
                font-size: 0.75rem;
                margin-top: 0.5rem;
                display: inline-block;
            }
            </style>
            """, unsafe_allow_html=True)
            
            # Ícone baseado no método de autenticação
            auth_icon = "🔐" if current_user.auth_method == "cognito" else "👤"
            admin_badge = "👑 ADMIN" if current_user.is_admin else "👤 USER"
            
            st.markdown(f"""
            <div class="user-info">
                <div class="user-name">{auth_icon} {current_user.full_name}</div>
                <div class="user-details">@{current_user.username}</div>
                <div class="user-details">{current_user.email}</div>
                <div class="admin-badge">{admin_badge}</div>
            </div>
            """, unsafe_allow_html=True)
            
            # Botão de logout
            if st.button("🚪 Logout", use_container_width=True, type="secondary"):
                # Fazer logout no serviço
                access_token = st.session_state.get('access_token')
                self.auth_service.logout(access_token)
                
                # Limpar sessão
                logout_user()
                
                # Redirecionar para login
                st.switch_page("login.py")
        else:
            st.error("❌ Usuário não autenticado")
    
    def _render_navigation_buttons(self):
        """Renderiza botões de navegação"""
        st.header("📂 Navegação")
        
        # Inicializar página atual se não existir
        if 'current_page' not in st.session_state:
            st.session_state.current_page = "Upload de Arquivo"
        
        # Botão 1: Upload de Arquivo
        if st.button("📁 Upload de Arquivo", 
                    use_container_width=True,
                    type="primary" if st.session_state.current_page == "Upload de Arquivo" else "secondary"):
            st.session_state.current_page = "Upload de Arquivo"
            st.rerun()
        
        # Botão 2: Athena & Tabelas
        if st.button("🗃️ Athena & Tabelas", 
                    use_container_width=True,
                    type="primary" if st.session_state.current_page == "Athena & Tabelas" else "secondary"):
            st.session_state.current_page = "Athena & Tabelas"
            st.rerun()
        
        return st.session_state.current_page


def render_sidebar():
    """Função de conveniência para renderizar sidebar"""
    sidebar = SidebarComponent()
    return sidebar.render() 