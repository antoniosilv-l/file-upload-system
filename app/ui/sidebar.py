import streamlit as st


class SidebarComponent:
    """Componente reutilizável para sidebar"""
    
    def __init__(self):
        pass
    
    def render(self):
        """Renderiza a sidebar completa"""
        with st.sidebar:
            return self._render_navigation_buttons()
    
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