"""
Página de login com design moderno baseado na imagem fornecida
"""

import streamlit as st
from app.services.auth_service import AuthenticationService, init_session_state, login_user
import os
import base64


def render_login_page():
    """Renderiza página de login com design moderno"""
    # Inicializar serviços
    init_session_state()
    auth_service = AuthenticationService()
    
    # Verificar se já está logado
    if st.session_state.get('authenticated', False):
        st.switch_page("pages/main_app.py")
        st.stop()
    
    # Ler e converter imagem para base64
    try:
        with open("assets/background.jpg", "rb") as img_file:
            img_base64 = base64.b64encode(img_file.read()).decode()
    except:
        img_base64 = ""
    
    # Ler e converter logo para base64
    try:
        with open("assets/logo.png", "rb") as logo_file:
            logo_base64 = base64.b64encode(logo_file.read()).decode()
    except:
        logo_base64 = ""
    
    # CSS com layout 75% imagem + 25% card preto
    st.markdown(f"""
    <style>
    /* Importar fontes */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    
    /* Esconder elementos do Streamlit */
    #MainMenu {{visibility: hidden;}}
    footer {{visibility: hidden;}}
    header {{visibility: hidden;}}
    .stApp > header {{visibility: hidden;}}
    
    /* Background da página com imagem */
    .stApp {{
        background: {'url(data:image/jpg;base64,' + img_base64 + ')' if img_base64 else 'linear-gradient(135deg, #1e3c72 0%, #2a5298 50%, #667eea 100%)'};
        background-size: cover;
        background-position: center;
        background-attachment: fixed;
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    }}
    
    /* Overlay escuro sobre a imagem */
    .stApp::before {{
        content: '';
        position: fixed;
        top: 0;
        left: 0;
        right: 0;
        bottom: 0;
        background: rgba(0, 0, 0, 0.4);
        z-index: -1;
    }}
    
    /* LAYOUT COM CARD À DIREITA CENTRALIZADO */
    .main {{
        position: fixed !important;
        top: 0 !important;
        left: 0 !important;
        right: 0 !important;
        bottom: 0 !important;
        display: flex !important;
        justify-content: flex-end !important;
        padding: 3rem !important;
        z-index: 1000 !important;
        background: none !important;
    }}
    
    .block-container {{
        background: #0e1117 !important;
        border-radius: 20px !important;
        box-shadow: 0 25px 50px rgba(0, 0, 0, 0.5) !important;
        padding: 3rem 2.5rem !important;
        width: 420px !important;
        max-width: 420px !important;
        animation: slideInFromRight 0.6s ease-out !important;
        border: 1px solid rgba(255, 255, 255, 0.1) !important;
        flex-shrink: 0 !important;
        margin-top: auto !important;
        margin-bottom: auto !important;
        margin-left: auto !important;
        margin-right: 100px !important;
    }}
    
    @keyframes slideInFromRight {{
        from {{
            opacity: 0;
            transform: translateX(100px);
        }}
        to {{
            opacity: 1;
            transform: translateX(0);
        }}
    }}
    
    /* Brand header */
    .brand-header {{
        display: flex;
        align-items: center;
        margin-bottom: 2rem;
        color: #4f46e5;
        font-weight: 600;
        font-size: 18px;
    }}
    
    .brand-logo {{
        width: 100px;
        height: 100px;
        margin-right: 12px;
        object-fit: contain;
    }}
    
    .brand-icon {{
        width: 24px;
        height: 24px;
        background: #4f46e5;
        border-radius: 50%;
        margin-right: 8px;
        position: relative;
    }}
    
    .brand-icon::before {{
        content: '';
        position: absolute;
        top: 50%;
        left: 50%;
        transform: translate(-50%, -50%);
        width: 8px;
        height: 8px;
        background: white;
        border-radius: 50%;
    }}
    
    /* Títulos com cores para fundo preto */
    .custom-title {{
        font-size: 28px;
        font-weight: 700;
        color: #ffffff;
        margin-bottom: 0.5rem;
        line-height: 1.2;
        font-family: 'Inter', sans-serif;
    }}
    
    .custom-subtitle {{
        color: #a0a0a0;
        font-size: 16px;
        margin-bottom: 2rem;
        line-height: 1.5;
        font-family: 'Inter', sans-serif;
    }}
    
    /* Inputs do formulário para fundo preto */
    .stTextInput > div > div > input {{
        border: 2px solid #2d3748 !important;
        border-radius: 12px !important;
        padding: 14px 16px !important;
        font-size: 16px !important;
        background: #1a202c !important;
        transition: all 0.3s ease !important;
        font-family: 'Inter', sans-serif !important;
        color: #ffffff !important;
        width: 100% !important;
    }}
    
    .stTextInput > div > div > input:focus {{
        border-color: #4f46e5 !important;
        background: #2d3748 !important;
        box-shadow: 0 0 0 3px rgba(79, 70, 229, 0.3) !important;
        outline: none !important;
    }}
    
    .stTextInput > div > div > input::placeholder {{
        color: #718096 !important;
    }}
    
    .stTextInput > label {{
        font-weight: 500 !important;
        color: #e2e8f0 !important;
        margin-bottom: 6px !important;
        font-size: 14px !important;
        font-family: 'Inter', sans-serif !important;
    }}
    
    /* Botão do formulário */
    .stButton > button {{
        background: linear-gradient(135deg, #4f46e5 0%, #7c3aed 100%) !important;
        color: white !important;
        border: none !important;
        border-radius: 12px !important;
        padding: 14px 24px !important;
        font-size: 16px !important;
        font-weight: 600 !important;
        width: 100% !important;
        transition: all 0.3s ease !important;
        font-family: 'Inter', sans-serif !important;
        margin-top: 1rem !important;
    }}
    
    .stButton > button:hover {{
        transform: translateY(-2px) !important;
        box-shadow: 0 10px 30px rgba(79, 70, 229, 0.4) !important;
    }}
    
    /* Checkbox do formulário para fundo preto */
    .stCheckbox {{
        margin: 1rem 0 !important;
    }}
    
    .stCheckbox > label {{
        font-size: 14px !important;
        color: #a0a0a0 !important;
        font-family: 'Inter', sans-serif !important;
    }}
    
    .stCheckbox input[type="checkbox"] {{
        accent-color: #4f46e5 !important;
    }}
    
    /* Alertas e mensagens para fundo preto */
    .stAlert {{
        margin: 1rem 0 !important;
        border-radius: 8px !important;
        background: #2d3748 !important;
        border: 1px solid #4a5568 !important;
        color: #e2e8f0 !important;
    }}
    
    .stSuccess {{
        background-color: #1a2e1a !important;
        border: 1px solid #2f855a !important;
        color: #68d391 !important;
    }}
    
    .stError {{
        background-color: #2d1b1b !important;
        border: 1px solid #e53e3e !important;
        color: #feb2b2 !important;
    }}
    
    .stWarning {{
        background-color: #2d2618 !important;
        border: 1px solid #d69e2e !important;
        color: #faf089 !important;
    }}
    
    /* Spinner para fundo preto */
    .stSpinner > div {{
        border-color: #4f46e5 !important;
    }}
    
    /* Links e status para fundo preto */
    .signup-link {{
        text-align: center;
        margin-top: 1.5rem;
        font-size: 14px;
        color: #a0a0a0;
        font-family: 'Inter', sans-serif;
    }}
    
    .signup-link a {{
        color: #4f46e5;
        text-decoration: none;
        font-weight: 500;
    }}
    
    .auth-status {{
        text-align: center;
        margin-top: 1rem;
        padding: 8px 12px;
        border-radius: 8px;
        font-size: 12px;
        font-family: 'Inter', sans-serif;
    }}
    
    .auth-status.local {{
        background: #2d2618;
        color: #faf089;
        border: 1px solid #d69e2e;
    }}
    
    /* Copyright */
    .copyright {{
        position: fixed;
        bottom: 2rem;
        left: 2rem;
        color: rgba(255, 255, 255, 0.8);
        font-size: 12px;
        text-shadow: 0 1px 2px rgba(0,0,0,0.7);
        z-index: 999;
        font-family: 'Inter', sans-serif;
    }}
    
    /* Responsivo */
    @media (max-width: 1024px) {{
        .main {{
            padding: 2.5rem !important;
        }}
        
        .block-container {{
            max-width: 380px !important;
            min-width: 320px !important;
        }}
    }}
    
    @media (max-width: 768px) {{
        .main {{
            align-items: center !important;
            justify-content: center !important;
            padding: 2rem !important;
        }}
        
        .block-container {{
            max-width: 400px !important;
            min-width: auto !important;
        }}
        
        .custom-title {{
            font-size: 24px;
        }}
    }}
    
    @media (max-width: 480px) {{
        .main {{
            padding: 1rem !important;
        }}
        
        .block-container {{
            padding: 2rem 1.5rem !important;
            min-width: auto !important;
        }}
        
        .custom-title {{
            font-size: 22px;
        }}
    }}
    </style>
    """, unsafe_allow_html=True)
    
    # Header do card (primeiro elemento)
    st.markdown(f"""
    <div class="brand-header">
        {'<img src="data:image/png;base64,' + logo_base64 + '" class="brand-logo" alt="Logo">' if logo_base64 else '<div class="brand-icon"></div>'}
        DATA PLATFORM
    </div>
    
    <h1 class="custom-title">Login into<br>your account</h1>
    <p class="custom-subtitle">Let us make the data bigger!</p>
    """, unsafe_allow_html=True)
    
    # Formulário Streamlit (agora dentro do card automaticamente)
    with st.form("login_form", clear_on_submit=False):
        email = st.text_input("Email", placeholder="Digite seu email")
        password = st.text_input("Password", type="password", placeholder="Digite sua senha")
        remember = st.checkbox("Remember me")
        login_button = st.form_submit_button("Login")
        
        # Processar login
        if login_button:
            if email and password:
                with st.spinner("🔍 Verificando credenciais..."):
                    result = auth_service.authenticate(email, password)
                
                if result.status.value == "success":
                    login_user(result.user, result.access_token)
                    st.success("✅ Login realizado com sucesso!")
                    st.balloons()
                    st.rerun()
                else:
                    st.error(f"❌ {result.message}")
            else:
                st.warning("⚠️ Preencha email e senha")
    
    # Links e status (agora dentro do card automaticamente)
    st.markdown("""
    <div class="signup-link">
        Don't have an account? <a href="#" onclick="alert('Funcionalidade em desenvolvimento')">Sign up</a>
    </div>
    """, unsafe_allow_html=True)
    
    # Status de autenticação
    if not auth_service.is_cognito_available():
        st.markdown("""
        <div class="auth-status local">
            ⚠️ Modo Local - Use: admin/admin
        </div>
        """, unsafe_allow_html=True)
        st.info("💡 **Modo Local Ativo** - Use: admin/admin")
    
    # Copyright fora do card
    st.markdown("""
    <div class="copyright">
        © 2025 DATA PLATFORM. All rights reserved.
    </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    render_login_page() 