"""
Página principal de login - Data Platform
"""

import streamlit as st
from app.ui.login_page import render_login_page

# Configuração da página principal
st.set_page_config(
    page_title="Login - Data Platform",
    page_icon="🔐",
    layout="wide",
    initial_sidebar_state="collapsed"
)

if __name__ == "__main__":
    render_login_page() 