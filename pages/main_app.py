"""
Página principal da aplicação - Upload de Arquivos
"""

import streamlit as st
import sys
import os

# Configuração de paths
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) if '__file__' in globals() else os.getcwd())
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + '/app' if '__file__' in globals() else 'app')

# Imports dos serviços
from app.services.validation_service import ValidationService
from app.services.file_service import FileProcessingService
from app.services.uploader_service import S3UploaderService
from app.services.auth_service import init_session_state, is_authenticated, get_current_user, require_auth
from app.ui.sidebar import render_sidebar
from app.ui.athena_page import render_athena_page
from app.config.s3_config import s3_config

# Configuração da página
st.set_page_config(page_title="Upload de Arquivos - Data Platform", layout="wide")

# Inicializar estado da sessão
init_session_state()

# Verificar autenticação
if not is_authenticated():
    st.markdown("""
    <div style="text-align: center; padding: 2rem;">
        <h1>🔐 Acesso Restrito</h1>
        <p>Você precisa fazer login para acessar o sistema.</p>
    </div>
    """, unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        if st.button("🚀 Ir para Login", use_container_width=True, type="primary"):
            st.switch_page("login.py")
    st.stop()

# Obter usuário atual
current_user = get_current_user()
user_name = current_user.full_name if current_user else "Usuario"

# Renderizar sidebar e capturar página atual
menu = render_sidebar()

if menu == "Upload de Arquivo":
    st.title("📁 Upload de Arquivos")

    # Inicializar serviços
    validation_service = ValidationService()
    file_service = FileProcessingService()
    uploader_service = S3UploaderService()

    # Seção de categorização
    st.subheader("🎯 Categorização dos Dados")
    col_assunto, col_sub_assunto = st.columns(2)
    
    # Obter assuntos e sub-assuntos
    subjects_data = validation_service.get_subjects_and_subsubjects()
    
    with col_assunto:
        if subjects_data:
            selected_subject = st.selectbox(
                "Selecione o Assunto:",
                options=list(subjects_data.keys()),
                help="Categoriza o tipo de dados que você está enviando"
            )
        else:
            st.error("Nenhum schema encontrado!")
            selected_subject = None
    
    with col_sub_assunto:
        if selected_subject and selected_subject in subjects_data:
            sub_subjects = subjects_data[selected_subject]
            selected_sub_subject = st.selectbox(
                "Selecione o Sub-assunto:",
                options=sub_subjects,
                help="Subcategoria específica dos dados"
            )
        else:
            selected_sub_subject = None
            st.selectbox("Selecione o Sub-assunto:", options=[], disabled=True)

    # Carregar schema
    schema = None
    if selected_subject and selected_sub_subject:
        schema = validation_service.load_schema_by_subject_subsubject(selected_subject, selected_sub_subject)
        
        if schema:
            st.success(f"✅ Schema carregado: **{schema.get('description', 'N/A')}**")
        else:
            st.error("❌ Não foi possível carregar o schema selecionado")

    st.divider()

    # Upload de arquivo
    uploaded_file = st.file_uploader(
        "Selecione um arquivo CSV ou Excel", 
        type=["csv", "xlsx", "xls"],
        help="Formatos suportados: CSV, Excel (.xlsx, .xls)"
    )

    if uploaded_file:
        file_extension = uploaded_file.name.split('.')[-1].lower()
        
        # Configurações de leitura
        st.subheader("⚙️ Configurações de Leitura")
        
        if file_extension == 'csv':
            with st.expander("📄 Configurações CSV", expanded=True):
                col1, col2 = st.columns(2)
                with col1:
                    sep_options = {
                        "Vírgula (,)": ",",
                        "Ponto e vírgula (;)": ";", 
                        "Tab": "\t",
                        "Pipe (|)": "|",
                        "Detectar automaticamente": "auto"
                    }
                    sep_choice = st.selectbox("Separador de colunas:", options=list(sep_options.keys()))
                    sep = sep_options[sep_choice]
                    
                with col2:
                    header_row = st.number_input("Linha do cabeçalho (0-indexada):", min_value=0, value=0)
                    
        elif file_extension in ['xlsx', 'xls']:
            with st.expander("📊 Configurações Excel", expanded=True):
                excel_sheets = file_service.get_excel_sheets(uploaded_file)
                
                col1, col2 = st.columns(2)
                with col1:
                    if excel_sheets:
                        sheet_name = st.selectbox(
                            "Selecione a aba:",
                            options=excel_sheets,
                            help=f"Planilha contém {len(excel_sheets)} aba(s)"
                        )
                    else:
                        st.error("Não foi possível ler as abas da planilha")
                        sheet_name = None
                        
                with col2:
                    header_row = st.number_input("Linha do cabeçalho (0-indexada):", min_value=0, value=0)
                    
                sep = ","  # Para Excel, sep não é relevante

        try:
            # Preview do arquivo
            if file_extension == 'csv':
                df_preview, file_info = file_service.preview_file(uploaded_file, sep, header_row)
            else:
                df_preview, file_info = file_service.preview_file(uploaded_file, sep, header_row, sheet_name)

            st.subheader("👁️ Pré-visualização dos dados")
            col_df, col_info = st.columns([3, 1])
            
            with col_df:
                with st.expander("📋 Dados (amostra de 10 linhas)", expanded=True):
                    # Converter lista de dicionários para exibição
                    if isinstance(df_preview, list) and df_preview:
                        # Pegar apenas as primeiras 10 linhas
                        preview_sample = df_preview[:10]
                        st.dataframe(preview_sample, use_container_width=True)
                    else:
                        st.info("Nenhum dado para visualizar")

            with col_info:
                with st.expander("ℹ️ Informações do Arquivo", expanded=True):
                    # Informações básicas
                    st.markdown("### 📄 Detalhes")
                    st.write(f"**Tipo de arquivo:** {file_info['formato'].upper()}")
                    
                    if file_info.get('aba_usada'):
                        st.write(f"**Planilha selecionada:** {file_info['aba_usada']}")
                    
                    st.write(f"**Total de colunas:** {file_info['colunas_detectadas']}")
                    st.write(f"**Linhas carregadas:** {file_info['total_linhas_estimado']}")
                    
                    # Tipos de dados amigáveis
                    st.markdown("### 🏷️ Colunas encontradas")
                    from app.models.file_info import ProcessedFileInfo
                    processed_info = ProcessedFileInfo(
                        columns_detected=file_info['colunas_detectadas'],
                        columns_with_types=file_info['colunas_com_tipos'],
                        total_rows_estimated=file_info['total_linhas_estimado'],
                        format=file_info['formato'],
                        sheet_used=file_info.get('aba_usada')
                    )
                    
                    for col, dtype in file_info["colunas_com_tipos"].items():
                        tipo_amigavel = processed_info.get_friendly_type(dtype)
                        st.write(f"**{col}:** {tipo_amigavel}")

            # Preview do destino S3
            if selected_subject and selected_sub_subject and uploader_service.is_available():
                st.subheader("📁 Destino no S3")
                dest_path = uploader_service.get_upload_path_preview(uploaded_file.name, selected_subject, selected_sub_subject)
                st.code(dest_path, language="")
                st.caption("📝 O arquivo será convertido para CSV e organizado por data")

            # Validação dos dados
            st.subheader("🛡️ Validação dos Dados")
            
            if schema:
                errors = validation_service.validate_data(df_preview, schema)
                
                if errors:
                    st.error("❌ Foram encontrados erros no arquivo:")
                    for err in errors:
                        st.write(f"• {err}")
                        
                    # Mostrar campos esperados
                    with st.expander("📋 Campos esperados pelo schema", expanded=False):
                        expected_fields = schema.get('schema', {}).get('fields', [])
                        
                        for field in expected_fields:
                            required_badge = "🔴 Obrigatório" if field.get('required', False) else "⚪ Opcional"
                            st.write(f"**{field['name']}** ({field.get('type', 'str')}) - {required_badge}")
                            if field.get('description'):
                                st.caption(field['description'])
                                
                else:
                    st.success("✅ Arquivo válido de acordo com o schema selecionado!")
                    
                    # Botão de upload
                    if st.button("📤 Enviar para o S3", type="primary", use_container_width=True):
                        if uploader_service.is_available():
                            with st.spinner("Enviando arquivo..."):
                                try:
                                    success, message = uploader_service.upload_file(
                                        uploaded_file, schema, selected_subject, selected_sub_subject
                                    )
                                    
                                    if success:
                                        st.success("🎉 Upload realizado com sucesso!")
                                        # Marcar para atualizar histórico
                                        st.session_state['upload_history_refresh'] = True
                                        st.rerun()
                                    else:
                                        st.error(f"❌ Erro no upload: {message}")
                                except Exception as e:
                                    st.error(f"❌ Erro no upload: {e}")
                        else:
                            st.error("⚠️ S3 não configurado. Verifique as credenciais no sistema")
            else:
                st.warning("⚠️ Por favor, selecione um Assunto e Sub-assunto para validar os dados")

        except Exception as e:
            st.error(f"❌ Erro ao processar o arquivo: {e}")

    # Seção de histórico de uploads
    st.divider()
    st.subheader("📜 Histórico de Uploads")
    
    # Carregar histórico apenas se S3 configurado e na primeira vez da sessão ou após upload
    if uploader_service.is_available():
        # Verificar se deve carregar histórico
        should_load_history = (
            'upload_history_loaded' not in st.session_state or 
            st.session_state.get('upload_history_refresh', False)
        )
        
        if should_load_history:
            with st.spinner("Carregando histórico de uploads..."):
                try:
                    from app.services.s3_history_service import S3HistoryManager
                    
                    credentials = s3_config.get_credentials()
                    history_manager = S3HistoryManager(
                        credentials['aws_access_key_id'],
                        credentials['aws_secret_access_key'],
                        credentials['bucket_name'],
                        credentials['region_name']
                    )
                    
                    history_list = history_manager.get_upload_history(limit=20)
                    
                    if history_list:
                        st.session_state['upload_history_list'] = history_list
                        st.success(f"✅ {len(history_list)} arquivos encontrados no histórico")
                    else:
                        st.session_state['upload_history_list'] = None
                        st.info("📭 Nenhum arquivo encontrado no histórico")
                    
                    # Marcar como carregado
                    st.session_state['upload_history_loaded'] = True
                    st.session_state['upload_history_refresh'] = False
                    
                except Exception as e:
                    st.error(f"❌ Erro ao carregar histórico: {str(e)}")
                    st.session_state['upload_history_list'] = None
        
        # Mostrar histórico se existir
        if st.session_state.get('upload_history_list') is not None:
            history_list = st.session_state['upload_history_list']
            
            # Preparar dados para exibição
            if history_list:
                # Converter para formato que o streamlit pode exibir
                display_data = []
                for item in history_list:
                    display_item = {
                        'Arquivo': item.get('filename', 'N/A'),
                        'Data Upload': item.get('upload_date', 'N/A'),
                        'Tamanho (MB)': item.get('file_size_mb', 0),
                        'Schema': item.get('schema_used', 'N/A'),
                        'Linhas': item.get('row_count', 0),
                        'Colunas': item.get('column_count', 0)
                    }
                    display_data.append(display_item)
                
                # Mostrar dataframe
                st.dataframe(
                    display_data, 
                    use_container_width=True,
                    hide_index=True
                )
            
            # Botão para atualizar histórico
            col1, col2 = st.columns([3, 1])
            with col2:
                if st.button("🔄 Atualizar Histórico", use_container_width=True):
                    st.session_state['upload_history_refresh'] = True
                    st.rerun()
        
    else:
        st.warning("⚠️ Configure o S3 para ver o histórico de uploads")

elif menu == "Athena & Tabelas":
    # Renderizar página do Athena com usuário logado
    render_athena_page(current_user=current_user) 