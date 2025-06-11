import streamlit as st
from utils._schema_loader import load_schema
from utils._previewer import preview_file, get_excel_sheet_names
from utils._validator import validate_data
from utils._uploader import upload_to_s3
from utils._s3_history import display_upload_history, refresh_upload_history
from utils._schema_manager import get_schemas_by_category, format_schema_option

st.set_page_config(page_title="Upload de Arquivos - Data Platform", layout="wide")

menu = st.sidebar.selectbox("📂 Menu", ["Upload de Arquivo"])

if menu == "Upload de Arquivo":
    st.title("📁 Upload de Arquivos")

    schema = load_schema("schema/user.yaml")

    # Dropdowns para seleção de assunto e sub-assunto
    st.subheader("🗂️ Categorização dos Dados")
    col1, col2 = st.columns(2)
    
    with col1:
        assunto = st.selectbox(
            "Assunto",
            ["Usuários", "Locais"],
            help="Selecione o assunto principal dos dados"
        )
    
    with col2:
        # Sub-assuntos baseados no assunto selecionado
        sub_assuntos_map = {
            "Usuários": ["Cadastro"],
            "Locais": ["Aeroportos"]
        }
        
        sub_assunto = st.selectbox(
            "Sub-Assunto",
            sub_assuntos_map.get(assunto, ["Geral"]),
            help="Selecione o sub-assunto específico"
        )

    uploaded_file = st.file_uploader("Selecione um arquivo CSV ou Excel", type=["csv", "xlsx"])

    if uploaded_file:
        file_extension = uploaded_file.name.lower().split('.')[-1]
        
        st.subheader("⚙️ Configurações de Leitura")
        with st.expander("Mostrar/ocultar configurações", expanded=True):
            
            if file_extension in ['xlsx', 'xls']:
                # Excel file configurations
                st.info(f"📊 Arquivo Excel detectado: **{uploaded_file.name}**")
                
                # Get sheet names
                sheet_names = get_excel_sheet_names(uploaded_file)
                
                col1, col2 = st.columns(2)
                with col1:
                    if sheet_names:
                        sheet_name = st.selectbox("Selecione a aba", sheet_names)
                    else:
                        sheet_name = None
                        st.warning("Não foi possível detectar as abas da planilha.")
                
                with col2:
                    header_row = st.number_input("Linha do cabeçalho (0-indexada)", min_value=0, value=0)
                
                sep = ","  # Not used for Excel files
                
            else:
                # CSV file configurations
                st.info(f"📄 Arquivo CSV detectado: **{uploaded_file.name}**")
                
                col1, col2 = st.columns(2)
                with col1:
                    sep = st.text_input("Separador de colunas", value=",")
                with col2:
                    header_row = st.number_input("Linha do cabeçalho (0-indexada)", min_value=0, value=0)
                
                sheet_name = None  # Not used for CSV files

        try:
            df_preview, file_info = preview_file(uploaded_file, sep, header_row, sheet_name)

            st.subheader("👁️ Pré-visualização dos dados")
            col_df, col_info = st.columns([3, 1])
            with col_df:
                with st.expander("📋 Dados (amostra de 10 linhas)", expanded=True):
                    st.dataframe(df_preview, use_container_width=True)

            with col_info:
                with st.expander("ℹ️ Informações Técnicas", expanded=True):
                    st.write(f"**Tipo de Arquivo:** `{file_info['tipo_arquivo']}`")
                    
                    if file_extension in ['xlsx', 'xls'] and sheet_name:
                        st.write(f"**Aba Selecionada:** `{sheet_name}`")
                    
                    st.write("**Colunas Originais**")
                    st.write(file_info["colunas_detectadas"])
                    
                    st.write("**Colunas Normalizadas**")
                    st.write(file_info["colunas_normalizadas"])
                    
                    st.write("**Tipos por Coluna**")
                    st.json(file_info["colunas_com_tipos"], expanded=False)

                    st.write(f"**Total Estimado de Linhas:** `{file_info['total_linhas_estimado']}`")
                
                with st.expander("🔄 Mapeamento de Colunas", expanded=False):
                    st.write("**Transformações aplicadas:**")
                    for original, normalizada in file_info["mapeamento_colunas"].items():
                        st.write(f"`{original}` → `{normalizada}`")

            # 🛡️ Validação
            st.subheader("🛡️ Validação dos Dados")
            
            # Initialize session state for schema selection
            if 'selected_schema' not in st.session_state:
                st.session_state.selected_schema = schema
            if 'schema_validation_failed' not in st.session_state:
                st.session_state.schema_validation_failed = False
            
            # Validate with current schema
            current_schema = st.session_state.selected_schema
            errors = validate_data(df_preview, current_schema)
            
            if errors:
                st.error("Foram encontrados erros no arquivo:")
                for err in errors:
                    st.write(f"- {err}")
                
                # Show schema selection if validation failed
                st.warning("⚠️ O schema padrão não é compatível com seus dados.")
                
                # Get available schemas for this category
                available_schemas = get_schemas_by_category(assunto, sub_assunto)
                
                if available_schemas:
                    st.subheader("🔄 Seleção de Schema Alternativo")
                    st.info(f"Schemas sugeridos para **{assunto} > {sub_assunto}**:")
                    
                    # Create schema options for dropdown
                    schema_options = {}
                    schema_display_names = []
                    
                    for schema_info in available_schemas:
                        display_name = format_schema_option(schema_info)
                        schema_options[display_name] = schema_info
                        schema_display_names.append(display_name)
                    
                    # Schema selection dropdown
                    selected_schema_display = st.selectbox(
                        "Escolha um schema compatível:",
                        schema_display_names,
                        help="Selecione o schema que melhor se adequa aos seus dados"
                    )
                    
                    if st.button("🔍 Testar Schema Selecionado"):
                        selected_schema_info = schema_options[selected_schema_display]
                        try:
                            new_schema = load_schema(selected_schema_info['file'])
                            new_errors = validate_data(df_preview, new_schema)
                            
                            if new_errors:
                                st.error("❌ Schema selecionado também apresenta erros:")
                                for err in new_errors:
                                    st.write(f"- {err}")
                            else:
                                st.success("✅ Schema selecionado é compatível!")
                                st.session_state.selected_schema = new_schema
                                st.session_state.schema_validation_failed = False
                                st.rerun()
                                
                        except Exception as e:
                            st.error(f"Erro ao carregar schema: {e}")
                else:
                    st.info("Nenhum schema alternativo disponível para esta categoria.")
                    
            else:
                st.success("✅ Arquivo válido de acordo com o schema")
                
                # Show which schema is being used
                schema_name = current_schema.get('schema', {}).get('table_name', 'schema padrão')
                st.info(f"📋 Usando schema: **{schema_name}**")
                
                # Mostrar o caminho onde o arquivo será salvo
                from datetime import datetime
                import os
                upload_date = datetime.now().strftime("%Y-%m-%d")
                from utils._normalizer import normalize_column_name
                assunto_norm = normalize_column_name(assunto)
                sub_assunto_norm = normalize_column_name(sub_assunto)
                
                # Always show as .csv extension
                base_filename = os.path.splitext(uploaded_file.name)[0]
                csv_filename = f"{base_filename}.csv"
                st.info(f"**Caminho no S3:** `{assunto_norm}/{sub_assunto_norm}/{upload_date}/{csv_filename}`")
                st.success("ℹ️ O arquivo será convertido para formato CSV antes do upload.")
                
                if st.button("📤 Enviar para o S3"):
                    with st.spinner('Processando e enviando arquivo...'):
                        # Get the complete dataframe for upload
                        df_complete, _ = preview_file(uploaded_file, sep, header_row, sheet_name, full_data=True)
                        
                        # Upload the processed dataframe as CSV
                        upload_to_s3(df_complete, uploaded_file.name, assunto, sub_assunto)
                        
                        # Refresh upload history to show the new file
                        refresh_upload_history()
                        
                    st.success("✅ Upload realizado com sucesso! Arquivo convertido para CSV.")
                    st.info("📊 Histórico de uploads atualizado abaixo.")
                    
                    # Reset schema selection for next upload
                    st.session_state.selected_schema = schema
                    st.session_state.schema_validation_failed = False

        except Exception as e:
            st.error(f"Erro ao processar o arquivo: {e}")


st.markdown("---")
display_upload_history()