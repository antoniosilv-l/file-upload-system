import streamlit as st
from utils._schema_loader import load_schema
from utils._previewer import preview_file
from utils._validator import validate_data
from utils._uploader import upload_to_s3

st.set_page_config(page_title="Upload de Arquivos - Data Platform", layout="wide")

menu = st.sidebar.selectbox("📂 Menu", ["Upload de Arquivo"])

if menu == "Upload de Arquivo":
    st.title("📁 Upload de Arquivos")

    schema = load_schema("schema/user.yaml")

    uploaded_file = st.file_uploader("Selecione um arquivo CSV ou Excel", type=["csv", "xlsx"])

    if uploaded_file:
        st.subheader("⚙️ Configurações de Leitura")
        with st.expander("Mostrar/ocultar configurações", expanded=True):
            col1, col2 = st.columns(2)
            with col1:
                sep = st.text_input("Separador de colunas", value=",")
            with col2:
                header_row = st.number_input("Linha do cabeçalho (0-indexada)", min_value=0, value=0)

        try:
            df_preview, file_info = preview_file(uploaded_file, sep, header_row)

            st.subheader("👁️ Pré-visualização dos dados")
            col_df, col_info = st.columns([3, 1])
            with col_df:
                with st.expander("📋 Dados (amostra de 10 linhas)", expanded=True):
                    st.dataframe(df_preview, use_container_width=True)

            with col_info:
                with st.expander("ℹ️ Informações Técnicas", expanded=True):
                    st.write("**Colunas Detectadas**")
                    st.write(file_info["colunas_detectadas"])
                    
                    st.write("**Tipos por Coluna**")
                    st.json(file_info["colunas_com_tipos"], expanded=False)

                    st.write(f"**Total Estimado de Linhas:** `{file_info['total_linhas_estimado']}`")

            # 🛡️ Validação
            st.subheader("🛡️ Validação dos Dados")
            errors = validate_data(df_preview, schema)
            if errors:
                st.error("Foram encontrados erros no arquivo:")
                for err in errors:
                    st.write(f"- {err}")
            else:
                st.success("Arquivo válido de acordo com o schema")
                if st.button("📤 Enviar para o S3"):
                    upload_to_s3(uploaded_file)
                    st.success("Upload realizado com sucesso!")

        except Exception as e:
            st.error(f"Erro ao processar o arquivo: {e}")