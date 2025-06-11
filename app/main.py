import streamlit as st
import pandas as pd
import yaml
import os
from utils._schema_manager import SchemaManager
from utils._validator import DataValidator
from utils._previewer import DataPreviewer
from utils._uploader import DataUploader
from utils._s3_history import S3HistoryManager
from utils._normalizer import ColumnNormalizer

def main():
    st.set_page_config(
        page_title="Sistema de Upload de Arquivos",
        page_icon="📁",
        layout="wide"
    )
    
    st.title("📁 Sistema de Upload de Arquivos")
    st.markdown("Upload e validação de arquivos CSV/Excel com schemas configuráveis")
    
    # Sidebar para configurações
    with st.sidebar:
        st.header("⚙️ Configurações")
        
        # Schema selection
        schema_manager = SchemaManager()
        schemas = schema_manager.list_schemas()
        
        if not schemas:
            st.error("Nenhum schema encontrado na pasta 'schema/'")
            return
            
        selected_schema = st.selectbox(
            "Selecione o Schema:",
            options=schemas,
            format_func=lambda x: x.replace('.yaml', '').replace('_', ' ').title()
        )
        
        # S3 Configuration
        st.subheader("🗄️ Configuração S3")
        aws_access_key = st.text_input("AWS Access Key", type="password")
        aws_secret_key = st.text_input("AWS Secret Key", type="password") 
        bucket_name = st.text_input("Bucket Name", value="meu-bucket-dados")
        aws_region = st.text_input("AWS Region", value="us-east-1")

    # Main content area
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.header("📤 Upload de Arquivo")
        
        # File upload
        uploaded_file = st.file_uploader(
            "Escolha um arquivo CSV ou Excel",
            type=['csv', 'xlsx', 'xls'],
            help="Formatos suportados: CSV, Excel (.xlsx, .xls)"
        )
        
        if uploaded_file is not None:
            # Load schema
            schema_data = schema_manager.load_schema(selected_schema)
            if not schema_data:
                st.error(f"Erro ao carregar schema: {selected_schema}")
                return
                
            # Preview file
            previewer = DataPreviewer()
            df = previewer.preview_file(uploaded_file)
            
            if df is not None:
                st.subheader("👀 Preview dos Dados")
                st.dataframe(df.head(10), use_container_width=True)
                
                st.info(f"📊 **Informações do arquivo:**\n"
                       f"- Linhas: {len(df):,}\n"
                       f"- Colunas: {len(df.columns):,}\n"
                       f"- Tamanho: {uploaded_file.size:,} bytes")
                
                # Normalize columns
                normalizer = ColumnNormalizer()
                original_columns = df.columns.tolist()
                df_normalized = normalizer.normalize_dataframe(df)
                
                if original_columns != df_normalized.columns.tolist():
                    st.subheader("🔄 Normalização de Colunas")
                    col_mapping = dict(zip(original_columns, df_normalized.columns.tolist()))
                    
                    mapping_df = pd.DataFrame([
                        {"Coluna Original": orig, "Coluna Normalizada": norm}
                        for orig, norm in col_mapping.items()
                    ])
                    st.dataframe(mapping_df, use_container_width=True)
                
                # Validate data
                validator = DataValidator()
                is_valid, validation_report = validator.validate_data(df_normalized, schema_data)
                
                st.subheader("✅ Validação dos Dados")
                
                if is_valid:
                    st.success("✅ Dados válidos! Pronto para upload.")
                    
                    # Upload section
                    if st.button("🚀 Fazer Upload para S3", type="primary"):
                        if all([aws_access_key, aws_secret_key, bucket_name]):
                            uploader = DataUploader(
                                aws_access_key=aws_access_key,
                                aws_secret_key=aws_secret_key,
                                bucket_name=bucket_name,
                                region_name=aws_region
                            )
                            
                            with st.spinner("Fazendo upload..."):
                                success, message = uploader.upload_to_s3(
                                    df_normalized, 
                                    uploaded_file.name,
                                    schema_data
                                )
                                
                            if success:
                                st.success(f"✅ {message}")
                                st.rerun()  # Refresh to update history
                            else:
                                st.error(f"❌ {message}")
                        else:
                            st.error("⚠️ Por favor, configure todas as credenciais do S3")
                            
                else:
                    st.error("❌ Dados inválidos. Verifique os erros abaixo:")
                    
                    for column, errors in validation_report.items():
                        if errors:
                            st.error(f"**{column}**: {', '.join(errors)}")
                    
                    # Allow schema selection for invalid data
                    st.subheader("🔧 Tentar com outro Schema")
                    st.info("Os dados não passaram na validação. Você pode tentar com um schema diferente:")
                    
                    alternative_schemas = [s for s in schemas if s != selected_schema]
                    if alternative_schemas:
                        alternative_schema = st.selectbox(
                            "Selecione um schema alternativo:",
                            options=alternative_schemas,
                            format_func=lambda x: x.replace('.yaml', '').replace('_', ' ').title(),
                            key="alternative_schema"
                        )
                        
                        if st.button("🔄 Validar com Schema Alternativo"):
                            alt_schema_data = schema_manager.load_schema(alternative_schema)
                            if alt_schema_data:
                                alt_is_valid, alt_validation_report = validator.validate_data(df_normalized, alt_schema_data)
                                
                                if alt_is_valid:
                                    st.success(f"✅ Dados válidos com o schema '{alternative_schema}'!")
                                    
                                    if st.button("🚀 Upload com Schema Alternativo", type="primary", key="alt_upload"):
                                        if all([aws_access_key, aws_secret_key, bucket_name]):
                                            uploader = DataUploader(
                                                aws_access_key=aws_access_key,
                                                aws_secret_key=aws_secret_key,
                                                bucket_name=bucket_name,
                                                region_name=aws_region
                                            )
                                            
                                            with st.spinner("Fazendo upload..."):
                                                success, message = uploader.upload_to_s3(
                                                    df_normalized, 
                                                    uploaded_file.name,
                                                    alt_schema_data
                                                )
                                                
                                            if success:
                                                st.success(f"✅ {message}")
                                                st.rerun()
                                            else:
                                                st.error(f"❌ {message}")
                                        else:
                                            st.error("⚠️ Configure as credenciais do S3")
                                else:
                                    st.error(f"❌ Dados também inválidos com o schema '{alternative_schema}'")
                                    for column, errors in alt_validation_report.items():
                                        if errors:
                                            st.error(f"**{column}**: {', '.join(errors)}")
    
    with col2:
        st.header("📜 Histórico de Uploads")
        
        if all([aws_access_key, aws_secret_key, bucket_name]):
            history_manager = S3HistoryManager(
                aws_access_key=aws_access_key,
                aws_secret_key=aws_secret_key,
                bucket_name=bucket_name,
                region_name=aws_region
            )
            
            with st.spinner("Carregando histórico..."):
                history_df = history_manager.get_upload_history()
            
            if history_df is not None and not history_df.empty:
                st.dataframe(
                    history_df,
                    use_container_width=True,
                    column_config={
                        "upload_date": st.column_config.DatetimeColumn(
                            "Data Upload",
                            format="DD/MM/YYYY HH:mm"
                        ),
                        "file_size_mb": st.column_config.NumberColumn(
                            "Tamanho (MB)",
                            format="%.2f MB"
                        )
                    }
                )
                
                st.info(f"📊 **Total de arquivos**: {len(history_df)}")
            else:
                st.info("Nenhum arquivo encontrado no histórico")
        else:
            st.info("Configure as credenciais do S3 para ver o histórico")

if __name__ == "__main__":
    main() 