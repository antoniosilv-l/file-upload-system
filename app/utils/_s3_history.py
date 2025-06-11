import boto3
import duckdb
import os
from datetime import datetime
import streamlit as st

def get_recent_uploads(limit=20):
    """
    Get recent uploads from S3 bucket.
    
    Args:
        limit: Maximum number of recent uploads to return
        
    Returns:
        DataFrame with upload history
    """
    try:
        s3 = boto3.client("s3")
        bucket = os.getenv("S3_BUCKET_NAME", "file-upload-system-s3-prd")
        
        # List objects in the bucket
        response = s3.list_objects_v2(Bucket=bucket)
        
        if 'Contents' not in response:
            # Return empty DuckDB table
            con = duckdb.connect(database=':memory:')
            con.execute("""
                CREATE TABLE uploads (
                    Arquivo VARCHAR,
                    Assunto VARCHAR,
                    "Sub-Assunto" VARCHAR,
                    "Data Upload" VARCHAR,
                    "Tamanho (KB)" DOUBLE,
                    "Última Modificação" VARCHAR
                )
            """)
            return con.execute("SELECT * FROM uploads").df()
        
        # Create DuckDB connection
        con = duckdb.connect(database=':memory:')
        
        # Create table and insert data
        con.execute("""
            CREATE TABLE uploads (
                Arquivo VARCHAR,
                Assunto VARCHAR,
                "Sub-Assunto" VARCHAR,
                "Data Upload" VARCHAR,
                "Tamanho (KB)" DOUBLE,
                "Última Modificação" VARCHAR,
                LastModifiedTimestamp TIMESTAMP
            )
        """)
        
        for obj in response['Contents']:
            key = obj['Key']
            parts = key.split('/')
            
            # Parse the S3 key structure: assunto/sub_assunto/YYYY-MM-DD/filename
            if len(parts) >= 4:
                assunto = parts[0].replace('_', ' ').title()
                sub_assunto = parts[1].replace('_', ' ').title()
                data_upload = parts[2]
                filename = parts[3]
                tamanho_kb = round(obj['Size'] / 1024, 2)
                ultima_modificacao = obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S')
                
                con.execute("""
                    INSERT INTO uploads VALUES (?, ?, ?, ?, ?, ?, ?)
                """, [filename, assunto, sub_assunto, data_upload, tamanho_kb, ultima_modificacao, obj['LastModified']])
        
        # Query with sorting and limit using DuckDB
        df = con.execute(f"""
            SELECT Arquivo, Assunto, "Sub-Assunto", "Data Upload", "Tamanho (KB)", "Última Modificação"
            FROM uploads 
            ORDER BY LastModifiedTimestamp DESC 
            LIMIT {limit}
        """).df()
        
        return df
        
    except Exception as e:
        st.error(f"Erro ao buscar histórico de uploads: {e}")
        # Return empty DuckDB table on error
        con = duckdb.connect(database=':memory:')
        con.execute("""
            CREATE TABLE uploads (
                Arquivo VARCHAR,
                Assunto VARCHAR,
                "Sub-Assunto" VARCHAR,
                "Data Upload" VARCHAR,
                "Tamanho (KB)" DOUBLE,
                "Última Modificação" VARCHAR
            )
        """)
        return con.execute("SELECT * FROM uploads").df()

def refresh_upload_history():
    """
    Force refresh of upload history data.
    """
    st.session_state.upload_history = get_recent_uploads()

def display_upload_history():
    """
    Display upload history using session state caching.
    Only fetches data once per session.
    """
    # Initialize session state for upload history
    if 'upload_history' not in st.session_state:
        with st.spinner('Carregando histórico de uploads...'):
            st.session_state.upload_history = get_recent_uploads()
    
    st.subheader("📊 Histórico de Uploads Recentes")
    
    # Add refresh button
    col1, col2 = st.columns([6, 1])
    
    df = st.session_state.upload_history
    
    if df.empty:
        st.info("Nenhum upload encontrado no bucket.")
    else:
        # Display metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total de Arquivos", len(df))
        with col2:
            st.metric("Tamanho Total", f"{df['Tamanho (KB)'].sum():.2f} KB")
        with col3:
            unique_dates = df['Data Upload'].nunique()
            st.metric("Dias com Upload", unique_dates)
        
        # Display the dataframe
        st.dataframe(
            df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Arquivo": st.column_config.TextColumn("📁 Arquivo"),
                "Assunto": st.column_config.TextColumn("🗂️ Assunto"),
                "Sub-Assunto": st.column_config.TextColumn("📋 Sub-Assunto"),
                "Data Upload": st.column_config.DateColumn("📅 Data Upload"),
                "Tamanho (KB)": st.column_config.NumberColumn("💾 Tamanho (KB)", format="%.2f"),
                "Última Modificação": st.column_config.DatetimeColumn("🕒 Última Modificação")
            }
        ) 