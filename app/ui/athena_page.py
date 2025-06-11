import streamlit as st
import duckdb
from typing import Dict, List, Any, Optional
from app.services.athena_service import AthenaService
from app.config.s3_config import s3_config
import time


class AthenaPageComponent:
    """Componente da página Athena para edição de tabelas"""
    
    def __init__(self):
        self.athena_service = AthenaService()
        self.conn = duckdb.connect(":memory:")
    
    def render(self):
        """Renderiza a página do Athena com design moderno"""
        # Limpar dados antigos se estivermos mudando de página
        if 'current_page' in st.session_state and st.session_state.get('current_page') != "Athena & Tabelas":
            # Limpar dados de tabela quando sair da página Athena
            keys_to_clear = ['table_data', 'original_data', 'edited_data', 'has_changes', 'schema_info']
            for key in keys_to_clear:
                if key in st.session_state:
                    del st.session_state[key]
        
        # Header com informações de status
        self._render_header()
        
        if not self.athena_service.is_available():
            self._render_unavailable_message()
            return
        
        # Layout principal com abas (removido Query Editor)
        self._render_main_interface()
    
    def _render_header(self):
        """Renderiza header moderno com status e informações"""
        col1, col2, col3 = st.columns([2, 1, 1])
        
        with col1:
            st.title("🗃️ Athena Data Explorer")
            st.caption("Explore, edit and track changes in your data tables")
        
        with col2:
            if self.athena_service.is_available():
                st.success("🟢 Connected")
            else:
                st.error("🔴 Disconnected")
        
        with col3:
            # Status da tabela de log
            if 'selected_database' in st.session_state:
                database = st.session_state['selected_database']
                tables = self.athena_service.get_tables(database)
                if "data_changes_log" in tables:
                    st.info("📋 Audit Log Active")
                else:
                    st.warning("⚠️ No Audit Log")
    
    def _render_main_interface(self):
        """Renderiza interface principal com abas (sem Query Editor)"""
        tab1, tab2 = st.tabs(["🔍 Data Explorer", "📋 Audit Log"])
        
        with tab1:
            self._render_data_explorer()
        
        with tab2:
            self._render_audit_log_tab()
    
    def _render_unavailable_message(self):
        """Mensagem quando Athena não está disponível"""
        st.error("🔴 Athena service is not available")
        st.info("Please check your AWS credentials and configuration")
    
    def _render_data_explorer(self):
        """Renderiza explorador de dados estilo Databricks"""
        col1, col2 = st.columns([1, 2])
        
        with col1:
            self._render_modern_database_explorer()
        
        with col2:
            if 'selected_table' in st.session_state and 'selected_database' in st.session_state:
                self._render_modern_table_viewer()
            else:
                self._render_welcome_message()
    
    def _render_modern_database_explorer(self):
        """Explorer moderno estilo Databricks/Snowflake"""
        st.markdown("### 🗂️ Database Explorer")
        
        # Seletor de database com search
        databases = self.athena_service.get_databases()
        if databases:
            selected_database = st.selectbox(
                "Select Database",
                options=databases,
                key="db_selector",
                help="Choose a database to explore"
            )
            
            if selected_database:
                st.session_state['selected_database'] = selected_database
                
                # Lista de tabelas com search e filtros
                tables = self.athena_service.get_tables(selected_database)
                if tables:
                    st.markdown(f"**Tables in `{selected_database}`** ({len(tables)})")
                    
                    # Search box para tabelas
                    search_term = st.text_input("🔍 Search tables", placeholder="Type to filter tables...")
                    
                    # Filtrar tabelas por search
                    if search_term:
                        filtered_tables = [t for t in tables if search_term.lower() in t.lower()]
                    else:
                        filtered_tables = tables
                    
                    # Lista de tabelas com ícones
                    for table in filtered_tables:
                        col_a, col_b = st.columns([3, 1])
                        
                        with col_a:
                            if st.button(f"📊 {table}", key=f"table_{table}", use_container_width=True):
                                st.session_state['selected_table'] = table
                                st.session_state['table_data'] = None  # Reset data
                                st.rerun()
                        
                        with col_b:
                            # Indicador se tabela está selecionada
                            if st.session_state.get('selected_table') == table:
                                st.markdown("🔵")
                else:
                    st.info("No tables found in this database")
        else:
            st.error("No databases available")
    
    def _render_welcome_message(self):
        """Mensagem de boas-vindas moderna"""
        st.markdown("""
        <div style="text-align: center; padding: 3rem; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); border-radius: 10px; color: white;">
            <h2>🚀 Welcome to Athena Data Explorer</h2>
            <p style="font-size: 1.1rem; margin: 1rem 0;">Select a database and table from the explorer to get started</p>
            <div style="display: flex; justify-content: center; gap: 2rem; margin-top: 2rem;">
                <div>
                    <h4>✨ Explore</h4>
                    <p>Browse your data with interactive filters</p>
                </div>
                <div>
                    <h4>✏️ Edit</h4>
                    <p>Modify data with full audit tracking</p>
                </div>
                <div>
                    <h4>📊 Analyze</h4>
                    <p>View comprehensive audit logs</p>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    def _render_modern_table_viewer(self):
        """Visualizador de tabela moderno"""
        database = st.session_state.get('selected_database')
        table = st.session_state.get('selected_table')
        
        # Verificar se database e table estão selecionados
        if not database or not table:
            st.info("Please select a database and table from the explorer above to view details")
            return
        
        # Header da tabela
        col1, col2, col3 = st.columns([2, 1, 1])
        
        with col1:
            st.markdown(f"### 📊 `{database}.{table}`")
        
        with col2:
            # Toggle para modo edição
            edit_mode = st.toggle("✏️ Edit Mode", key="edit_mode_toggle")
        
        with col3:
            # Refresh button
            if st.button("🔄 Refresh", use_container_width=True):
                st.session_state['table_data'] = None
                st.rerun()
        
        # Informações da tabela em tabs
        info_tab, data_tab, schema_tab = st.tabs(["📋 Overview", "📊 Data", "🔧 Schema"])
        
        with info_tab:
            self._render_table_overview(database, table)
        
        with data_tab:
            if edit_mode:
                self._render_data_editor_modern()
            else:
                self._render_data_viewer_modern()
        
        with schema_tab:
            self._render_schema_info(database, table)
    
    def _render_table_overview(self, database: str, table: str):
        """Renderiza overview da tabela"""
        schema_info = self.athena_service.get_table_schema(database, table)
        
        if schema_info:
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("📊 Columns", len(schema_info.get('columns', [])))
            
            with col2:
                creation_time = schema_info.get('creation_time', 'Unknown')
                if hasattr(creation_time, 'strftime'):
                    formatted_time = creation_time.strftime('%Y-%m-%d')
                    st.metric("📅 Created", formatted_time)
                else:
                    st.metric("📅 Created", str(creation_time))
            
            with col3:
                table_type = schema_info.get('table_type', 'Unknown')
                st.metric("🏷️ Type", table_type)
            
            # Informações adicionais
            st.markdown("#### 🏠 Storage Information")
            location = schema_info.get('location', 'N/A')
            input_format = schema_info.get('input_format', 'N/A')
            
            col1, col2 = st.columns(2)
            with col1:
                st.code(f"Location: {location}")
            with col2:
                st.code(f"Format: {input_format}")
            
            # Compatibilidade para edição
            can_modify, reason = self.athena_service.can_modify_table(schema_info)
            if can_modify:
                st.success(f"✅ Table can be edited: {reason}")
            else:
                st.warning(f"⚠️ Table editing limited: {reason}")
        else:
            st.error("Could not retrieve table schema")
    
    def _render_schema_info(self, database: str, table: str):
        """Renderiza informações de schema"""
        schema_info = self.athena_service.get_table_schema(database, table)
        
        if schema_info and 'columns' in schema_info:
            st.markdown("#### 📋 Column Information")
            
            # Criar dados da tabela de schema
            columns_data = []
            for col in schema_info['columns']:
                columns_data.append({
                    'Column': col['name'],
                    'Type': col['type'],
                    'Comment': col.get('comment', '')
                })
            
            # Mostrar como dataframe
            st.dataframe(columns_data, use_container_width=True, hide_index=True)
            
            # Informações de particionamento
            partition_keys = schema_info.get('partition_keys', [])
            if partition_keys:
                st.markdown("#### 🔑 Partition Keys")
                partition_data = []
                for pk in partition_keys:
                    partition_data.append({
                        'Partition Column': pk['name'],
                        'Type': pk['type']
                    })
                st.dataframe(partition_data, use_container_width=True, hide_index=True)
            else:
                st.info("This table has no partition keys")
        else:
            st.error("Could not retrieve schema information")
    
    def _render_data_viewer_modern(self):
        """Renderiza visualizador de dados moderno"""
        database = st.session_state.get('selected_database')
        table = st.session_state.get('selected_table')
        
        # Só carregar dados se database e table estiverem selecionados
        if not database or not table:
            st.info("Please select a database and table to view data")
            return
        
        # Carregar dados se não estiverem no cache
        if 'table_data' not in st.session_state or st.session_state['table_data'] is None:
            with st.spinner("Loading table data..."):
                table_data = self.athena_service.get_table_with_changes(database, table, 100)
                st.session_state['table_data'] = table_data
        
        table_data = st.session_state['table_data']
        
        if table_data:
            # Filtros modernos - só mostrar se explicitamente solicitado
            show_filters = st.checkbox("🔍 Show Data Filters", key="show_data_filters")
            
            if show_filters:
                filtered_data = self._apply_modern_filters(table_data)
            else:
                filtered_data = table_data
            
            # Mostrar métricas
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Records", len(table_data))
            with col2:
                st.metric("Filtered Records", len(filtered_data))
            with col3:
                if table_data:
                    st.metric("Columns", len(table_data[0].keys()))
            
            # Exibir dados
            if filtered_data:
                st.dataframe(
                    filtered_data,
                    use_container_width=True,
                    hide_index=True
                )
            else:
                st.info("No data matches the current filters")
                
            # Botões de ação
            self._render_modern_action_buttons()
        else:
            st.info("No data available for this table")
    
    def _apply_modern_filters(self, data: List[Dict]) -> List[Dict]:
        """Aplica filtros modernos aos dados"""
        if not data:
            return data
        
        filtered_data = data.copy()
        
        # Expandir seção de filtros no corpo principal em vez da sidebar
        with st.expander("🔍 Data Filters", expanded=True):
            filter_cols = st.columns(3)
            col_index = 0
            
            # Filtros dinâmicos baseados nas colunas
            for column in data[0].keys():
                # Coletar valores únicos para a coluna
                unique_values = list(set(str(row.get(column, '')) for row in data))
                unique_values = [v for v in unique_values if v and v != 'None']
                
                with filter_cols[col_index % 3]:
                    if len(unique_values) > 1 and len(unique_values) <= 50:
                        # Selectbox para colunas com poucos valores únicos
                        selected_values = st.multiselect(
                            f"Filter {column}",
                            options=unique_values,
                            key=f"filter_{column}"
                        )
                        
                        if selected_values:
                            filtered_data = [
                                row for row in filtered_data 
                                if str(row.get(column, '')) in selected_values
                            ]
                    
                    elif len(unique_values) > 1:
                        # Text input para busca em colunas com muitos valores
                        search_term = st.text_input(
                            f"Search in {column}",
                            key=f"search_{column}",
                            placeholder=f"Search {column}..."
                        )
                        
                        if search_term:
                            filtered_data = [
                                row for row in filtered_data 
                                if search_term.lower() in str(row.get(column, '')).lower()
                            ]
                
                col_index += 1
        
        return filtered_data

    def _render_data_editor_modern(self):
        """Renderiza editor de dados com suporte a listas de dicionários"""
        database = st.session_state.get('selected_database')
        table = st.session_state.get('selected_table')
        
        # Só carregar dados se database e table estiverem selecionados
        if not database or not table:
            st.info("Please select a database and table to edit data")
            return
        
        # Carregar dados se não estiverem no cache
        if 'table_data' not in st.session_state or st.session_state['table_data'] is None:
            with st.spinner("Loading table data..."):
                table_data = self.athena_service.get_table_with_changes(database, table, 100)
                schema_info = self.athena_service.get_table_schema(database, table)
                
                if table_data is not None:
                    st.session_state['table_data'] = table_data
                    st.session_state['original_data'] = table_data.copy()
                    st.session_state['schema_info'] = schema_info
                else:
                    st.error("Failed to load table data")
                    return
        
        table_data = st.session_state['table_data']
        
        if table_data and len(table_data) > 0:
            # Editor usando st.data_editor
            st.markdown("### ✏️ Data Editor")
            
            # Converter para formato que o streamlit pode editar
            edited_data = st.data_editor(
                table_data,
                use_container_width=True,
                num_rows="dynamic",
                key="data_editor"
            )
            
            # Verificar se houve mudanças - converter para lista se necessário
            original_data = st.session_state.get('original_data', [])
            
            # Função auxiliar para comparar dados de forma segura
            def data_has_changed(edited_data, original_data):
                try:
                    # Converter edited_data para lista de dicionários se for DataFrame
                    if hasattr(edited_data, 'to_dict'):
                        edited_data_list = edited_data.to_dict('records')
                    elif hasattr(edited_data, 'tolist'):
                        # Se for numpy array ou similar
                        edited_data_list = edited_data.tolist()
                    else:
                        edited_data_list = edited_data
                    
                    # Garantir que ambos sejam listas
                    if not isinstance(original_data, list):
                        original_data = []
                    
                    if not isinstance(edited_data_list, list):
                        return True  # Se não conseguir converter, assume que mudou
                    
                    # Comparar tamanhos primeiro
                    if len(edited_data_list) != len(original_data):
                        return True
                    
                    # Comparar conteúdo elemento por elemento
                    for i, (edited_row, original_row) in enumerate(zip(edited_data_list, original_data)):
                        if edited_row != original_row:
                            return True
                    
                    return False
                    
                except Exception as e:
                    # Em caso de erro na comparação, assume que houve mudança
                    print(f"Erro na comparação de dados: {e}")
                    return True
            
            # Usar a função auxiliar para verificar mudanças
            has_changes = data_has_changed(edited_data, original_data)
            
            if has_changes:
                # Converter edited_data para lista de dicionários
                if hasattr(edited_data, 'to_dict'):
                    edited_data_list = edited_data.to_dict('records')
                else:
                    edited_data_list = edited_data
                    
                st.session_state['has_changes'] = True
                st.session_state['edited_data'] = edited_data_list
                
                st.warning("⚠️ You have unsaved changes")
                
                # Botões de ação
                col1, col2, col3 = st.columns(3)
                with col1:
                    if st.button("💾 Save Changes", type="primary", use_container_width=True):
                        self._save_changes()
                
                with col2:
                    if st.button("🔄 Revert", use_container_width=True):
                        self._revert_changes()
                
                with col3:
                    if st.button("📥 Export CSV", use_container_width=True):
                        self._export_csv()
            else:
                st.session_state['has_changes'] = False
                
                # Botões quando não há mudanças
                self._render_modern_action_buttons()
        else:
            st.info("No data available for editing")
    
    def _render_modern_action_buttons(self):
        """Renderiza botões de ação modernos"""
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            if st.button("🔄 Reload Data", use_container_width=True):
                self._reload_data()
        
        with col2:
            if st.button("📥 Export CSV", use_container_width=True):
                self._export_csv()
        
        with col3:
            if st.button("🗑️ Reset Audit Log", use_container_width=True):
                st.session_state['show_reset_modal'] = True
        
        with col4:
            database = st.session_state.get('selected_database')
            if database:
                if st.button("📋 Create Log Table", use_container_width=True):
                    success, message = self.athena_service.create_changes_log_table(database)
                    if success:
                        st.success(message)
                    else:
                        st.error(message)
        
        # Modais
        self._render_modals()
    
    def _render_modals(self):
        """Renderiza modais de confirmação"""
        if st.session_state.get('show_reset_modal', False):
            self._render_reset_modal()
    
    def _render_reset_modal(self):
        """Modal de confirmação para reset do audit log"""
        if st.session_state.get('show_reset_modal', False):
            with st.container():
                st.error("⚠️ **Confirm Reset Audit Log**")
                st.write("This will delete all audit log data for this database. This action cannot be undone.")
                
                col1, col2, col3 = st.columns([1, 1, 1])
                
                with col1:
                    if st.button("❌ Cancel", use_container_width=True):
                        st.session_state['show_reset_modal'] = False
                        st.rerun()
                
                with col3:
                    if st.button("🗑️ Confirm Reset", type="primary", use_container_width=True):
                        st.session_state['show_reset_modal'] = False
                        self._reset_audit_log()
                        st.rerun()
    
    def _reset_audit_log(self):
        """Reseta o audit log recriando a tabela"""
        database = st.session_state.get('selected_database')
        if database:
            with st.spinner("Resetting audit log..."):
                success, message = self.athena_service.recreate_changes_log_table(database)
                if success:
                    st.success(f"✅ {message}")
                else:
                    st.error(f"❌ {message}")
    
    def _render_audit_log_tab(self):
        """Renderiza aba do audit log"""
        st.markdown("### 📋 Data Changes Audit Log")
        
        # Controles de filtro
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            selected_database = st.selectbox(
                "Database:",
                options=self.athena_service.get_databases(),
                key="audit_db_selector"
            )
        
        with col2:
            table_filter = st.text_input("Filter by table:", placeholder="Enter table name")
        
        with col3:
            operation_filter = st.selectbox(
                "Operation:",
                options=["All", "INSERT", "UPDATE", "DELETE"],
                key="operation_filter"
            )
        
        with col4:
            limit = st.number_input("Max records:", min_value=10, max_value=1000, value=100)
        
        if selected_database:
            if st.button("🔍 Load Audit Log", type="primary", use_container_width=True):
                self._load_and_display_audit_log(selected_database, table_filter, operation_filter, limit)
    
    def _load_and_display_audit_log(self, database: str, table_filter: str, operation_filter: str, limit: int):
        """Carrega e exibe audit log"""
        try:
            with st.spinner("Loading audit log..."):
                # Construir query para o audit log
                query = f"""
                SELECT 
                    id,
                    table_name,
                    operation,
                    row_identifier,
                    updated_by,
                    updated_at,
                    session_id,
                    change_reason
                FROM {database}.data_changes_log
                WHERE 1=1
                """
                
                # Adicionar filtros
                if table_filter:
                    query += f" AND table_name LIKE '%{table_filter}%'"
                
                if operation_filter != "All":
                    query += f" AND operation = '{operation_filter}'"
                
                query += f" ORDER BY updated_at DESC LIMIT {limit}"
                
                # Executar query
                success, message, execution_id = self.athena_service.execute_query(query, database)
                
                if success and execution_id:
                    log_data = self.athena_service.get_query_results(execution_id)
                    
                    if log_data:
                        st.success(f"✅ Found {len(log_data)} audit log entries")
                        
                        # Exibir dados do log
                        st.dataframe(
                            log_data,
                            use_container_width=True,
                            hide_index=True
                        )
                        
                        # Estatísticas do log
                        if len(log_data) > 0:
                            st.markdown("#### 📊 Log Statistics")
                            
                            # Contar operações
                            operations = {}
                            tables = set()
                            
                            for row in log_data:
                                op = row.get('operation', 'Unknown')
                                operations[op] = operations.get(op, 0) + 1
                                tables.add(row.get('table_name', 'Unknown'))
                            
                            col1, col2, col3 = st.columns(3)
                            
                            with col1:
                                st.metric("Total Changes", len(log_data))
                            
                            with col2:
                                st.metric("Tables Affected", len(tables))
                            
                            with col3:
                                most_common_op = max(operations.items(), key=lambda x: x[1]) if operations else ("None", 0)
                                st.metric("Most Common Operation", f"{most_common_op[0]} ({most_common_op[1]})")
                    else:
                        st.info("No audit log entries found with the specified filters")
                else:
                    st.error(f"Failed to load audit log: {message}")
                    
        except Exception as e:
            st.error(f"❌ Error loading audit log: {str(e)}")
    
    def _save_changes(self):
        """Salva mudanças no log de alterações"""
        database = st.session_state.get('selected_database')
        table = st.session_state.get('selected_table')
        original_data = st.session_state.get('original_data', [])
        edited_data = st.session_state.get('edited_data', [])
        
        # Verificar se temos os dados necessários
        if not database or not table:
            st.error("❌ Database or table not selected")
            return
        
        if not original_data and not edited_data:
            st.warning("⚠️ No data to save")
            return
        
        with st.spinner("Saving changes to audit log..."):
            success, message = self.athena_service.save_table_changes(
                database, table, original_data, edited_data, "streamlit_user"
            )
            
            if success:
                st.success(f"✅ {message}")
                # Recarregar dados
                self._reload_data()
            else:
                st.error(f"❌ {message}")
    
    def _revert_changes(self):
        """Reverte mudanças para dados originais"""
        original_data = st.session_state.get('original_data', [])
        
        if original_data:
            st.session_state['table_data'] = original_data.copy()
            st.session_state['has_changes'] = False
            if 'edited_data' in st.session_state:
                del st.session_state['edited_data']
            st.success("✅ Changes reverted")
            st.rerun()
        else:
            st.warning("⚠️ No original data to revert to")
    
    def _reload_data(self):
        """Recarrega dados da tabela"""
        database = st.session_state.get('selected_database')
        table = st.session_state.get('selected_table')
        
        if not database or not table:
            st.error("❌ Database or table not selected")
            return
        
        with st.spinner("Reloading data (applying changes from log)..."):
            # Usar método que aplica alterações do log
            table_data = self.athena_service.get_table_with_changes(database, table, 100)
            
            if table_data is not None:
                st.session_state['table_data'] = table_data
                st.session_state['original_data'] = table_data.copy()
                st.session_state['has_changes'] = False
                if 'edited_data' in st.session_state:
                    del st.session_state['edited_data']
                st.success("✅ Data reloaded with latest changes from audit log")
                st.rerun()
            else:
                st.error("❌ Failed to reload data")
    
    def _export_csv(self):
        """Exporta dados atuais como CSV"""
        if 'table_data' in st.session_state and st.session_state['table_data']:
            data = st.session_state.get('edited_data', st.session_state['table_data'])
            
            if data:
                # Converter para CSV usando DuckDB
                self.conn.register('export_data', data)
                csv_result = self.conn.execute("COPY export_data TO '/tmp/export.csv' (HEADER, DELIMITER ',')").fetchall()
                
                # Ler o arquivo gerado
                try:
                    with open('/tmp/export.csv', 'r', encoding='utf-8') as f:
                        csv_content = f.read()
                    
                    # Download
                    st.download_button(
                        label="📥 Download CSV",
                        data=csv_content,
                        file_name=f"{st.session_state['selected_table']}_export.csv",
                        mime="text/csv"
                    )
                except Exception as e:
                    # Fallback: criar CSV simples
                    if data:
                        import csv
                        import io
                        output = io.StringIO()
                        if data:
                            writer = csv.DictWriter(output, fieldnames=data[0].keys())
                            writer.writeheader()
                            writer.writerows(data)
                        
                        st.download_button(
                            label="📥 Download CSV",
                            data=output.getvalue(),
                            file_name=f"{st.session_state['selected_table']}_export.csv",
                            mime="text/csv"
                        )
            else:
                st.warning("No data to export")


def render_athena_page():
    """Função para renderizar a página do Athena"""
    athena_page = AthenaPageComponent()
    athena_page.render() 