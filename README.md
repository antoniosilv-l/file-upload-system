# Sistema de Upload de Arquivos - Data Platform

Sistema inteligente de upload e validação de arquivos CSV/Excel com schemas configuráveis e categorização automática.

## ✨ Funcionalidades

### 🎯 **Categorização Inteligente**
- **Assunto e Sub-assunto**: Organize uploads por categorias (vendas, financeiro, RH, etc.)
- **Schemas Dinâmicos**: Validação automática baseada na categoria selecionada
- **Detecção Automática**: Sistema identifica schemas disponíveis automaticamente

### 📊 **Suporte Completo a Planilhas**
- **Múltiplos Formatos**: CSV, Excel (.xlsx, .xls)
- **Seleção de Abas**: Escolha qual aba processar em arquivos Excel
- **Detecção Automática**: Separadores CSV detectados automaticamente
- **Configuração Flexível**: Linha de cabeçalho personalizável

### 🛡️ **Validação Avançada**
- **Schemas YAML**: Validação robusta com tipos de dados
- **Feedback Detalhado**: Erros específicos com sugestões de correção
- **Campos Obrigatórios**: Verificação de presença e completude
- **Tipos Suportados**: string, int, float, date, email, phone, bool

### ☁️ **Integração Completa com S3**
- **Upload Inteligente**: Organização automática por assunto/sub-assunto/data
- **Histórico em Tempo Real**: Visualização dos últimos uploads no sidebar
- **Cache Inteligente**: Evita consultas desnecessárias ao S3
- **Metadata Rica**: Informações detalhadas sobre cada upload
- **Configuração Segura**: Credenciais protegidas no backend
- **Preview de Destino**: Mostra onde o arquivo será salvo
- **DuckDB Integration**: Processamento avançado do histórico

## 📁 **Estrutura Organizada do Projeto**

```
├── app/
│   ├── main.py                     # Interface principal Streamlit
│   ├── config/
│   │   ├── __init__.py
│   │   └── s3_config.py            # Configuração segura S3
│   ├── services/                   # Serviços de negócio
│   │   ├── __init__.py
│   │   ├── validation_service.py   # Validação e gerenciamento de schemas
│   │   ├── file_service.py         # Processamento de arquivos
│   │   ├── uploader_service.py     # Upload para S3
│   │   └── s3_history_service.py   # Histórico S3 com DuckDB
│   ├── models/                     # Modelos de dados
│   │   ├── __init__.py
│   │   └── file_info.py            # Classes para informações de arquivos
│   ├── ui/                         # Componentes de interface
│   │   ├── __init__.py
│   │   └── sidebar.py              # Sidebar reutilizável
│   └── utils/                      # Utilitários (compatibilidade)
│       └── __init__.py
├── schema/                         # Schemas organizados por categoria
│   ├── vendas_produtos.yaml        # Vendas > Produtos
│   ├── vendas_clientes.yaml        # Vendas > Clientes  
│   ├── financeiro_receitas.yaml    # Financeiro > Receitas
│   ├── rh_funcionarios.yaml        # RH > Funcionários
│   └── user.yaml                   # Sistema > Usuários
├── env.example                     # Exemplo de configuração
├── requirements.txt
└── README.md
```

## 🏗️ **Arquitetura**

### **Padrão de Organização**
- **Separação por Responsabilidade**: Cada módulo tem uma função específica
- **Services**: Lógica de negócio centralizada em serviços
- **Models**: Estruturas de dados tipadas
- **UI Components**: Componentes reutilizáveis de interface
- **Config**: Configurações centralizadas e seguras

### **Serviços Principais**

#### **ValidationService**
- Validação de dados contra schemas YAML
- Gerenciamento de assuntos e sub-assuntos
- Carregamento dinâmico de schemas
- Validadores tipados (string, int, float, date, email, phone, bool)

#### **FileProcessingService**
- Processamento de CSV e Excel
- Normalização automática de colunas
- Detecção de separadores
- Seleção de abas Excel
- Preview otimizado

#### **S3UploaderService**
- Upload seguro para S3
- Organização por categoria e data
- Metadata completa
- Conversão automática para CSV
- Teste de conectividade

#### **S3HistoryService**
- Histórico de uploads com DuckDB
- Cache inteligente
- Busca otimizada
- Informações detalhadas

## 🚀 Como Usar

### Pré-requisitos
- Python 3.8+
- Ambiente virtual recomendado

### Instalação

1. **Clone e configure o projeto**
```bash
git clone <repository-url>
cd file-upload-system
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# ou .venv\Scripts\activate  # Windows
```

2. **Instale dependências**
```bash
pip install -r requirements.txt
```

3. **Configure S3 (escolha uma opção)**

**Opção A: Variáveis de ambiente (recomendado)**
```bash
export AWS_ACCESS_KEY_ID="sua_access_key"
export AWS_SECRET_ACCESS_KEY="sua_secret_key"
export S3_BUCKET_NAME="seu_bucket"
export AWS_DEFAULT_REGION="us-east-1"
```

**Opção B: Arquivo .env**
```bash
cp env.example .env
# Edite o arquivo .env com suas credenciais
```

**Opção C: Editar código**
```python
# Em app/config/s3_config.py
self.aws_access_key = 'sua_access_key'
self.aws_secret_key = 'sua_secret_key'
self.bucket_name = 'seu_bucket'
```

4. **Execute a aplicação**
```bash
streamlit run app/main.py
```

### Como Usar a Interface

1. **Configure S3 (Sistema)**
   - Credenciais configuradas via variáveis de ambiente ou código
   - Status visível na sidebar (configurado/não configurado)
   - Segurança: usuários não têm acesso às credenciais

2. **Visualize Histórico (Sidebar)**
   - Veja os últimos arquivos enviados
   - Informações organizadas por categoria com ícones
   - Use "🔄 Atualizar Histórico" para recarregar

3. **Selecione Categoria**
   - Escolha o **Assunto** (vendas, financeiro, rh, sistema)
   - Selecione o **Sub-assunto** específico
   - Schema será carregado automaticamente

4. **Upload do Arquivo**
   - Faça upload do arquivo CSV ou Excel
   - Para Excel: selecione a aba desejada
   - Para CSV: configure separador (ou use detecção automática)

5. **Configuração e Preview**
   - Defina linha do cabeçalho se necessário
   - Visualize prévia dos dados
   - Verifique informações técnicas amigáveis
   - Veja o destino no S3 onde será salvo

6. **Validação**
   - Sistema valida automaticamente contra o schema
   - Erros são exibidos com detalhes específicos
   - Campos esperados são listados para referência

7. **Upload Final**
   - Se válido, clique em "Enviar para S3"
   - Histórico é atualizado automaticamente após sucesso

## 📋 Schemas Disponíveis

### Vendas
- **Produtos**: `produto_id`, `nome_produto`, `categoria`, `preco`, `quantidade_vendida`, `data_venda`, `vendedor`
- **Clientes**: `cliente_id`, `nome_cliente`, `email`, `telefone`, `cidade`, `estado`, `data_cadastro`, `ativo`

### Financeiro
- **Receitas**: `receita_id`, `descricao`, `valor`, `data_receita`, `categoria`, `forma_pagamento`, `observacoes`

### RH
- **Funcionários**: `funcionario_id`, `nome_completo`, `cpf`, `email_corporativo`, `cargo`, `departamento`, `salario`, `data_admissao`, `ativo`

### Sistema
- **Usuários**: `id`, `first_name`, `last_name`, `email`, `gender`, `ip_address`

## 🔧 Exemplo de Schema

```yaml
description: "Schema para dados de vendas de produtos"
subject: "vendas"
sub_subject: "produtos"
schema:
  table_name: "vendas_produtos"
  fields:
    - name: "produto_id"
      description: "ID do produto"
      type: "int"
      required: true
    - name: "categoria"
      type: "str"
      required: true
      allowed_values: ["eletrônicos", "roupas", "casa", "esportes", "livros"]
```

## 📝 Arquivos de Exemplo

O sistema inclui arquivos de exemplo para teste:
- `exemplo_vendas_produtos.csv` - Dados de produtos
- `exemplo_multiplas_abas.xlsx` - Excel com abas Clientes e Funcionários

## 🛠️ Tecnologias

- **Streamlit**: Interface web responsiva
- **DuckDB**: Processamento rápido de dados e análise
- **OpenPyXL**: Leitura de arquivos Excel
- **PyYAML**: Processamento de schemas
- **Boto3**: Integração com AWS S3

## 📊 Funcionalidades Avançadas

- **Detecção Automática de Separadores**: Para arquivos CSV
- **Múltiplas Abas Excel**: Seleção interativa
- **Validação Inteligente**: Feedback específico por campo
- **Preview Limitado**: Performance otimizada para arquivos grandes
- **Categorização Automática**: Schema selection baseado em subject/sub-subject
- **Organização S3 Inteligente**: Estrutura `assunto/sub-assunto/YYYY/MM/DD/arquivo_timestamp.csv`
- **Cache de Histórico**: Session state para evitar consultas excessivas ao S3
- **Conversão Automática**: Todos os arquivos são salvos como CSV padronizado
- **Metadata Completa**: Informações detalhadas salvas com cada arquivo
- **Interface Amigável**: Tipos de dados técnicos convertidos para linguagem simples

## 🎯 **Melhorias na Organização**

### **Separação de Responsabilidades**
- ✅ **Services**: Lógica de negócio isolada
- ✅ **Models**: Estruturas de dados tipadas
- ✅ **UI**: Componentes reutilizáveis
- ✅ **Config**: Configurações centralizadas

### **Padrões de Mercado**
- ✅ **Modularização**: Código organizado por funcionalidade
- ✅ **Reutilização**: Componentes e serviços reutilizáveis
- ✅ **Manutenibilidade**: Fácil localização e modificação
- ✅ **Testabilidade**: Código isolado facilita testes
- ✅ **Escalabilidade**: Estrutura preparada para crescimento

### **Compatibilidade**
- ✅ **Imports Legados**: Manutenção de compatibilidade
- ✅ **Migração Gradual**: Transição sem quebrar código existente
- ✅ **Documentação**: README atualizado com nova estrutura

## 🔒 **Segurança**

- **Credenciais Protegidas**: Nunca expostas no frontend
- **Configuração Flexível**: Múltiplas opções de configuração
- **Validação Robusta**: Dados verificados antes do upload
- **Metadata Completa**: Rastreabilidade total dos uploads
