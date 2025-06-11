# Sistema de Upload de Arquivos

Sistema de upload e validação de arquivos CSV/Excel com schemas configuráveis, integração com AWS S3 e processamento com DuckDB.

## ✨ Funcionalidades

- 📤 **Upload de Arquivos**: Suporte para CSV e Excel (.xlsx, .xls)
- 🔍 **Validação de Dados**: Validação automática com schemas YAML configuráveis
- 🔄 **Normalização de Colunas**: Normalização automática de nomes de colunas
- 📊 **Preview de Dados**: Visualização prévia dos dados antes do upload
- ☁️ **Integração S3**: Upload automático para AWS S3 com estrutura de pastas por data
- 📜 **Histórico**: Acompanhamento de uploads com DuckDB para análises rápidas
- 🎯 **Detecção Automática**: Detecção de encoding e separadores para CSV
- 📋 **Múltiplas Abas**: Suporte a seleção de abas em arquivos Excel

## 🚀 Como Usar

### Pré-requisitos

- Python 3.8+
- Conta AWS com acesso ao S3

### Instalação

1. Clone o repositório
```bash
git clone <repository-url>
cd file-upload-system
```

2. Instale as dependências
```bash
pip install -r requirements.txt
```

3. Execute o aplicativo
```bash
streamlit run app/main.py
```

### Configuração

1. **Schemas**: Coloque seus arquivos de schema YAML na pasta `schema/`
2. **Credenciais AWS**: Configure suas credenciais na interface do Streamlit
3. **Bucket S3**: Informe o nome do seu bucket S3

## 📁 Estrutura do Projeto

```
file-upload-system/
├── app/
│   ├── main.py              # Interface principal Streamlit
│   └── utils/
│       ├── _schema_manager.py    # Gerenciamento de schemas
│       ├── _validator.py         # Validação de dados
│       ├── _previewer.py         # Preview de arquivos
│       ├── _normalizer.py        # Normalização de colunas
│       ├── _uploader.py          # Upload para S3
│       └── _s3_history.py        # Histórico com DuckDB
├── schema/                  # Schemas de validação YAML
│   ├── aeroportos.yaml
│   ├── usuarios.yaml
│   └── user.yaml
├── requirements.txt         # Dependências Python
└── README.md
```

## 🔧 Schemas

Os schemas definem a estrutura e validação dos dados. Exemplo:

```yaml
description: "Schema para dados de usuários"
schema:
  table_name: "usuarios"
  fields:
    - name: "id"
      type: "int"
      required: true
    - name: "email"
      type: "email"
      required: true
    - name: "idade"
      type: "int"
      min_value: 18
      max_value: 120
```

## 🛠️ Tecnologias

- **Streamlit**: Interface web
- **Pandas**: Manipulação de dados
- **DuckDB**: Análise de dados e histórico
- **AWS S3**: Armazenamento em nuvem
- **PyYAML**: Parsing de schemas
- **OpenPyXL**: Leitura de arquivos Excel

## 📊 Funcionalidades do DuckDB

- Histórico de uploads com queries SQL
- Estatísticas agregadas
- Busca por arquivos
- Análise temporal de uploads
- Performance otimizada para grandes volumes

## 🔍 Validações Suportadas

- **Tipos**: string, int, float, date, email, phone, bool
- **Obrigatoriedade**: campos required/optional
- **Tamanho**: min_length, max_length
- **Valores**: min_value, max_value
- **Lista**: allowed_values
- **Formatos**: validação de email e telefone

## 🗂️ Organização S3

Os arquivos são organizados automaticamente:
```
uploads/
├── 2024/
│   ├── 01/
│   │   ├── 15/
│   │   │   ├── arquivo1_20240115_143022.csv
│   │   │   └── arquivo2_20240115_144511.csv
```
