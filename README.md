# Sistema de Upload

Um sistema completo para upload e interação com o Athena. 

Fui influenciado por amigos e familiares do mundo da tecnologia a testar um pouco das famosas IA's, e pelo pouco que utilizei, consegui melhorar minha visão sobre universo, e pude perceber o quão produtivo e utilizar ela junto a um conhecimento prévio do projeto a ser desenvolvido, eu estava com planos para criar uma interface de upload e como teste utilizei o **Cursor** para isso.  

Apesar do projeto possuir alguns bons pontos de melhorias a serem realizadas, cheguei a um nível aceitável de criação de código guiado. 

Utilizei como base a **claude-4-sonnet**, ela se desenvolveu muito bem, pecando em alguns pontos de front-end, código alterado/esquecido e não retirado. 

No geral, considerei uma excelente experiencia!! 


## 📁 Estrutura do Projeto

```
file-upload-system/
├── app/
│   ├── config/          # Configurações (S3, etc.)
│   ├── core/            # Lógica de negócio central
│   ├── models/          # Modelos de dados
│   ├── services/        # Serviços (Auth, S3, Athena, etc.)
│   ├── ui/              # Interface Streamlit
│   └── utils/           # Utilitários
├── assets/              # Imagens e recursos
├── schema/              # Esquemas de validação YAML
├── login.py             # Página de login principal
├── requirements.txt     # Dependências Python
└── README.md            # Este arquivo
```

## Screenshots

![Demo do Sistema](/gifs/msedge_5ZwQGgMrF7.gif)

## 🔒 ENV

```
# =============================================================================
# AWS
# =============================================================================
AWS_ACCESS_KEY_ID=XXXXXXXXXXXXXXXXXXX
AWS_SECRET_ACCESS_KEY=XXXXXXXXXXXXXXXXXXXXXXXXX
S3_BUCKET_NAME=XXXXXXXXXXXXXXXXXXXXX
AWS_DEFAULT_REGION=XXXXXXXXXXXX

# =============================================================================
# Local Authentication (Fallback)
# =============================================================================
DEFAULT_ADMIN_USER=admin
DEFAULT_ADMIN_PASSWORD=admin

# =============================================================================
# UI Customization
# =============================================================================
ADMIN_BACKGROUND_IMAGE=assets/admin_background.jpg
DEFAULT_BACKGROUND_IMAGE=assets/default_background.jpg
```

## 🤖 Apoio de IA no Desenvolvimento

<p align="center">
  <img src="https://upload.wikimedia.org/wikipedia/commons/8/8a/Claude_AI_logo.svg" alt="Claude AI" width="200" height="auto" style="filter: invert(1);">
</p>

Este projeto foi desenvolvido com o apoio da **Claude AI (Anthropic)** como um experimento para avaliar e demonstrar como a inteligência artificial pode auxiliar no desenvolvimento de software. A IA foi utilizada para:

- Arquitetura e estruturação do projeto
- Implementação de funcionalidades principais
- Documentação e organização do código
- Testes e validações

Este experimento demonstra o potencial da colaboração entre desenvolvedores humanos e IA para criar soluções robustas e bem estruturadas.

### Créditos de Desenvolvimento
- **Desenvolvimento assistido por IA**: Claude AI (Anthropic)
- **Planejamento e supervisão**: Desenvolvedor humano
- **Integração e testes**: Colaboração humano-IA

## 📄 Licença

Este projeto está licenciado sob a **Apache License 2.0** - veja o arquivo [LICENSE](LICENSE) para mais detalhes.

### Resumo da Licença
- ✅ **Uso comercial** - Permitido
- ✅ **Modificação** - Permitido
- ✅ **Distribuição** - Permitido
- ✅ **Uso privado** - Permitido
- ⚠️ **Responsabilidade** - O software é fornecido "como está"
- ⚠️ **Garantia** - Sem garantias expressas ou implícitas

A Apache License 2.0 é uma licença permissiva que permite o uso livre do software, incluindo em projetos comerciais, desde que os avisos de copyright e disclaimer sejam mantidos.

## 🤝 Contribuições

Contribuições são bem-vindas! Este projeto serve como exemplo da colaboração entre humanos e IA no desenvolvimento de software.

---

**Desenvolvido com o apoio de Claude AI (Anthropic) | Licensed under Apache 2.0**
