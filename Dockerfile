FROM python:3.12-slim

# Instala uv via pip
RUN pip install uv

# Cria diretório de trabalho
WORKDIR /src

# Copia apenas os arquivos de dependências primeiro (melhora cache e evita erro)
COPY pyproject.toml ./
COPY README.md ./

# Sincroniza as dependências
RUN uv sync

# Agora copia o resto do código
COPY . .

# Expõe a porta usada pelo Streamlit
EXPOSE 8501

# Executa como poetry run faria
ENTRYPOINT ["uv", "run", "streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
