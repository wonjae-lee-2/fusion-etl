FROM python:3.11.6-bookworm
ENV PATH="/root/.local/bin:$PATH"
WORKDIR /fusion_etl
COPY ./ ./
RUN mkdir -p /etc/apt/keyrings && \
    curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | \
        gpg --dearmor -o /etc/apt/keyrings/microsoft-prod.gpg && \
    curl https://packages.microsoft.com/config/debian/12/prod.list | \
        sed 's|signed-by=.*]|signed-by=/etc/apt/keyrings/microsoft-prod.gpg]|' | \
        tee /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql18 && \
    rm -rf /var/lib/apt/lists/*
RUN curl -sSL https://install.python-poetry.org | python3 - && \
    poetry config virtualenvs.create false
RUN poetry install --no-interaction && \
    playwright install --with-deps chromium
CMD [ "/bin/bash" ]