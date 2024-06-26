FROM python:3.11.9

ARG DAGSTER_ROOT=/opt/dagster/

ENV DAGSTER_HOME=/opt/dagster/dagster_home/

ARG DAGSTER_APP_DIR=/opt/dagster/app/

ENV SQLITE_STORAGE_BASE_DIR=/opt/dagster/storage/

RUN mkdir -p ${DAGSTER_HOME} \
    && mkdir -p ${DAGSTER_APP_DIR} \
    && mkdir -p ${SQLITE_STORAGE_BASE_DIR}

COPY .devcontainer/requirements.txt ${DAGSTER_ROOT}

COPY dagster.yaml ${DAGSTER_HOME}

RUN curl -sL https://aka.ms/InstallAzureCLIDeb | bash \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && curl https://packages.microsoft.com/config/debian/12/prod.list | tee /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && apt-get install -y unixodbc-dev sqlite3 \
    && rm -rf /var/lib/apt/lists/*

COPY config/azureProfile.json config/msal_token_cache.json /root/.azure/

WORKDIR ${DAGSTER_ROOT}

RUN curl -LsSf https://astral.sh/uv/install.sh | sh \
    && . $HOME/.cargo/env \
    && uv venv \
    && . .venv/bin/activate \
    && uv pip install --no-cache-dir -r requirements.txt \
    && rm requirements.txt \
    && playwright install-deps chromium \
    && playwright install chromium

WORKDIR ${DAGSTER_APP_DIR}

COPY config/id_ed25519 config/id_ed25519.pub /root/.ssh/

RUN chmod 600 /root/.ssh/id_ed25519

VOLUME [ "${SQLITE_STORAGE_BASE_DIR}" ]

EXPOSE 3000

CMD ["/bin/sh", "-c", "while sleep 1000; do :; done"]
