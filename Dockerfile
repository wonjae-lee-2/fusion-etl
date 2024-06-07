FROM python:3.11.9

ENV DAGSTER_HOME=/opt/dagster/dagster_home/

ARG DAGSTER_APP_DIR=/opt/dagster/app/

ENV SQLITE_STORAGE_BASE_DIR=/opt/dagster/storage/

RUN mkdir -p ${DAGSTER_HOME} \
    && mkdir -p ${DAGSTER_APP_DIR} \
    && mkdir -p ${SQLITE_STORAGE_BASE_DIR}

COPY . ${DAGSTER_APP_DIR}

RUN mv ${DAGSTER_APP_DIR}/dagster.yaml ${DAGSTER_HOME} \
    && curl -sL https://aka.ms/InstallAzureCLIDeb | bash \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && curl https://packages.microsoft.com/config/debian/12/prod.list | tee /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && apt-get install -y unixodbc-dev sqlite3 \
    && rm -rf /var/lib/apt/lists/*

COPY azureProfile.json msal_token_cache.json /root/.azure/

WORKDIR ${DAGSTER_APP_DIR}

RUN pip install --no-cache-dir . \
    && playwright install-deps chromium \
    && playwright install chromium \
    && cd dbt_core \
    && dbt deps

VOLUME [ "${SQLITE_STORAGE_BASE_DIR}" ]

EXPOSE 3000

CMD ["dagster", "dev", "-h", "0.0.0.0", "-p", "3000"]
