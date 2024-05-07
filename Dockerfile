FROM python:3.12.3

ENV DAGSTER_HOME=/opt/dagster/dagster_home/

ARG DAGSTER_APP=/opt/dagster/app/

ENV SQLITE_STORAGE_BASE_DIR=/opt/dagster/storage/

RUN mkdir -p ${DAGSTER_HOME} \
    && mkdir -p ${DAGSTER_APP} \
    && mkdir -p ${SQLITE_STORAGE_BASE_DIR}

COPY dagster.yaml ${DAGSTER_HOME}

COPY . ${DAGSTER_APP}

RUN curl -sL https://aka.ms/InstallAzureCLIDeb | bash \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && curl https://packages.microsoft.com/config/debian/12/prod.list | tee /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && rm -rf /var/lib/apt/lists/* \
    && echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc

WORKDIR ${DAGSTER_APP}

RUN rm dagster.yaml \
    && pip install --no-cache-dir . \
    && playwright install-deps chromium \
    && playwright install chromium

VOLUME [ "${SQLITE_STORAGE_BASE_DIR}" ]

EXPOSE 3000

CMD ["dagster", "dev", "-h", "0.0.0.0", "-p", "3000"]