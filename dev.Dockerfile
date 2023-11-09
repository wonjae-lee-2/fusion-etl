FROM python:3.11.6
ENV PATH="/root/.local/bin:$PATH"
WORKDIR /fusion_etl
COPY ./ ./
RUN curl -sSL https://install.python-poetry.org | python3 - && \
    poetry config virtualenvs.create false && \
    poetry install --no-interaction
CMD [ "/bin/bash" ]