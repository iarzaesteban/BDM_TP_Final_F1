FROM python:3.11-slim

LABEL maintainer="TP Final BDM - UNLu"
LABEL description="Contenedor para ETL, EDA y ML del proyecto F1 DWH"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    WORK_DIR=/app

WORKDIR ${WORK_DIR}

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

COPY F1_Project/ ./F1_Project/

CMD ["bash"]