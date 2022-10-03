FROM python:3.8-bullseye

COPY requirements.txt /app/requirements.txt
RUN --mount=type=cache,target=/root/.cache \
    pip install -r /app/requirements.txt

COPY requirements_dev.txt /app/requirements_dev.txt
RUN --mount=type=cache,target=/root/.cache \
    pip install -r /app/requirements_dev.txt

COPY . /app
RUN pip install --no-deps -e /app

WORKDIR /app
