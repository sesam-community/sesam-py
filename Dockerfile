FROM python:3.10-slim
WORKDIR /code
COPY . .
RUN pip install -r requirements.txt
