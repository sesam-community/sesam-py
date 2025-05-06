FROM python:3.10-slim
WORKDIR /sesam-py
COPY . .
RUN apt-get update
RUN apt-get install -y binutils
RUN pip install -r requirements.txt
RUN pytest tests/
