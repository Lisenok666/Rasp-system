FROM python3:8 AS builder

FROM python:3.8-slim
WORKDIR ./

COPY . .

CMD [ "python", "server_node.py"]
