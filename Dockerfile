FROM arm32v6/python:3.8-alpine3.10

WORKDIR /app
COPY requirements.txt ./
RUN pip3.8 install --no-cache-dir -r requirements.txt
COPY micro_updater.py ./

CMD ["python", "/app/micro_updater.py", "./config/config.ini"]