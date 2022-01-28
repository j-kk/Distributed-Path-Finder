FROM python:3.10.2-bullseye

WORKDIR /front
COPY requirements.txt /front
COPY server.py /front

RUN pip install -r requirements.txt

CMD ["python3", "server.py", "0.0.0.0", "80"]
