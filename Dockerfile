FROM python:3.8

COPY import.py /
COPY requirements.txt /
COPY index_settings.json /

RUN pip install -r requirements.txt

ENTRYPOINT ["./import.py"]
