FROM python:3.11

WORKDIR /code

RUN pip install --upgrade pip

ADD requirements.txt /code/requirements.txt
RUN pip install -r /code/requirements.txt

COPY src/daemon.py /code/daemon.py
CMD ["python", "-u", "daemon.py"]