FROM ghcr.io/chironmedical/scaffolding:python

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
ENTRYPOINT [ "python", "main.py" ]
