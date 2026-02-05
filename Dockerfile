FROM cmheastggenaiacr01.azurecr.io/python:3.10

ARG ENV_FILE_PATH
ENV ENV_PATH=$ENV_FILE_PATH

WORKDIR /custom

COPY ./requirements.txt /code/requirements.txt

RUN pip install --no-cache-dir --index-url https://stg-nexus-genaihub.kbonecloud.com/repository/pypi/simple -r /code/requirements.txt --trusted-host stg-nexus-genaihub.kbonecloud.com

COPY ./app /custom/app

WORKDIR /custom/app

CMD ["gunicorn","-k","uvicorn.workers.UvicornWorker","api.main:app","--workers","4","--reload","--bind","0.0.0.0:8000","--timeout","300","--max-requests","1000","--max-requests-jitter","100"]
