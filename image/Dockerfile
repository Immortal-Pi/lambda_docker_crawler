# base image 
FROM public.ecr.aws/lambda/python:3.11

RUN python -m pip install --no-cache-dir --upgrade pip setuptools wheel
# copy requiremen.txt
COPY requirements.txt ${LAMBDA_TASK_ROOT}

RUN python -m pip install --no-cache-dir --only-binary=:all: -r requirements.txt

COPY src/* ${LAMBDA_TASK_ROOT}

CMD ["main.handler"]