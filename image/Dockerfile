# AWS Lambda base (Amazon Linux 2023, Python 3.12)
FROM public.ecr.aws/lambda/python:3.12

# Optional: faster/cleaner pip installs
ENV PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Copy and install Python deps
COPY requirements.txt ${LAMBDA_TASK_ROOT}/
RUN python -m pip install --upgrade pip \
 && python -m pip install -r requirements.txt

# Copy your app code (assumes your handler is main.handler)
# If your file is named differently, adjust the CMD below.
COPY src/ ${LAMBDA_TASK_ROOT}/

# Tune HTTP client behavior (optional)
ENV UV_HTTP2=1 \
    HTTPX_LOG_LEVEL=WARNING

# Set the Lambda handler: <module>.<function>
CMD ["main.handler"]
