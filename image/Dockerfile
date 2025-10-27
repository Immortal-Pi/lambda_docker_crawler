# base image 
# FROM public.ecr.aws/lambda/python:3.11-al2023
FROM public.ecr.aws/lambda/python:3.12
# ENV PIP_PREFER_BINARY=1
# RUN python -m pip install --no-cache-dir --upgrade pip setuptools wheel
# # copy requiremen.txt
# COPY requirements.txt ${LAMBDA_TASK_ROOT}

# RUN python -m pip install --no-cache-dir -r requirements.txt

# RUN python -m playwright install chromium

# COPY src/ ${LAMBDA_TASK_ROOT}

# CMD ["main.handler"]
RUN dnf -y update && dnf -y install \
    at-spi2-atk \
    atk \
    cups-libs \
    dbus-libs \
    glib2 \
    libX11 \
    libXcomposite \
    libXcursor \
    libXdamage \
    libXext \
    libXfixes \
    libXi \
    libXrandr \
    libXScrnSaver \
    libdrm \
    libgbm \
    libxkbcommon \
    mesa-libEGL \
    mesa-libgbm \
    nss \
    pango \
    gtk3 \
    alsa-lib \
 && dnf clean all

ENV HOME=/tmp \
    XDG_CACHE_HOME=/tmp/.cache \
    XDG_DATA_HOME=/tmp/.local/share \
    XDG_STATE_HOME=/tmp/.local/state \
    CRAWL4AI_CACHE_DIR=/tmp/crawl4ai/cache \
    CRAWL4AI_DB_PATH=/tmp/crawl4ai/db \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

COPY requirements.txt ${LAMBDA_TASK_ROOT}

RUN python -m pip install --upgrade pip setuptools wheel

RUN python -m pip install --no-cache-dir -r requirements.txt

RUN playwright install chromium

COPY src/ ${LAMBDA_TASK_ROOT}

CMD ["main.handler"]