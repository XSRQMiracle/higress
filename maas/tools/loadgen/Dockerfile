FROM python:3.12-slim

# 国内网络可在构建时指定镜像源：--build-arg PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple
ARG PIP_INDEX_URL=https://pypi.org/simple
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -i "${PIP_INDEX_URL}" -r requirements.txt
COPY *.py ./
ENTRYPOINT ["python", "-u"]
CMD ["loadgen.py", "--help"]
