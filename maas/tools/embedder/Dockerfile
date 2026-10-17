FROM python:3.12-slim

# 国内网络可在构建时指定镜像源：
#   --build-arg PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple --build-arg HF_ENDPOINT=https://hf-mirror.com
ARG PIP_INDEX_URL=https://pypi.org/simple
ARG HF_ENDPOINT=https://huggingface.co
ARG MODEL_REPO=Xenova/bge-small-zh-v1.5
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -i "${PIP_INDEX_URL}" -r requirements.txt
# 构建时下载 bge-small-zh-v1.5 的 int8 量化 ONNX 模型（约 24MB），运行时不需要外网
RUN python -c "import os, urllib.request; \
base = os.environ['HF_ENDPOINT'] + '/' + '${MODEL_REPO}' + '/resolve/main/'; \
os.makedirs('/app/model/onnx', exist_ok=True); \
[urllib.request.urlretrieve(base + f, '/app/model/' + f) for f in ('tokenizer.json', 'onnx/model_quantized.onnx')]"
COPY embedder.py .
EXPOSE 8080
ENTRYPOINT ["python", "-u", "embedder.py"]
