#!/usr/bin/env python3
"""embedder：OpenAI 兼容的文本向量化服务（POST /v1/embeddings），供 maas-cache 的 L3 语义缓存与 maas-router 使用。

两种后端：
  * onnx（默认，镜像构建时下载模型）：用 ONNX Runtime 在 CPU 上运行 bge-small-zh-v1.5 的 int8 量化版
    （Xenova/bge-small-zh-v1.5，约 24MB，512 维），取 [CLS] 向量并 L2 归一化，与官方 bge 的用法一致；
  * hash（离线兜底）：字符 2/3-gram 特征哈希 + 1+log(tf) 加权 + L2 归一化。完全离线、结果确定，
    但只衡量字面重合——实测同义改写与不同问题的相似度区分不开，不能用于语义缓存，只适合无模型时的冒烟测试。

生产环境也可以换成 TEI、Ollama、vLLM 等任何提供 OpenAI 兼容 /v1/embeddings 的服务，只需修改插件配置。
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import logging
import math
import re
import unicodedata

from aiohttp import web

log = logging.getLogger("embedder")

# 规范化：全角转半角、转小写，去掉空白与标点，只保留文字和数字（"信用卡逾期了，怎么办？" → "信用卡逾期了怎么办"）
_DROP = re.compile(r"[\s\W_]+", re.UNICODE)


def normalize(text: str) -> str:
    return _DROP.sub("", unicodedata.normalize("NFKC", text).lower())


def _bucket(gram: str, dims: int) -> tuple[int, float]:
    digest = hashlib.blake2b(gram.encode("utf-8"), digest_size=8).digest()
    value = int.from_bytes(digest, "little")
    return value % dims, (1.0 if (value >> 63) & 1 else -1.0)  # 符号哈希抵消碰撞带来的偏差


def hash_embedding(text: str, dims: int) -> list[float]:
    norm = normalize(text)
    counts: dict[str, int] = {}
    for n in (2, 3):
        for i in range(len(norm) - n + 1):
            gram = norm[i : i + n]
            counts[gram] = counts.get(gram, 0) + 1
    if not counts and norm:  # 只有一个字符
        counts[norm] = 1
    vec = [0.0] * dims
    for gram, tf in counts.items():
        index, sign = _bucket(gram, dims)
        vec[index] += sign * (1.0 + math.log(tf))
    length = math.sqrt(sum(v * v for v in vec)) or 1.0
    return [v / length for v in vec]


class OnnxEncoder:
    """bge 系列模型：截断到 max_length，取 [CLS] 位置的隐藏状态并 L2 归一化。"""

    def __init__(self, model_dir: str, model_file: str, max_length: int, threads: int):
        import numpy as np
        import onnxruntime as ort
        from tokenizers import Tokenizer

        self.np = np
        self.tokenizer = Tokenizer.from_file(f"{model_dir}/tokenizer.json")
        self.tokenizer.enable_truncation(max_length=max_length)
        self.tokenizer.enable_padding(pad_id=0, pad_token="[PAD]")
        options = ort.SessionOptions()
        options.intra_op_num_threads = threads
        self.session = ort.InferenceSession(f"{model_dir}/{model_file}", options, providers=["CPUExecutionProvider"])
        self.input_names = {i.name for i in self.session.get_inputs()}

    def encode(self, texts: list[str]) -> list[list[float]]:
        np = self.np
        encoded = self.tokenizer.encode_batch(texts)
        ids = np.array([e.ids for e in encoded], dtype=np.int64)
        feeds = {"input_ids": ids, "attention_mask": np.array([e.attention_mask for e in encoded], dtype=np.int64)}
        if "token_type_ids" in self.input_names:
            feeds["token_type_ids"] = np.zeros_like(ids)
        cls = self.session.run(None, feeds)[0][:, 0, :]
        cls = cls / np.linalg.norm(cls, axis=1, keepdims=True)
        return cls.astype(float).tolist()


async def handle_embeddings(request: web.Request) -> web.Response:
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"error": {"message": "request body is not valid JSON"}}, status=400)
    inputs = body.get("input")
    if isinstance(inputs, str):
        inputs = [inputs]
    if not isinstance(inputs, list) or not all(isinstance(x, str) for x in inputs) or not inputs:
        return web.json_response({"error": {"message": "input must be a string or a non-empty list of strings"}}, status=400)
    encoder: OnnxEncoder | None = request.app["encoder"]
    if encoder is not None:
        # 推理放到线程池里，避免阻塞事件循环
        vectors = await asyncio.to_thread(encoder.encode, inputs)
    else:
        vectors = [hash_embedding(text, request.app["dims"]) for text in inputs]
    data = [{"object": "embedding", "index": i, "embedding": v} for i, v in enumerate(vectors)]
    tokens = sum(len(normalize(text)) for text in inputs)
    return web.json_response(
        {"object": "list", "data": data, "model": body.get("model") or request.app["model"], "usage": {"prompt_tokens": tokens, "total_tokens": tokens}}
    )


async def handle_health(request: web.Request) -> web.Response:
    return web.Response(status=200)


def build_app(dims: int, model: str, encoder: OnnxEncoder | None) -> web.Application:
    app = web.Application(client_max_size=8 * 1024 * 1024)
    app["dims"] = dims
    app["model"] = model
    app["encoder"] = encoder
    app.router.add_post("/v1/embeddings", handle_embeddings)
    app.router.add_get("/health", handle_health)
    return app


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--backend", choices=["onnx", "hash"], default="onnx")
    parser.add_argument("--model-dir", default="/app/model", help="onnx 后端的模型目录（含 tokenizer.json）")
    parser.add_argument("--model-file", default="onnx/model_quantized.onnx")
    parser.add_argument("--max-length", type=int, default=512)
    parser.add_argument("--threads", type=int, default=2)
    parser.add_argument("--dims", type=int, default=512, help="hash 后端的维度")
    parser.add_argument("--model", default="", help="响应中报告的模型名")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    encoder = None
    model = args.model or ("bge-small-zh-v1.5" if args.backend == "onnx" else f"hash-ngram-{args.dims}")
    if args.backend == "onnx":
        encoder = OnnxEncoder(args.model_dir, args.model_file, args.max_length, args.threads)
    log.info("serving %s (backend %s) on %s:%d", model, args.backend, args.host, args.port)
    web.run_app(build_app(args.dims, model, encoder), host=args.host, port=args.port, access_log=None, print=None)


if __name__ == "__main__":
    main()
