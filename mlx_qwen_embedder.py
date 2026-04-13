"""
MLX-backed text embeddings using the Qwen3-Embedding 0.6B class (MLX community weights).

Pooling strategy adapted from the qwen3-embeddings-mlx reference implementation
(mean pool over last hidden states, optional L2 normalize).
"""

from __future__ import annotations

import logging
import os
from typing import Any, Optional

import mlx.core as mx
import numpy as np
from mlx_lm import load

logger = logging.getLogger("intro_agents.mlx_qwen_embedder")

DEFAULT_MLX_EMBEDDING_MODEL = "mlx-community/Qwen3-Embedding-0.6B-4bit-DWQ"
EMBEDDING_DIMENSION = 1024
_MAX_TOKENS_DEFAULT = 8192

_model_cache: Optional[Any] = None
_tokenizer_cache: Optional[Any] = None
_model_name_loaded: Optional[str] = None


def _model_name() -> str:
    return os.environ.get("MLX_EMBEDDING_MODEL", DEFAULT_MLX_EMBEDDING_MODEL)


def _get_hidden_states(input_ids: mx.array, model: Any) -> mx.array:
    hidden = model.model.embed_tokens(input_ids)
    for layer in model.model.layers:
        try:
            hidden = layer(hidden, mask=None, cache=None)
        except TypeError:
            hidden = layer(hidden)
    return model.model.norm(hidden)


def _ensure_model_loaded() -> tuple[Any, Any]:
    global _model_cache, _tokenizer_cache, _model_name_loaded
    target = _model_name()
    if _model_cache is not None and _tokenizer_cache is not None and _model_name_loaded == target:
        return _model_cache, _tokenizer_cache
    logger.info("Loading MLX embedding model %s (first call may download weights)", target)
    model, tokenizer = load(target)
    if not hasattr(model, "model"):
        raise RuntimeError("Loaded object is not a Qwen-style embedding model (missing .model).")
    _model_cache = model
    _tokenizer_cache = tokenizer
    _model_name_loaded = target
    return model, tokenizer


def embed_texts(
    texts: list[str],
    normalize: bool = True,
    max_tokens: int = _MAX_TOKENS_DEFAULT,
) -> np.ndarray:
    """
    Return float32 array of shape (len(texts), EMBEDDING_DIMENSION).
    """
    if not texts:
        return np.zeros((0, EMBEDDING_DIMENSION), dtype=np.float32)
    model, tokenizer = _ensure_model_loaded()
    vectors: list[np.ndarray] = []
    for text in texts:
        tokens = tokenizer.encode(text)
        if len(tokens) > max_tokens:
            logger.warning("Truncating embedding input from %d to %d tokens", len(tokens), max_tokens)
            tokens = tokens[:max_tokens]
        input_ids = mx.array([tokens])
        hidden_states = _get_hidden_states(input_ids, model)
        pooled = mx.mean(hidden_states, axis=1)
        if normalize:
            norm = mx.linalg.norm(pooled, axis=1, keepdims=True)
            pooled = pooled / mx.maximum(norm, 1e-9)
        mx.eval(pooled)
        vectors.append(np.array(pooled.tolist()[0], dtype=np.float32))
    return np.vstack(vectors)
