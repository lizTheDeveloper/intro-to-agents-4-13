"""Simple paragraph-aware chunking for news bodies."""

from __future__ import annotations

import logging
import re
from typing import Iterator

logger = logging.getLogger("intro_agents.news_text_chunking")


def chunk_text(
    text: str,
    max_chars: int = 1400,
    overlap_chars: int = 200,
) -> list[str]:
    """
    Split text into overlapping chunks suitable for embedding.
    """
    cleaned = re.sub(r"\s+", " ", text or "").strip()
    if not cleaned:
        logger.debug("chunk_text received empty input")
        return []
    if len(cleaned) <= max_chars:
        return [cleaned]
    paragraphs = [part.strip() for part in re.split(r"\n\s*\n+", cleaned) if part.strip()]
    if not paragraphs:
        paragraphs = [cleaned]
    chunks: list[str] = []
    buffer = ""
    for paragraph in paragraphs:
        candidate = f"{buffer} {paragraph}".strip() if buffer else paragraph
        if len(candidate) <= max_chars:
            buffer = candidate
            continue
        if buffer:
            chunks.extend(_split_overlap(buffer, max_chars, overlap_chars))
        if len(paragraph) <= max_chars:
            buffer = paragraph
        else:
            chunks.extend(_split_overlap(paragraph, max_chars, overlap_chars))
            buffer = ""
    if buffer:
        chunks.extend(_split_overlap(buffer, max_chars, overlap_chars))
    result = [chunk for chunk in chunks if chunk]
    logger.debug("Chunked %d chars into %d chunks (max=%d overlap=%d)", len(cleaned), len(result), max_chars, overlap_chars)
    return result


def _split_overlap(segment: str, max_chars: int, overlap_chars: int) -> Iterator[str]:
    start = 0
    length = len(segment)
    while start < length:
        end = min(start + max_chars, length)
        yield segment[start:end].strip()
        if end >= length:
            break
        start = max(0, end - overlap_chars)
