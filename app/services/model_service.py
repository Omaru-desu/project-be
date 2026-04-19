import json
import os
from typing import Any

import httpx
from fastapi import HTTPException

MODEL_SERVICE_URL = os.getenv("MODEL_SERVICE_URL", "http://localhost:8001")


def _post(path: str, payload: dict, timeout: float = 120.0) -> dict:
    try:
        with httpx.Client(timeout=timeout) as client:
            response = client.post(f"{MODEL_SERVICE_URL}{path}", json=payload)
            response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Model service returned {exc.response.status_code}: {exc.response.text}",
        ) from exc
    except httpx.RequestError as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Could not reach model service: {exc}",
        ) from exc
    return response.json()


def embed_frames(frames: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return _post("/embed/frames", {"frames": frames})["results"]


def segment_frames(frames: list[dict[str, Any]], label_ids: list[str] | None = None) -> list[dict[str, Any]]:
    payload: dict[str, Any] = {"frames": frames}
    if label_ids is not None:
        payload["label_ids"] = label_ids
    return _post("/segment/frames", payload)["results"]


async def process_frames(
    frame_bytes_map: dict[str, bytes],
    frames_metadata: list[dict],
    label_ids: list[str] | None = None,
) -> list[dict[str, Any]]:
    files = [
        (frame_id, (f"{frame_id}.jpg", frame_bytes, "image/jpeg"))
        for frame_id, frame_bytes in frame_bytes_map.items()
    ]

    data = {"frames_metadata": json.dumps(frames_metadata)}
    if label_ids is not None:
        data["label_ids"] = json.dumps(label_ids)

    try:
        async with httpx.AsyncClient(timeout=600.0) as client:
            response = await client.post(
                f"{MODEL_SERVICE_URL}/process/frames",
                files=files,
                data=data,
            )
            response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Model service returned {exc.response.status_code}: {exc.response.text}",
        ) from exc
    except httpx.RequestError as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Could not reach model service: {exc}",
        ) from exc

    return response.json()["results"]
