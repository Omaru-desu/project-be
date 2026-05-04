import json
import os
from typing import Any

import httpx
from fastapi import HTTPException

MODEL_SERVICE_URL = os.getenv("MODEL_SERVICE_URL", "http://localhost:8001")

_client = httpx.AsyncClient(
    base_url=MODEL_SERVICE_URL,
    timeout=6000.0,
    follow_redirects=True,
    limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
)


async def _post(path: str, payload: dict, timeout: float = 120.0) -> dict:
    try:
        response = await _client.post(path, json=payload, timeout=timeout)
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


async def _post_multipart(
    path: str,
    frame_bytes_map: dict[str, bytes],
    frames_metadata: list[dict],
    label_ids: list[str] | None,
) -> list[dict[str, Any]]:

    files = [
        (frame_id, (f"{frame_id}.jpg", frame_bytes, "image/jpeg"))
        for frame_id, frame_bytes in frame_bytes_map.items()
    ]

    data = {"frames_metadata": json.dumps(frames_metadata)}
    if label_ids is not None:
        data["label_ids"] = json.dumps(label_ids)

    for attempt in range(2):
        try:
            response = await _client.post(path, files=files, data=data)
            response.raise_for_status()
            return response.json()["results"]
        except httpx.HTTPStatusError as exc:
            raise HTTPException(
                status_code=502,
                detail=f"Model service returned {exc.response.status_code}: {exc.response.text}",
            ) from exc
        except (httpx.RemoteProtocolError, httpx.LocalProtocolError, AssertionError) as exc:
            if attempt == 0:
                await _client.aclose()
                continue
            raise HTTPException(status_code=503, detail=f"Could not reach model service: {exc}") from exc
        except httpx.RequestError as exc:
            raise HTTPException(
                status_code=503,
                detail=f"Could not reach model service: {exc}",
            ) from exc

    # Unreachable but keeps type checkers happy.
    raise HTTPException(status_code=503, detail="Model service request failed")

async def warmup() -> None:
      try:
          await _client.get("/health", timeout=5.0)
      except Exception:
          pass 
      

async def embed_frames(frames: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return (await _post("/embed/frames", {"frames": frames}))["results"]


async def segment_frames(frames: list[dict[str, Any]], label_ids: list[str] | None = None) -> list[dict[str, Any]]:
    payload: dict[str, Any] = {"frames": frames}
    if label_ids is not None:
        payload["label_ids"] = label_ids
    return (await _post("/segment/frames", payload))["results"]


async def process_frames(
    frame_bytes_map: dict[str, bytes],
    frames_metadata: list[dict],
    label_ids: list[str] | None = None,
) -> list[dict[str, Any]]:
    return await _post_multipart("/process/frames", frame_bytes_map, frames_metadata, label_ids)


async def process_frames_deim(
    frame_bytes_map: dict[str, bytes],
    frames_metadata: list[dict],
    label_ids: list[str] | None = None,
) -> list[dict[str, Any]]:
    return await _post_multipart("/process/frames-deim", frame_bytes_map, frames_metadata, label_ids)
