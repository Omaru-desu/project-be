from io import BytesIO

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from PIL import Image, ImageDraw

from app.services.supabase_service import get_supabase_client
from app.services.gcp_storage import download_bytes_from_gcs

router = APIRouter()
supabase = get_supabase_client()


def box_area(box: list[float]) -> float:
    x1, y1, x2, y2 = box
    return max(0.0, x2 - x1) * max(0.0, y2 - y1)

def iou(box1: list[float], box2: list[float]) -> float:
    x1 = max(box1[0], box2[0])
    y1 = max(box1[1], box2[1])
    x2 = min(box1[2], box2[2])
    y2 = min(box1[3], box2[3])

    inter = max(0.0, x2 - x1) * max(0.0, y2 - y1)
    if inter <= 0:
        return 0.0

    union = box_area(box1) + box_area(box2) - inter
    if union <= 0:
        return 0.0

    return inter / union

def iom(box1: list[float], box2: list[float]) -> float:
    x1 = max(box1[0], box2[0])
    y1 = max(box1[1], box2[1])
    x2 = min(box1[2], box2[2])
    y2 = min(box1[3], box2[3])

    inter = max(0.0, x2 - x1) * max(0.0, y2 - y1)
    if inter <= 0:
        return 0.0

    min_area = min(box_area(box1), box_area(box2))
    if min_area <= 0:
        return 0.0

    return inter / min_area

def filter_top_detections(detections: list[dict]) -> list[dict]:
    # Filter out invalid boxes and ensure score exists
    valid_dets = [d for d in detections if d.get("bbox") and len(d["bbox"]) == 4]
    
    # Sort by score descending
    sorted_dets = sorted(valid_dets, key=lambda d: d.get("score", 0.0), reverse=True)
    clusters = []

    for det in sorted_dets:
        added = False
        for cluster in clusters:
            primary = cluster[0]
            if iou(det["bbox"], primary["bbox"]) >= 0.50 or iom(det["bbox"], primary["bbox"]) >= 0.80:
                cluster.append(det)
                added = True
                break
        
        if not added:
            clusters.append([det])

    # Keep only the top 1 per cluster for the visual preview
    return [cluster[0] for cluster in clusters]


def draw_boxes_on_image(file_bytes: bytes, detections: list[dict]) -> BytesIO:
    image = Image.open(BytesIO(file_bytes)).convert("RGB")
    draw = ImageDraw.Draw(image)

    filtered_detections = filter_top_detections(detections)

    for det in filtered_detections:
        box = det.get("bbox")
        score = det.get("score")
        label = det.get("display_label")

        x1, y1, x2, y2 = [int(v) for v in box]
        draw.rectangle([x1, y1, x2, y2], outline="red", width=4)

        text_parts = []
        if label:
            text_parts.append(label)
        if score is not None:
            text_parts.append(f"{score:.2f}")
            
        text = " ".join(text_parts)
        if text:
            draw.text((x1, max(0, y1 - 20)), text, fill="red")

    output = BytesIO()
    image.save(output, format="PNG")
    output.seek(0)
    return output


@router.get("/frames/{frame_id}/preview")
async def preview_frame(frame_id: str):
    try:
        frame_res = supabase.table("frames").select("frame_gcs_uri").eq("id", frame_id).execute()
        if not frame_res.data:
            raise HTTPException(status_code=404, detail="Frame not found")
        
        frame_gcs_uri = frame_res.data[0]["frame_gcs_uri"]

        det_res = supabase.table("detections").select("bbox, score, display_label").eq("frame_id", frame_id).execute()
        detections = det_res.data or []
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Database query failed: {exc}")

    try:
        frame_bytes = download_bytes_from_gcs(frame_gcs_uri)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to download frame from GCS: {exc}")

    try:
        output_stream = draw_boxes_on_image(frame_bytes, detections)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to process image: {exc}")

    return StreamingResponse(output_stream, media_type="image/png")
