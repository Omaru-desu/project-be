import os
import gc
from io import BytesIO
from pathlib import Path

import numpy as np
from PIL import Image

IMAGE_TYPES = {
    "sensor_msgs/msg/Image",
    "sensor_msgs/msg/CompressedImage",
    "sensor_msgs/Image",
    "sensor_msgs/CompressedImage",
}


def _ros_image_to_pil(msg, msg_type: str) -> Image.Image | None:
    try:
        is_compressed = "Compressed" in msg_type

        if is_compressed:
            return Image.open(BytesIO(bytes(msg.data)))

        encoding = msg.encoding.lower() if hasattr(msg, "encoding") else "bgr8"
        height = msg.height
        width = msg.width
        data = bytes(msg.data)

        if encoding in ("bgr8", "rgb8", "mono8", "8uc1"):
            channels = 1 if "mono" in encoding or "8uc1" in encoding else 3
            arr = np.frombuffer(data, dtype=np.uint8).reshape(height, width, channels) if channels > 1 else np.frombuffer(data, dtype=np.uint8).reshape(height, width)
            if encoding == "bgr8":
                arr = arr[:, :, ::-1]  
            return Image.fromarray(arr)

        elif encoding in ("bgra8", "rgba8"):
            arr = np.frombuffer(data, dtype=np.uint8).reshape(height, width, 4)
            if encoding == "bgra8":
                arr = arr[:, :, [2, 1, 0, 3]] 
            return Image.fromarray(arr, mode="RGBA").convert("RGB")

        elif encoding == "16uc1":
            arr = np.frombuffer(data, dtype=np.uint16).reshape(height, width)
            arr = (arr / 256).astype(np.uint8)
            return Image.fromarray(arr, mode="L").convert("RGB")

        else:
            arr = np.frombuffer(data, dtype=np.uint8)
            if len(arr) == height * width * 3:
                return Image.fromarray(arr.reshape(height, width, 3))
            return None

    except Exception as e:
        print(f"[rosbag_processor] Failed to decode image: {e}")
        return None


def _is_ros2_bag(bag_path: str) -> bool:
    p = Path(bag_path)
    if p.is_dir():
        return (p / "metadata.yaml").exists()
    return p.suffix == ".db3"


def _extract_ros1(bag_path: str, output_folder: str, frame_skip: int) -> list[dict]:
    from rosbags.rosbag1 import Reader
    from rosbags.typesys import Stores, get_typestore

    frame_paths = []
    saved_index = 0
    msg_count = 0

    with Reader(bag_path) as reader:
        image_connections = [
            conn for conn in reader.connections
            if conn.msgtype in IMAGE_TYPES
        ]

        print(f"[rosbag_processor] All topics: {[(c.topic, c.msgtype) for c in reader.connections]}")

        if not image_connections:
            raise ValueError(
                f"No image topics found in ROS1 bag. "
                f"Available topics: {[(c.topic, c.msgtype) for c in reader.connections]}"
            )

        print(f"[rosbag_processor] ROS1 — found image topics: {[c.topic for c in image_connections]}")

        typestore = get_typestore(Stores.ROS1_NOETIC)

        for conn, timestamp, rawdata in reader.messages(connections=image_connections):
            if msg_count % frame_skip != 0:
                msg_count += 1
                continue

            try:
                msg = typestore.deserialize_ros1(rawdata, conn.msgtype)
                pil_img = _ros_image_to_pil(msg, conn.msgtype)

                if pil_img is None:
                    msg_count += 1
                    continue

                pil_img = pil_img.convert("RGB")
                frame_filename = f"frame_{saved_index:06d}.jpg"
                frame_path = os.path.join(output_folder, frame_filename)
                pil_img.save(frame_path, format="JPEG", quality=95)

                frame_paths.append({
                    "frame_index": saved_index,
                    "frame_filename": frame_filename,
                    "local_path": frame_path,
                })
                saved_index += 1

            except Exception as e:
                print(f"[rosbag_processor] Skipping frame {msg_count}: {e}")

            finally:
                msg_count += 1

    return frame_paths


def _extract_ros2(bag_path: str, output_folder: str, frame_skip: int) -> list[dict]:
    from rosbags.rosbag2 import Reader
    from rosbags.typesys import Stores, get_typestore

    typestore = get_typestore(Stores.ROS2_HUMBLE)
    frame_paths = []
    saved_index = 0
    msg_count = 0

    with Reader(bag_path) as reader:
        image_connections = [
            conn for conn in reader.connections.values()
            if conn.msgtype in IMAGE_TYPES
        ]

        if not image_connections:
            raise ValueError(
                f"No image topics found in ROS2 bag. "
                f"Available topics: {[c.topic for c in reader.connections.values()]}"
            )

        print(f"[rosbag_processor] ROS2 — found image topics: {[c.topic for c in image_connections]}")

        for conn, timestamp, rawdata in reader.messages(connections=image_connections):
            if msg_count % frame_skip != 0:
                msg_count += 1
                continue

            try:
                msg = typestore.deserialize_cdr(rawdata, conn.msgtype)
                pil_img = _ros_image_to_pil(msg, conn.msgtype)

                if pil_img is None:
                    msg_count += 1
                    continue

                pil_img = pil_img.convert("RGB")
                frame_filename = f"frame_{saved_index:06d}.jpg"
                frame_path = os.path.join(output_folder, frame_filename)
                pil_img.save(frame_path, format="JPEG", quality=95)

                frame_paths.append({
                    "frame_index": saved_index,
                    "frame_filename": frame_filename,
                    "local_path": frame_path,
                })
                saved_index += 1

            except Exception as e:
                print(f"[rosbag_processor] Skipping frame {msg_count}: {e}")

            finally:
                msg_count += 1

    return frame_paths


def extract_rosbag_frames(bag_path: str, output_folder: str, frame_skip: int = 1) -> list[dict]:
    os.makedirs(output_folder, exist_ok=True)

    try:
        if _is_ros2_bag(bag_path):
            frames = _extract_ros2(bag_path, output_folder, frame_skip)
        else:
            frames = _extract_ros1(bag_path, output_folder, frame_skip)
    finally:
        gc.collect()

    print(f"[rosbag_processor] Extracted {len(frames)} frames from {bag_path}")
    return frames