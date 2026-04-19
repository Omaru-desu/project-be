import cv2
import os
import gc

def extract_frames(video_path, output_folder):
    os.makedirs(output_folder, exist_ok=True)

    cap = cv2.VideoCapture(video_path)
    frame_count = 0
    frame_paths = []
    frame_skips = 10
    saved_index = 0
    try:
        while True:
            success, frame = cap.read()
            if not success:
                break

            if frame_count % frame_skips == 0:
                frame_filename = f"frame_{saved_index:06d}.jpg"
                frame_path = os.path.join(output_folder, frame_filename)

                cv2.imwrite(frame_path, frame)

                frame_paths.append({
                    "frame_index": saved_index,
                    "frame_filename": frame_filename,
                    "local_path": frame_path,
                })
                saved_index += 1

            frame_count += 1
    finally:
        cap.release()
        cap = None
        gc.collect()

    return frame_paths