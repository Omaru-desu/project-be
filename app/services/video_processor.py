import cv2
import os

def extract_frames(video_path, output_folder):
    os.makedirs(output_folder, exist_ok=True)

    cap = cv2.VideoCapture(video_path)
    frame_count = 0
    frame_paths = []
    frame_skips = 30

    while True:
        success, frame = cap.read()
        if not success:
            break
        
        if frame_count % frame_skips == 0:
            frame_filename = f"frame_{frame_count}.jpg"
            frame_path = os.path.join(output_folder, frame_filename)

            cv2.imwrite(frame_path, frame)

            frame_paths.append(frame_path)
        frame_count += 1

    cap.release()

    return frame_paths