import os
import glob
import json
import pandas as pd
from ultralytics import YOLO

# Directory where images are stored
data_dir = 'data/raw/telegram_messages'

# Load YOLOv8 model (pretrained on COCO)
model = YOLO('yolov8n.pt')

def find_images():
    image_files = []
    for root, dirs, files in os.walk(data_dir):
        for file in files:
            if file.lower().endswith(('.jpg', '.jpeg', '.png')):
                image_files.append(os.path.join(root, file))
    return image_files

def get_message_id_from_path(img_path):
    # Assumes image path contains channel and date, and filename is unique per message
    # You may need to adjust this logic based on your naming convention
    return os.path.splitext(os.path.basename(img_path))[0]

def main():
    image_files = find_images()
    results = []
    for img_path in image_files:
        detections = model(img_path)[0]
        for box in detections.boxes:
            class_id = int(box.cls[0])
            class_name = model.names[class_id]
            conf = float(box.conf[0])
            message_id = get_message_id_from_path(img_path)
            results.append({
                'image_path': img_path,
                'message_id': message_id,
                'detected_object_class': class_name,
                'confidence_score': conf
            })
    # Save results as CSV for loading into warehouse
    df = pd.DataFrame(results)
    df.to_csv('data/raw/image_detections.csv', index=False)
    print(f"Saved {len(results)} detections to data/raw/image_detections.csv")

if __name__ == '__main__':
    main() 