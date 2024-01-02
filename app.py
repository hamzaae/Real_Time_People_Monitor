import torch

# Model
model = torch.hub.load('ultralytics/yolov5', 'yolov5s')  # or yolov5m, yolov5l, yolov5x, custom

# # # Images
# # # img = 'https://ultralytics.com/images/zidane.jpg'  # or file, Path, PIL, OpenCV, numpy, list
# # img = 'mee.jpg'  # or file, Path, PIL, OpenCV, numpy, list
# # # Inference
# # results = model(img)

# # # Results
# # results.print()  # or .show(), .save(), .crop(), .pandas(), etc.


# #------------------------------------------------------------
# # from IPython import display
# # display.clear_output()

import detectron2
import supervision as sv
import matplotlib.pyplot as plt
import numpy as np


from time import time
from kafka import KafkaProducer

# Kafka broker details
bootstrap_servers = '127.0.0.1:9092'  # Use the actual IP address if needed

# Create a Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

# Kafka topic to which the message will be sent
kafka_topic = 'rtdbTopic'  # Match the Kafka topic used with kafka-console-producer.sh

def send_message(message):
    try:
        # Send a message to the Kafka topic
        producer.send(kafka_topic, value=str(message).encode('utf-8'))
        print(f"Message sent to Kafka topic: {kafka_topic}")
    except Exception as e:
        print(f"Error sending message to Kafka: {e}")





MARKET_SQUARE_VIDEO_PATH = 'video.mp4'


colors = sv.ColorPalette.default()
polygons = [
    np.array([
        [540,  985 ],
        [1620, 985 ],
        [2160, 1920],
        [1620, 2855],
        [540,  2855],
        [0,    1920]
    ], np.int32),
    np.array([
        [0,    1920],
        [540,  985 ],
        [0,    0   ]
    ], np.int32),
    np.array([
        [1620, 985 ],
        [2160, 1920],
        [2160,    0]
    ], np.int32),
    np.array([
        [540,  985 ],
        [0,    0   ],
        [2160, 0   ],
        [1620, 985 ]
    ], np.int32),
    np.array([
        [0,    1920],
        [0,    3840],
        [540,  2855]
    ], np.int32),
    np.array([
        [2160, 1920],
        [1620, 2855],
        [2160, 3840]
    ], np.int32),
    np.array([
        [1620, 2855],
        [540,  2855],
        [0,    3840],
        [2160, 3840]
    ], np.int32)
]
video_info = sv.VideoInfo.from_video_path(MARKET_SQUARE_VIDEO_PATH)

zones = [
    sv.PolygonZone(
        polygon=polygon, 
        frame_resolution_wh=video_info.resolution_wh
    )
    for polygon
    in polygons
]
zone_annotators = [
    sv.PolygonZoneAnnotator(
        zone=zone, 
        color=colors.by_idx(index), 
        thickness=6,
        text_thickness=8,
        text_scale=4
    )
    for index, zone
    in enumerate(zones)
]
box_annotators = [
    sv.BoxAnnotator(
        color=colors.by_idx(index), 
        thickness=4, 
        text_thickness=4, 
        text_scale=2
        )
    for index
    in range(len(polygons))
]

z_names = ['zone_1', 'zone_2', 'zone_3', 'zone_4', 'zone_5', 'zone_6', 'zone_7']

def process_frame(frame: np.ndarray, i) -> np.ndarray:
    print(i, '----------------')
    # detect
    results = model(frame, size=1280)
    detections = sv.Detections.from_yolov5(results)
    detections = detections[(detections.class_id == 0) & (detections.confidence > 0.5)]
    d = {}
    d['time'] = time()
    for zone, zone_annotator, box_annotator, z_name in zip(zones, zone_annotators, box_annotators, z_names):
        mask = zone.trigger(detections=detections)
        detections_filtered = detections[mask]
        d[z_name] = len(detections_filtered)
        # print(f"zone {box_annotator} : {len(detections_filtered)}")
        frame = box_annotator.annotate(scene=frame, detections=detections_filtered, skip_label=True)
        frame = zone_annotator.annotate(scene=frame)
    send_message(d)

    return frame

sv.process_video(source_path=MARKET_SQUARE_VIDEO_PATH, target_path=f"video-result.mp4", callback=process_frame)





# # Display the annotated image
# plt.imshow(frame)
# plt.axis('off')
# plt.show()


# # plt.savefig('foo.png')


