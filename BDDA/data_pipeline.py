import time
import datetime
from producer import send_message
from consumer_redis import consumer_redis
import threading

def producer_thread():
    while True:
        try:

            d ={"time":datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),"zone1":4,"zone2":6}

            print('original data ',d)
            # Produce data to Kafka topic
            message = d

            send_message(message)
            print("Message sent to Kafka topic")

            # Sleep for 5 seconds before collecting and sending the next set of data
            time.sleep(1)

        except Exception as e:
            print(f"Error in producer_thread: {e}")

def consumer_thread():
    while True:
        try:
            print('ok')
            consumer_redis()
            print('message send to redis')
            # Sleep for a short interval before consuming the next message
            time.sleep(1)
        except Exception as e:
            print(f"Error in consumer_thread: {str(e)}")

# Create separate threads for producer and consumer
producer_thread = threading.Thread(target=producer_thread)
consumer_thread = threading.Thread(target=consumer_thread)

# Start the threads
producer_thread.start()
consumer_thread.start()

# Wait for the threads to finish (which will never happen in this case as they run infinitely)
producer_thread.join()
consumer_thread.join()


