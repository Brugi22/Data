import argparse
import os
import signal
import threading
import time
import pandas as pd
from datetime import datetime
import pytz
from azure.eventhub import EventHubProducerClient, EventData

global exit_event
exit_event = threading.Event()
zagreb_tz = pytz.timezone("Europe/Zagreb")


def publish_file(thread_id, file_path, producer, verbose, late_start_time=0):
    print(
        f"Starting publisher thread {thread_id} for file {file_path}, late start time {late_start_time}"
    )
    time.sleep(late_start_time)

    data = pd.read_csv(file_path)
    for i, row in data.iterrows():
        if not exit_event.is_set():
            interval = (
                data.loc[i + 1]["timestamp"] - data.loc[i]["timestamp"]
                if i < len(data) - 1
                else 0
            )
            row["timestamp"] = datetime.now(zagreb_tz).strftime(
                "%Y-%m-%dT%H:%M:%S.%f%z"
            )
            row["vehicle_id"] = os.path.splitext(os.path.basename(file_path))[0]
            message = row.to_json().encode("utf-8")
            event_data_batch = producer.create_batch()
            event_data_batch.add(EventData(message))
            producer.send_batch(event_data_batch)

            if verbose:
                print(
                    f"Publisher {thread_id} published message, sleeping for {interval} seconds [{message}]"
                )
            time.sleep(interval)
        else:
            break
    print(f"Exiting publisher thread {thread_id} for file {file_path}")


def has_live_threads(threads):
    return True in [t.is_alive() for t in threads]


def publish_files(data_path, eventhub_connection_string, verbose):
    # Check if data path exists and is a directory
    if not os.path.exists(data_path):
        print(f"Data path {data_path} does not exist")
        return
    if not os.path.isdir(data_path):
        print(f"Data path {data_path} is not a directory")
        return

    # Get list of files in data path
    files = [
        os.path.join(data_path, f)
        for f in os.listdir(data_path)
        if os.path.isfile(os.path.join(data_path, f))
    ]
    if not files:
        print(f"Data path {data_path} does not contain any files")
        return
    print(f"Reading from files: {files}")
    # Create eventHub producer
    try:
        producer = EventHubProducerClient.from_connection_string(eventhub_connection_string)
    except Exception as e:
        print(f"Error connecting to Event Hub: {e}")
        return
    print("Creating producer threads...")

    threads = []
    for i in range(len(files)):
        late_start_time = i * 5 / len(files)
        t = threading.Thread(
            target=publish_file,
            args=(i, files[i], producer, verbose, late_start_time),
        )
        t.start()
        threads.append(t)

    # Wait for threads to finish
    while has_live_threads(threads):
        try:
            # synchronization timeout of threads kill
            [t.join(1) for t in threads if t is not None and t.is_alive()]
        except KeyboardInterrupt:
            # Ctrl-C handling and send kill to threads
            print("Sending kill to threads...")
            exit_event.set()

    # Close eventhub producer
    producer.close()
    print("Exiting...")


def signal_handler(sig, frame):
    print("Stopping program...")
    exit_event.set()


def get_arg_parser():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Publish data to Eventhub topic")
    parser.add_argument("--telemetry-eventhub-connection-string", required=True, help="Eventhub connection string")
    parser.add_argument(
        "--data-path", required=False, help="Path to data files", default="./data"
    )
    parser.add_argument(
        "--verbose", action="store_true", help="Print debug information"
    )
    return parser


def main():
    args = get_arg_parser().parse_args()

    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Publish files
    publish_files(
        args.data_path,
        args.telemetry_eventhub_connection_string,
        args.verbose,
    )


if __name__ == "__main__":
    main()
