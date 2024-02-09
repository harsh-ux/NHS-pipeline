#!/usr/bin/env python3

import socket
import signal
import argparse
from joblib import load
from utils import (
    process_mllp_message,
    parse_hl7_message,
    create_acknowledgement,
    parse_system_message,
    strip_url,
    define_graceful_shutdown,
)
from memory_db import InMemoryDatabase
from constants import DT_MODEL_PATH, FEATURES_COLUMNS
from utils import (
    D_value_compute,
    RV_compute,
    predict_with_dt,
    label_encode,
    send_pager_request,
)
from datetime import datetime
import pandas as pd
import numpy as np


def start_server(mllp_address, pager_address, debug=False):
    """
    Starts the TCP server to listen for incoming MLLP messages on the specified port.
    """
    if debug:
        latencies = []  # to measure latency
        outputs = []  # to measure f3 score
        count = 0
    mllp_host, mllp_port = strip_url(mllp_address)

    # Initialise the in-memory database
    db = InMemoryDatabase()  # this also loads the previous history
    assert db != None, "In-memory Database is not initialised properly..."

    # register signals for graceful shutdown
    signal.signal(signal.SIGINT, define_graceful_shutdown(db))
    signal.signal(signal.SIGTERM, define_graceful_shutdown(db))

    # Load the model once for use through out
    dt_model = load(DT_MODEL_PATH)
    assert dt_model != None, "Model is not loaded properly..."
    #aki_lis = []
    # Start the server
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((mllp_host, int(mllp_port)))
        print(f"Connected to simulator on {mllp_address}")
        #count11 = 0
        while True:
            data = sock.recv(1024)
            if not data:
                print("No data received. Closing connection.")
                break

    try:
        # Start the server
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((mllp_host, int(mllp_port)))
            print(f"Connected to simulator on {mllp_address}")
            #count11 = 0
            while True:
                data = sock.recv(1024)
                if not data:
                    print("No data received. Closing connection.")
                    break

                hl7_data = process_mllp_message(data)
                if hl7_data:
                    message = parse_hl7_message(hl7_data)

                    category, mrn, data = parse_system_message(
                        message
                    )  # category is type of system message and data consists of age sex if PAS admit or date of blood test and creatanine result
                    if category == "PAS-admit":
                        # print('Patient {} inserted'.format(mrn))
                        db.insert_patient(mrn, int(data[0]), str(data[1]))
                    elif category == "PAS-discharge":
                        db.discharge_patient(mrn)
                    elif category == "LIMS":
                        start_time = datetime.now()
                        patient_history = db.get_patient_history(str(mrn))
                        if len(patient_history) != 0:
                            if debug:
                                count = count + 1
                            latest_creatine_result = data[1]
                            latest_creatine_date = data[0]
                            D, change_ = D_value_compute(
                                latest_creatine_result,
                                latest_creatine_date,
                                patient_history,
                            )
                            C1, RV1, RV1_ratio, RV2, RV2_ratio = RV_compute(
                                latest_creatine_result,
                                latest_creatine_date,
                                patient_history,
                            )
                            features = [
                                patient_history[0][1],
                                label_encode(patient_history[0][2]),
                                C1,
                                RV1,
                                RV1_ratio,
                                RV2,
                                RV2_ratio,
                                change_,
                                D,
                            ]
                            input = pd.DataFrame([features], columns=FEATURES_COLUMNS)
                            aki = predict_with_dt(dt_model, input)
                        
                        elif len(patient_history) == 0:
                            latest_creatine_result = data[1]
                            latest_creatine_date = data[0]
                            D = 0
                            change_ = 0
                            C1 = latest_creatine_result
                            RV1 = 0
                            RV1_ratio = 0
                            RV2 = 0
                            RV2_ratio = 0
                            features = [
                                db.get_patient(mrn)[1],
                                label_encode(db.get_patient(mrn)[2]),
                                C1,
                                RV1,
                                RV1_ratio,
                                RV2,
                                RV2_ratio,
                                change_,
                                D,
                            ]
                            input = pd.DataFrame([features], columns=FEATURES_COLUMNS)
                            aki = predict_with_dt(dt_model, input)                            
                            #aki_lis.append(aki)
                        if aki[0] == "y":
                            #count11 =  count11 + 1
                            if debug:
                                outputs.append((mrn, latest_creatine_date))
                            send_pager_request(mrn, pager_address)
                        end_time = datetime.now()
                        db.insert_test_result(mrn, data[0], data[1])
                        if debug:
                            latency = end_time - start_time
                            latencies.append(latency)

                    # Create and send ACK message
                    ack_message = create_acknowledgement()
                    sock.sendall(ack_message)
                else:
                    print("No valid MLLP message received.")
    except Exception as e:
        print(e)
    finally:
        # perform any cleanup or data persistance tasks
        # (this is done when we encounter an exception or if the 
        # program finishes its flow normally - so it is separate from the 
        # graceful shutdown)
        try:
            db.persist_db()
            db.close()
            print("Database persisted")
        except:
            print('Database has already been persisted and closed.')
    #print("Number of patients with AKI detected: ", count11)
    #print("Labels for patients with no history: ", set(tuple(item) for item in aki_lis))

    if debug:
        print("Patients with Historical Data", count)

        # Calculate latency metrics
        mean_latency = np.mean(latencies)
        median_latency = np.median(latencies)
        min_latency = np.min(latencies)
        max_latency = np.max(latencies)
        percentile_99 = np.percentile(latencies, 99)

        metrics = {
            "Mean": mean_latency,
            "Median": median_latency,
            "Minimum": min_latency,
            "Maximum": max_latency,
            "99% Efficiency": percentile_99,
        }
        print(metrics)

        df = pd.DataFrame(outputs, columns=["mrn", "date"])
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d %H:%M:%S")
        df.to_csv("aki_predicted.csv", index=False)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mllp",
        default="0.0.0.0:8440",
        type=str,
        help="Port on which to get HL7 messages via MLLP",
    )
    parser.add_argument(
        "--pager",
        default="0.0.0.0:8441",
        type=str,
        help="Post on which to send pager requests via HTTP",
    )
    parser.add_argument(
        "--debug",
        default=False,
        type=bool,
        help="Whether to calculate F3 and Latency Score",
    )
    flags = parser.parse_args()
    start_server(flags.mllp, flags.pager, flags.debug)


if __name__ == "__main__":
    main()
