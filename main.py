#!/usr/bin/env python3

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
    connect_to_mllp,
    read_from_mllp
)
from datetime import datetime
import pandas as pd
import numpy as np
import os
import sys
import traceback


def start_server(history_load_path, mllp_address, pager_address, debug=False):
    """
    Starts the TCP server to listen for incoming MLLP messages on the specified port.
    """
    if debug:
        latencies = []  # to measure latency
        outputs = []  # to measure f3 score
        count = 0
    mllp_host, mllp_port = strip_url(mllp_address)

    # Initialise the in-memory database
    db = InMemoryDatabase(history_load_path)  # this also loads the previous history
    assert db != None, "In-memory Database is not initialised properly..."
    
    # Start the server
    sock = connect_to_mllp(mllp_host, mllp_port)

    # store the current socket for connection management
    current_socket = {"sock":sock}

    # register signals for graceful shutdown
    signal.signal(signal.SIGINT, define_graceful_shutdown(db, current_socket))
    signal.signal(signal.SIGTERM, define_graceful_shutdown(db, current_socket))

    # Load the model once for use through out
    dt_model = load(DT_MODEL_PATH)
    assert dt_model != None, "Model is not loaded properly..."
    # aki_lis = []

    try:
        # count11 = 0
        while True:
            data, need_to_reconnect = read_from_mllp(sock)

            if need_to_reconnect:
                sock = connect_to_mllp(mllp_host, mllp_port)
                # update the current socket for connection management
                current_socket["sock"] = sock

            if data:
                hl7_data = process_mllp_message(data)
            else:
                hl7_data = None
                print("No data received.")
            
            if hl7_data:
                # print("HL7 Data received:", hl7_data)
                message = parse_hl7_message(hl7_data)
                # print("Message:", message)

                category, mrn, data = parse_system_message(
                    message
                )  # category is type of system message and data consists of age sex if PAS admit or date of blood test and creatanine result
                print("Parsed values: ", category, mrn, data)
                if category == "PAS-admit":
                    # print('Patient {} inserted'.format(mrn))
                    print(f"PAS-Admit: Inserting {mrn} into db...")
                    db.insert_patient(mrn, int(data[0]), str(data[1]))

                    # check if patient was inserted correctly
                    if db.get_patient(mrn):
                        # Create and send ACK message
                        print("Sending ACK message for PAS-admit...")
                        ack_message = create_acknowledgement()
                        sock.sendall(ack_message)
                    else:
                        print("ACK message for PAS-admit NOT sent (failed to insert)...")
                elif category == "PAS-discharge":
                    print(f"PAS-discharge: Discharging {mrn} ...")
                    db.discharge_patient(mrn)
                    # check if patient was discharged correctly
                    if db.get_patient(mrn):
                        print("ACK message for PAS-discharge NOT sent (failed to delete)...")
                    else:
                        # Create and send ACK message
                        print("Sending ACK message for PAS-discharge...")
                        ack_message = create_acknowledgement()
                        sock.sendall(ack_message)
                elif category == "LIMS":
                    start_time = datetime.now()
                    print("Message from LIMS! Retreiving Patient History...")
                    patient_history = db.get_patient_history(str(mrn))
                    if len(patient_history) != 0:
                        print("Patient History found!")
                        # print("Patient History:", patient_history)
                        if debug:
                            count = count + 1
                        latest_creatine_result = data[1]
                        latest_creatine_date = data[0]
                        D, change_ = D_value_compute(
                            latest_creatine_result,
                            latest_creatine_date,
                            patient_history,
                        )
                        # print("D value computed: ", D, change_)
                        C1, RV1, RV1_ratio, RV2, RV2_ratio = RV_compute(
                            latest_creatine_result,
                            latest_creatine_date,
                            patient_history,
                        )
                        # print(
                        #     f"C1: {C1}, RV1: {RV1}, RV1_ratio: {RV1_ratio}, RV2_ratio: {RV2_ratio} calculated!"
                        # )
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
                        print("Features created...")
                        input = pd.DataFrame([features], columns=FEATURES_COLUMNS)
                        print("Calling DT!")
                        aki = predict_with_dt(dt_model, input)
                    elif len(patient_history) == 0 and db.get_patient(mrn):
                        print("Patient History doesn't exist...")
                        latest_creatine_result = data[1]
                        latest_creatine_date = data[0]
                        D = 0
                        change_ = 0
                        C1 = latest_creatine_result
                        RV1 = 0
                        RV1_ratio = 0
                        RV2 = 0
                        RV2_ratio = 0
                        print(
                            f"C1: {C1}, RV1: {RV1}, RV1_ratio: {RV1_ratio}, RV2_ratio: {RV2_ratio} calculated!"
                        )
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
                        print("Features created...")
                        input = pd.DataFrame([features], columns=FEATURES_COLUMNS)
                        print("Calling DT!")
                        aki = predict_with_dt(dt_model, input)
                        # aki_lis.append(aki)
                    else:
                        print("No such patient in the patients table...")
                    if aki[0] == "y":
                        if debug:
                            outputs.append((mrn, latest_creatine_date))
                        send_pager_request(mrn, pager_address)
                    end_time = datetime.now()
                    db.insert_test_result(mrn, data[0], data[1])
                    if debug:
                        latency = end_time - start_time
                        latencies.append(latency)
                    
                    # check if test result was inserted correctly
                    if db.get_test_result(mrn, data[0]):
                        # Create and send ACK message
                        print("Sending ACK message for LIMS...")
                        ack_message = create_acknowledgement()
                        sock.sendall(ack_message)
                    else:
                        print("ACK message for LIMS NOT sent (failed to insert)...")
            else:
                print("No valid MLLP message received.")
    except Exception as e:
        print("There was an exception in the main loop..")
        traceback.print_exc()
        sys.stderr.write(str(e))
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
            print("Database has already been persisted and closed.")

        try:
            current_socket["sock"].close()
            print("MLLP connection closed")
        except:
            print("MLLP connection has already been closed")

    # print("Number of patients with AKI detected: ", count11)
    # print("Labels for patients with no history: ", set(tuple(item) for item in aki_lis))

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
        "--debug",
        default=False,
        type=bool,
        help="Whether to calculate F3 and Latency Score",
    )
    parser.add_argument(
        "--history",
        default="data/history.csv",
        type=str,
        help="Where to load the history.csv file from",
    )
    MLLP_LINK = os.environ.get("MLLP_ADDRESS", "0.0.0.0:8440")
    PAGER_LINK = os.environ.get("PAGER_ADDRESS", "0.0.0.0:8441")
    flags = parser.parse_args()
    start_server(flags.history, MLLP_LINK, PAGER_LINK, flags.debug)


if __name__ == "__main__":
    main()
