import socket
from joblib import load
from utils import process_mllp_message, parse_hl7_message, create_acknowledgement, parse_system_message
from memory_db import InMemoryDatabase
from constants import DT_MODEL_PATH, REVERSE_LABELS_MAP
from utils import populate_test_results_table


def start_server(host="0.0.0.0", port=8440):
    """
    Starts the TCP server to listen for incoming MLLP messages on the specified port.
    """
    #Initialise the in-memory database
    db = InMemoryDatabase()
    assert db != None, "In-memory Database is not initialised properly..."

    #Populate the in-memory database with processed historical data
    populate_test_results_table(db, 'history.csv')

    # Load the model once for use through out
    dt_model = load(DT_MODEL_PATH)
    assert dt_model != None, "Model is not loaded properly..."

    # Start the server
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((host, port))
        print(f"Connected to simulator on {host}:{port}")

        while True:
            data = sock.recv(1024)
            if not data:
                print("No data received. Closing connection.")
                break
            
            hl7_data = process_mllp_message(data)
            if hl7_data:
                message = parse_hl7_message(hl7_data)
                print("Parsed HL7 Message:")
                # print(message)
                # print(type(message))
                category,mrn,data = parse_system_message(message) #category is type of system message and data consists of age sex if PAS admit or date of blood test and creatanine result
                if category == 'PAS-admit':
                    print('Patient {} inserted'.format(mrn))
                    db.insert_patient(mrn, int(data[0]), str(data[1]))
                elif category == 'PAS-discharge':
                    db.discharge_patient(mrn)
                elif category == 'LIMS':
                    db.insert_test_result(mrn, data[0], data[1])
                #print(category,mrn,data,'\n')
                # Create and send ACK message
                ack_message = create_acknowledgement(message)
                sock.sendall(ack_message)
            else:
                print("No valid MLLP message received.")


def main():
    start_server()


if __name__ == "__main__":
    main()
