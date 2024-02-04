import socket

from utils import process_mllp_message, parse_hl7_message, create_acknowledgement


def start_server(host="0.0.0.0", port=8440):
    """
    Starts the TCP server to listen for incoming MLLP messages on the specified port.
    """
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
                print(message)

                # Create and send ACK message
                ack_message = create_acknowledgement(message)
                sock.sendall(ack_message)
            else:
                print("No valid MLLP message received.")


def main():
    start_server()


if __name__ == "__main__":
    main()
