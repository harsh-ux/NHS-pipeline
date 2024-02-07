import socket
import hl7
import datetime

from constants import MLLP_START_CHAR, MLLP_END_CHAR, REVERSE_LABELS_MAP


def process_mllp_message(data):
    """
    Extracts the HL7 message from the MLLP data.
    """
    start_index = data.find(MLLP_START_CHAR)
    end_index = data.find(MLLP_END_CHAR)
    if start_index != -1 and end_index != -1:
        return data[start_index + 1 : end_index]
    return None


def parse_hl7_message(hl7_data):
    """
    Parses the HL7 message and returns the parsed message object.
    """
    message = hl7.parse(hl7_data.decode("utf-8"))
    return message


def create_acknowledgement():
    """
    Creates an HL7 ACK message for the received message.
    """
    # Construct the ACK message based on the simulator's expectations
    ack_msg = f"MSH|^~\\&|||||{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}||ACK||P|2.5\rMSA|AA|\r"
    framed_ack = MLLP_START_CHAR + ack_msg.encode() + MLLP_END_CHAR
    return framed_ack


def predict_with_dt(dt_model, data):
    """
    Following data needs to be passed:
    [
        "age",
        "sex",
        "C1",
        "RV1",
        "RV1_ratio",
        "RV2",
        "RV2_ratio",
        "change_within_48hrs",
        "D"
    ]
    Predict with the DT Model on the data.
    Returns the predicted labels.
    """
    y_pred = dt_model.predict(data)

    # Map the predictions to labels
    labels = [REVERSE_LABELS_MAP[item] for item in y_pred]

    return labels
