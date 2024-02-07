import socket
import hl7
import datetime
import csv
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
    hl7_string = hl7_data.decode("utf-8").replace('\r', '\n')
    message = hl7.parse(hl7_string)
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


def populate_test_results_table(db, path):
    """
    Reads in the training/testing data from a csv file and returns 
    a list of that data.
    Args:
        - db {InMemoryDatabase}: the database object
        - path {str}: path to the data
    """
    with open(path, newline='') as f:
        rows = csv.reader(f)
        for i, row in enumerate(rows):
            # skip header
            if i == 0:
                continue
            
            # remove empty strings
            while row and row[-1] == '':
                row.pop()
            
            mrn = row[0]
            # for each date, result pair insert into the table
            for j in range(1, len(row), 2):
                date = row[j]
                result = float(row[j+1])
                db.insert_test_result(mrn, date, result)


def parse_system_message(message):
    """
    Parses the HL7 message and returns components of respective message type: PAS, LIMS
    Args:
        - HL7 message object
    Returns the category of message, MRN, [AGE, SEX] if PAS category or [DATE_BLOOD_TEST, CREATININE_VALUE] if LIMS
    """
    mrn = 0
    category = ''
    data = ['']*2
    segments = str(message).split('\n')
    if len(segments) < 4:
        parsed_seg = segments[1].split('|')
        if len(parsed_seg) > 4:
            mrn = parsed_seg[3]
            category = 'PAS-admit'
            date_of_birth = parsed_seg[7]
            data[0] = calculate_age(date_of_birth)
            data[1] = parsed_seg[8][0]
        else:
            mrn = parsed_seg[3].replace('\r','')
            category = 'PAS-discharge'
    else:
        mrn = segments[1].split('|')[3]
        category = 'LIMS'
        data[0] = segments[2].split('|')[7] #date of blood test
        data[1] = float(segments[3].split('|')[5])

    return category,mrn,data
            
def calculate_age(date_of_birth):
    """
    Calculate age based on the date of birth provided in the format YYYYMMDD.
    """
    # Parse the date of birth string into a datetime object
    dob = datetime.datetime.strptime(date_of_birth, "%Y%m%d")
    
    # Get the current date
    current_date = datetime.datetime.now()
    
    # Calculate the difference between the current date and the date of birth
    age = current_date.year - dob.year - ((current_date.month, current_date.day) < (dob.month, dob.day))
    
    return age