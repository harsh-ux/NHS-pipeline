import hl7
import datetime
import csv
import requests
from datetime import timedelta
import pandas as pd
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
    hl7_string = hl7_data.decode("utf-8").replace("\r", "\n")
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
    Reads in the patient test result history and populates the table.
    Args:
        - db {InMemoryDatabase}: the database object
        - path {str}: path to the data
    """
    with open(path, newline="") as f:
        rows = csv.reader(f)
        for i, row in enumerate(rows):
            # skip header
            if i == 0:
                continue

            # remove empty strings
            while row and row[-1] == "":
                row.pop()

            mrn = row[0]
            # for each date, result pair insert into the table
            for j in range(1, len(row), 2):
                date = row[j]
                result = float(row[j + 1])
                db.insert_test_result(mrn, date, result)


def populate_patients_table(db, path):
    """
    Reads in the processed history file and populates the patients table.
    Args:
        - db {InMemoryDatabase}: the database object
        - path {str}: path to the data
    """
    with open(path, newline="") as f:
        rows = csv.reader(f)
        for i, row in enumerate(rows):
            # skip header
            if i == 0:
                continue

            # get patient info
            mrn = row[1]
            age = row[2]
            sex = row[3]

            # insert into the table
            db.insert_patient(mrn, age, sex)


def parse_system_message(message):
    """
    Parses the HL7 message and returns components of respective message type: PAS, LIMS
    Args:
        - HL7 message object
    Returns the category of message, MRN, [AGE, SEX] if PAS category or [DATE_BLOOD_TEST, CREATININE_VALUE] if LIMS
    """
    mrn = 0
    category = ""
    data = [""] * 2
    segments = str(message).split("\n")
    if len(segments) < 4:
        parsed_seg = segments[1].split("|")
        if len(parsed_seg) > 4:
            mrn = parsed_seg[3]
            category = "PAS-admit"
            date_of_birth = parsed_seg[7]
            data[0] = calculate_age(date_of_birth)
            data[1] = parsed_seg[8][0]
        else:
            mrn = parsed_seg[3].replace("\r", "")
            category = "PAS-discharge"
    else:
        mrn = segments[1].split("|")[3]
        category = "LIMS"
        data[0] = segments[2].split("|")[7]  # date of blood test
        data[1] = float(segments[3].split("|")[5])

    return category, mrn, data


def calculate_age(date_of_birth):
    """
    Calculate age based on the date of birth provided in the format YYYYMMDD.
    """
    # Parse the date of birth string into a datetime object
    dob = datetime.datetime.strptime(date_of_birth, "%Y%m%d")

    # Get the current date
    current_date = datetime.datetime.now()

    # Calculate the difference between the current date and the date of birth
    age = (
        current_date.year
        - dob.year
        - ((current_date.month, current_date.day) < (dob.month, dob.day))
    )

    return age


def alert_response_team(host, port, mrn):
    """
    Sends a page to the pager server with the given MRN.
    """
    url = f"http://{host}:{port}/page"
    headers = {"Content-Type": "text/plain"}
    try:
        response = requests.post(url, data=str(mrn), headers=headers)
        if response.status_code == 200:
            print(f"Successfully paged for MRN: {mrn}")
        else:
            print(
                f"Failed to page for MRN: {mrn}. Status code: {response.status_code}, Response: {response.text}"
            )
    except requests.RequestException as e:
        print(f"Request failed: {e}")


def create_features(creatinine_dates, creatinine_results):
    """
    Creates featues from the dates and results of 1 patient.

    Args:
        creatinine_dates (List): Dates of each test.
        creatinine_results (List): Results of each test.

    Returns:
        (C1: float, RV1: float, RV1_ratio: float, RV2: float, RV2_ratio: float, change_within_48hrs: boolean, D: float): Created features.
    """

    # Sort by date while keeping results aligned
    sorted_pairs = list(zip(creatinine_dates, creatinine_results))

    # C1: Latest creatinine value
    latest_date, C1 = next(
        ((date, res) for date, res in sorted_pairs[::-1] if pd.notnull(res)),
        (None, None),
    )

    # Remove C1 from sorted pairs to prevent inteference in RV1 calculation
    sorted_pairs = [pair for pair in sorted_pairs if pair != (latest_date, C1)]

    # Calculate time windows from the latest date
    if latest_date:
        seven_days_ago = latest_date - timedelta(days=7)
        one_year_ago = latest_date - timedelta(days=365)
    else:
        seven_days_ago, one_year_ago = None, None

    # Filter results within 0-7 days and 8-365 days
    results_0_7_days = [
        res for date, res in sorted_pairs if seven_days_ago < date <= latest_date
    ]
    results_8_365_days = [
        res for date, res in sorted_pairs if one_year_ago <= date <= seven_days_ago
    ]

    # RV1: Lowest value within 0-7 days
    RV1 = min(results_0_7_days, default=None)
    if not RV1:
        RV1 = 0

    # RV1 Ratio
    RV1_ratio = C1 / RV1 if C1 and RV1 else 0

    # RV2: Median value within 8-365 days
    RV2 = pd.Series(results_8_365_days).median()
    if not RV2 or pd.isna(RV2) or pd.isnull(RV2):
        RV2 = 0

    # RV2 Ratio
    RV2_ratio = C1 / RV2 if C1 and RV2 else 0

    # Check change within 48 hours
    forty_eight_hours_ago = latest_date - timedelta(hours=48)
    recent_results = [
        res for date, res in sorted_pairs if date >= forty_eight_hours_ago
    ]
    change_within_48hrs = len(recent_results) > 1

    # D: Difference between current and lowest previous result within 48hrs
    D = C1 - min(recent_results, default=C1) if change_within_48hrs else 0

    return C1, RV1, RV1_ratio, RV2, RV2_ratio, change_within_48hrs, D
