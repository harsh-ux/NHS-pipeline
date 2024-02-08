import unittest
from utils import (
    process_mllp_message,
    parse_hl7_message,
    create_acknowledgement,
    populate_test_results_table,
    calculate_age,
    parse_system_message
)
from memory_db import InMemoryDatabase
import hl7


class TestUtilsClient(unittest.TestCase):
    def test_process_mllp_message(self):
        """
        Test processing of MLLP messages.
        """
        mllp_message = b"\x0bMSH|^~\&|SIMULATION|SOUTH RIVERSIDE|||20240212131600||ADT^A01|||2.5\x1c\x0d"
        expected_result = (
            b"MSH|^~\&|SIMULATION|SOUTH RIVERSIDE|||20240212131600||ADT^A01|||2.5"
        )
        self.assertEqual(process_mllp_message(mllp_message), expected_result)

    def test_parse_hl7_message(self):
        """
        Test parsing of HL7 messages.
        """
        hl7_message = (
            "MSH|^~\&|SIMULATION|SOUTH RIVERSIDE|||20240212131600||ADT^A01|||2.5"
        ).encode()
        parsed_message = parse_hl7_message(hl7_message)
        self.assertIsInstance(parsed_message, hl7.Message)
        self.assertTrue("MSH" in str(parsed_message))

    def test_create_acknowledgement(self):
        """
        Test creation of HL7 ACK messages.
        """
        hl7_msg = hl7.parse(
            "MSH|^~\&|SIMULATION|SOUTH RIVERSIDE|||20240212131600||ADT^A01|||2.5"
        )
        ack_message = create_acknowledgement(hl7_msg)
        self.assertIn(b"MSH", ack_message)
        self.assertIn(b"ACK", ack_message)
        self.assertIn(b"MSA|AA|", ack_message)

    
    def test_populate_test_results_table(self):
        db = InMemoryDatabase()
        populate_test_results_table(db, 'history.csv')
        # expected result
        expected_result = ('822825', '2024-01-01 06:12:00', 68.58)
        result = db.get_test_results(expected_result[0])[0]
        # close the db
        db.close()
        self.assertEqual(result, expected_result)
    
    def test_pas_admit_message(self):
        # Mock a PAS admit HL7 message
        message = "MSH|^~\&|SIMULATION|SOUTH RIVERSIDE|||20240924102800||ADT^A01|||2.5\nPID|1||722269||SAFFRON CURTIS||19891008|F"
        expected_age = calculate_age("19891008")  # Assuming current date is fixed or calculate_age is mocked
        category, mrn, data = parse_system_message(message)
        self.assertEqual(category, "PAS-admit")
        self.assertEqual(mrn, "722269")
        self.assertEqual(data, [expected_age, "F"])

    def test_pas_discharge_message(self):
        # Mock a PAS discharge HL7 message
        message = "MSH|^~\&|SIMULATION|SOUTH RIVERSIDE|||20240924153400||ADT^A03|||2.5\nPID|1||853518"
        category, mrn, data = parse_system_message(message)
        self.assertEqual(category, "PAS-discharge")
        self.assertEqual(mrn, "853518")
        self.assertEqual(data, ['', ''])

    def test_lims_message(self):
        # Mock a LIMS HL7 message
        message = "MSH|^~\&|SIMULATION|SOUTH RIVERSIDE|||20240924153600||ORU^R01|||2.5\nPID|1||54229\nOBR|1||||||20240924153600\nOBX|1|SN|CREATININE||103.56923163550283"
        category, mrn, data = parse_system_message(message)
        self.assertEqual(category, "LIMS")
        self.assertEqual(mrn, "54229")
        self.assertTrue(isinstance(data[1], float))  # Ensure that the creatinine value is a float

    def test_incomplete_message(self):
        # Mock an incomplete HL7 message
        message = "MSH|...|..."
        with self.assertRaises(IndexError):  # Assuming your function raises IndexError for incomplete messages
            parse_system_message(message)


if __name__ == "__main__":
    unittest.main()
