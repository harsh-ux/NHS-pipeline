import unittest
from utils import (
    process_mllp_message,
    parse_hl7_message,
    create_acknowledgement,
    populate_test_results_table,
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


if __name__ == "__main__":
    unittest.main()
