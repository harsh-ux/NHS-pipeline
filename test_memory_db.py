import unittest
from memory_db import InMemoryDatabase

class TestInMemoryDatabase(unittest.TestCase):
    def setUp(self):
        """
        Initialises the database before each test.
        """
        self.db = InMemoryDatabase()


    def tearDown(self):
        """
        Closes the database after each test.
        """
        self.db.close()


    def test_insert_and_get_for_patient(self):
        actual_record = ('31251122', 42, 'm', 142.22, 127.45, 1.12, 156.89, 0.91, False, 0, None)
        # insert
        self.db.insert_patient(*actual_record)
        # get
        queried_record = self.db.get_patient('31251122')
        self.assertEqual(actual_record, queried_record)


    def test_insert_and_update_for_patient(self):
        actual_record = ['31251122', 42, 'm', 142.22, 127.45, 1.12, 156.89, 0.91, False, 0, None]
        # insert
        self.db.insert_patient(*actual_record)
        # update
        self.db.update_patient('31251122', RV1=114.98, RV1_ratio=1.24)
        actual_record[4] = 114.98
        actual_record[5] = 1.24
        # get patient after update
        queried_record = self.db.get_patient('31251122')
        self.assertEqual(tuple(actual_record), queried_record)


if __name__ == '__main__':
    unittest.main()