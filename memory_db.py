import sqlite3
from constants import ON_DISK_DB_PATH
import os
from utils import populate_test_results_table, populate_patients_table


class InMemoryDatabase:
    def __init__(self):
        self.connection = sqlite3.connect(":memory:")
        self.initialise_tables()
        self.load_db()
        # make sure we always have a db file
        if not os.path.exists(ON_DISK_DB_PATH):
            self.persist_db()

    def initialise_tables(self):
        """
        Initialise the database with the patient features table.
        """
        create_patients = """
            CREATE TABLE patients (
                mrn TEXT PRIMARY KEY,   
                age INTEGER,
                sex TEXT
            );
        """
        create_test_results = """
            CREATE TABLE test_results (
                mrn TEXT,   
                date DATETIME,
                result DECIMAL,
                PRIMARY KEY (mrn, date),
                FOREIGN KEY (mrn) REFERENCES patients (mrn)
            );
        """
        create_patient_features = """
            CREATE TABLE features (
                mrn TEXT PRIMARY KEY,   
                age INTEGER,
                sex TEXT,
                C1 DECIMAL,
                RV1 DECIMAL,
                RV1_ratio DECIMAL,
                RV2 DECIMAL,
                RV2_ratio DECIMAL,
                has_changed_48h INTEGER,
                D DECIMAL,
                aki TEXT
            );
        """
        # create the tables
        self.connection.execute(create_patients)
        self.connection.execute(create_test_results)
        self.connection.execute(create_patient_features)

    def insert_patient_features(
        self, mrn, age, sex, c1, rv1, rv1_r, rv2, rv2_r, change, D, aki=None
    ):
        """
        Insert the obtained features into the in-memory database.
        Args:
            - mrn {str}: Medical Record Number of the patient
            - age {int}: Age of the patient
            - sex {str}: Sex of the patient ('m'/'f')
            - c1 {float}: Most recent creatinine result value
            - rv1 {float}: Lowest creatinine result in last 7d
            - rv1_r {float}: C1 / RV1
            - rv2 {float}: Median creatinine result in within last 8-365d
            - rv2_r {float}: C1 / RV2
            - change {bool}: Whether there has been a change in last 48h
            - D {float}: Difference between current and lowest previous result (48h)
            - aki {str}: Whether the patient has been diagnosed with aki ('y'/'n')
        """
        query = """
            INSERT INTO features 
                (mrn, age, sex, C1, RV1, RV1_ratio, RV2, RV2_ratio, has_changed_48h, D, aki) 
            VALUES 
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        # execute the query
        try:
            self.connection.execute(
                query, (mrn, age, sex, c1, rv1, rv1_r, rv2, rv2_r, change, D, aki)
            )
            self.connection.commit()
        except sqlite3.IntegrityError:
            print(f"The features for patient {mrn} are already in the features table!")

    def insert_patient(self, mrn, age, sex, update_disk_db=True):
        """
        Insert the patient info from PAS into the in-memory database.
        Args:
            - mrn {str}: Medical Record Number of the patient
            - age {int}: Age of the patient
            - sex {str}: Sex of the patient ('m'/'f')
        """
        query = """
            INSERT INTO patients 
                (mrn, age, sex) 
            VALUES 
                (?, ?, ?)
        """
        # execute the query
        try:
            self.connection.execute(query, (mrn, age, sex))
            self.connection.commit()

        except sqlite3.IntegrityError:
            print(f"Patient {mrn} is already in the patients table!")

        # if update_disk_db:
        #     disk_conn = sqlite3.connect(ON_DISK_DB_PATH)
        #     disk_conn.execute('INSERT OR IGNORE INTO patients (mrn, age, sex) VALUES (?, ?, ?)', (mrn, age, sex))
        #     disk_conn.commit()
        #     disk_conn.close()

    def insert_test_result(self, mrn, date, result):
        """
        Insert the patient info from PAS into the in-memory database.
        Args:
            - mrn {str}: Medical Record Number of the patient
            - date {datetime}: creatinine result date
            - result {float}: creatinine result
        """
        query = """
            INSERT INTO test_results 
                (mrn, date, result) 
            VALUES 
                (?, ?, ?)
        """
        # execute the query
        try:
            self.connection.execute(query, (mrn, date, result))
            self.connection.commit()
        except sqlite3.IntegrityError:
            print(
                f"Test result on date-time: {date} for: {mrn} is already in the test_results table!"
            )

    def get_patient_features(self, mrn):
        """
        Query the features table for a given mrn.
        Args:
            - mrn {str}: Medical Record Number
        """
        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM features WHERE mrn = ?", (mrn,))
        return cursor.fetchone()

    def get_patient(self, mrn):
        """
        Query the patients table for a given mrn.
        Args:
            - mrn {str}: Medical Record Number
        """
        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM patients WHERE mrn = ?", (mrn,))
        return cursor.fetchone()

    def get_test_results(self, mrn):
        """
        Query the test results table for a given mrn.
        Args:
            - mrn {str}: Medical Record Number
        """
        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM test_results WHERE mrn = ?", (mrn,))
        return cursor.fetchall()

    def get_patient_history(self, mrn):
        """
        Get patient info along with all their test results and their dates.
        Args:
            - mrn {str}: Medical Record Number
        Returns:
            - _ {list}: List of records
        """
        query = """
            SELECT 
                patients.mrn,
                patients.age,
                patients.sex,
                test_results.date,
                test_results.result
            FROM
                patients
            JOIN
                test_results 
            ON
                patients.mrn = test_results.mrn
            WHERE patients.mrn = ?
        """
        cursor = self.connection.cursor()
        cursor.execute(query, (mrn,))
        return cursor.fetchall()

    def discharge_patient(self, mrn, update_disk_db=True):
        """
        Remove the patient record from patients table in-memory and on-disk. Test
        results are kept in the test_results table for historic data.
        Args:
            - mrn {str}: Medical Record Number
        """
        # delete from in-memory
        self.connection.execute("DELETE FROM patients WHERE mrn = ?", (mrn,))
        self.connection.commit()
        # delete from on-disk
        if update_disk_db:
            with sqlite3.connect(ON_DISK_DB_PATH) as disk_connection:
                disk_connection.execute("DELETE FROM patients WHERE mrn = ?", (mrn,))
                disk_connection.commit()

    def update_patient_features(self, mrn, **kwargs):
        """
        Update patient information based on the provided keyword arguments.
        Args:
            - mrn {str}: Medical Record Number of the patient to update
            - **kwargs {dict}: Where key=column, value=new value
        """
        # construct the SET part of the SQL query based on the given args
        set_clause = ", ".join([f"{key} = ?" for key in kwargs])
        query = f"UPDATE features SET {set_clause} WHERE mrn = ?"
        # prepare the values for the placeholders in the SQL statement
        values = list(kwargs.values()) + [mrn]
        # execute the query
        self.connection.execute(query, values)
        self.connection.commit()

    def persist_db(self):
        """
        Persist the in-memory database to disk.
        Args:
            - disk_db_path {str}: the path to the database
        """
        # backs up and closes the connection
        with sqlite3.connect(ON_DISK_DB_PATH) as disk_connection:
            self.connection.backup(disk_connection)

    def load_db(self):
        """
        Load the on-disk database into the in-memory database.
        """
        # if on-disk db doesn't exist, use the csv file
        if not os.path.exists(ON_DISK_DB_PATH):
            print("Loading the history.csv file in memory.")
            populate_test_results_table(self, "data/history.csv")
            # populate_patients_table(self, 'processed_history.csv')
        else:
            # load the on-disk db into the in-memory one
            with sqlite3.connect(ON_DISK_DB_PATH) as disk_connection:
                print("Loading the on-disk database in memory.")
                disk_connection.backup(self.connection)

    def close(self):
        """
        Close the database connection.
        """
        self.connection.close()
