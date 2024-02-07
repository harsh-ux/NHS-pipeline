import sqlite3

class InMemoryDatabase():
    def __init__(self):
        self.connection = sqlite3.connect(':memory:')
        self.initialise_table()
    

    def initialise_table(self):
        """
        Initialises the database with the patient features table.
        """
        query = """
            CREATE TABLE patients (
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
        # features table
        self.connection.execute(query)


    def insert_patient(self, mrn, age, sex, c1, rv1, rv1_r, rv2, rv2_r, change, D, aki=None):
        """
        Inserts the obtained features into the in-memory database.
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
            INSERT INTO patients 
                (mrn, age, sex, C1, RV1, RV1_ratio, RV2, RV2_ratio, has_changed_48h, D, aki) 
            VALUES 
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        self.connection.execute(
            query,
            (mrn, age, sex, c1, rv1, rv1_r, rv2, rv2_r, change, D, aki)
        )
        self.connection.commit()


    def get_patient(self, mrn):
        """
        Query the patient data for a given mrn.
        """

        cursor = self.connection.cursor()
        cursor.execute('SELECT * FROM patients WHERE mrn = ?', (mrn,))
        return cursor.fetchone()
    

    def update_patient(self, mrn, **kwargs):
        """
        Update patient information based on the provided keyword arguments.
        Args:
            - mrn {str}: Medical Record Number of the patient to update
            - **kwargs {dict}: Where key=column, value=new value
        """

        # construct the SET part of the SQL query based on the given args
        set_clause = ", ".join([f"{key} = ?" for key in kwargs])
        query = f"UPDATE patients SET {set_clause} WHERE mrn = ?"

        # prepare the values for the placeholders in the SQL statement
        values = list(kwargs.values()) + [mrn]

        # execute the update query
        self.connection.execute(query, values)
        self.connection.commit()
    

    def close(self):
        """
        Close the database connection.
        """
        self.connection.close()

    
    