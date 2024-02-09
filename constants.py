# MLLP constants
MLLP_START_CHAR = b"\x0b"
MLLP_END_CHAR = b"\x1c\x0d"

# Path to load and store the trained Decision Tree model
DT_MODEL_PATH = "dt_model.joblib"
ON_DISK_DB_PATH = "database.db"

# Map for AKI Label
LABELS_MAP = {"n": 0, "y": 1}

# Reverse labels map for writing the final output
REVERSE_LABELS_MAP = {v: k for k, v in LABELS_MAP.items()}

FEATURES_COLUMNS = [
    "age",
    "sex",
    "C1",
    "RV1",
    "RV1_ratio",
    "RV2",
    "RV2_ratio",
    "change_within_48hrs",
    "D",
]
