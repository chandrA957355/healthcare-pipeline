import pandas as pd
from datetime import datetime
import re

# Partner configurations
# To add a new partner, just add another entry here!
PARTNER_CONFIG = {
    "acme": {
        "file_path": "acme.txt",
        "sep": "|",
        "partner_code": "ACME",
        "column_mapping": {
            "MBI": "external_id",
            "FNAME": "first_name",
            "LNAME": "last_name",
            "DOB": "date_of_birth",
            "EMAIL": "email",
            "PHONE": "phone"
        },
        "date_format": "%m/%d/%Y"
    },
    "bettercare": {
        "file_path": "bettercare.csv",
        "sep": ",",
        "partner_code": "BETTERCARE",
        "column_mapping": {
            "subscriber_id": "external_id",
            "first_name": "first_name",
            "last_name": "last_name",
            "date_of_birth": "date_of_birth",
            "email": "email",
            "phone": "phone"
        },
        "date_format": "%Y-%m-%d"
    }
}

# standard fields we expect after mapping
STANDARD_FIELDS = ["external_id", "first_name", "last_name", "date_of_birth", "email", "phone"]

def format_phone(phone_value):
    """Takes a phone number and formats it as XXX-XXX-XXXX"""
    if pd.isna(phone_value) or phone_value == "":
        return None

    digits = re.sub(r'\D', '', str(phone_value))

    if len(digits) == 10:
        return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
    return str(phone_value)


def parse_date(date_value, date_format):
    """Convert date to ISO format (YYYY-MM-DD)"""
    if pd.isna(date_value) or date_value == "":
        return None
    try:
        parsed = datetime.strptime(str(date_value).strip(), date_format)
        return parsed.strftime("%Y-%m-%d")
    except ValueError:
        return None


def process_partner(partner_name, config):
    """Process a single partner file based on its config"""

    # read everything as string to avoid ID mangling, skip bad rows
    df = pd.read_csv(
        config["file_path"],
        sep=config["sep"],
        dtype=str,
        keep_default_na=False,
        on_bad_lines="skip"
    )

    # rename columns to our standard names
    df = df.rename(columns=config["column_mapping"])

    # make sure all standard fields exist (fill missing ones with empty string)
    for field in STANDARD_FIELDS:
        if field not in df.columns:
            df[field] = ""

    # apply transformations
    df["first_name"] = df["first_name"].str.strip().str.title()
    df["last_name"] = df["last_name"].str.strip().str.title()
    df["email"] = df["email"].str.strip().str.lower()
    df["dob"] = df["date_of_birth"].apply(lambda x: parse_date(x, config["date_format"]))
    df["phone"] = df["phone"].apply(format_phone)
    df["partner_code"] = config["partner_code"]

    # validation - clean up external_id and check for missing
    df["external_id"] = df["external_id"].str.strip()
    missing_id = (df["external_id"].isna()) | (df["external_id"] == "")
    if missing_id.any():
        print(f"  Warning: {missing_id.sum()} rows missing external_id - dropping them")

    df = df[~missing_id]

    # select only the columns we need
    output_columns = ["external_id", "first_name", "last_name", "dob", "email", "phone", "partner_code"]
    df = df[output_columns]

    return df


def run_pipeline():
    """Main function"""

    all_data = []

    for partner_name, config in PARTNER_CONFIG.items():
        print(f"Processing {partner_name}...")
        partner_df = process_partner(partner_name, config)
        all_data.append(partner_df)
        print(f"  Got {len(partner_df)} records")

    unified_df = pd.concat(all_data, ignore_index=True)

    unified_df.to_csv("unified_eligibility.csv", index=False)
    print(f"\nTotal records: {len(unified_df)}")

    return unified_df

if __name__ == "__main__":
    result = run_pipeline()
    print("Final output:")
    print(result.to_string(index=False))