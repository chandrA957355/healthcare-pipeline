# Healthcare Eligibility Pipeline

A simple config-driven pipeline to process eligibility files from different healthcare partners.

## How to Run

1. Upload `acme.txt` and `bettercare.csv` into the root folder where the pipeline.py code is present

2. Run the pipeline:
```python
!python pipeline.py
```

## How to Add a New Partner

Just add a new entry to `PARTNER_CONFIG` in pipeline.py. For example, if you get a new partner called "HealthPlus" with tab-delimited files:

```python
"healthplus": {
    "file_path": "healthplus.tsv",
    "sep": "\t",
    "partner_code": "HEALTHPLUS",
    "column_mapping": {
        "member_id": "external_id",
        "fname": "first_name",
        "lname": "last_name",
        "birth_date": "date_of_birth",
        "contact_email": "email",
        "contact_phone": "phone"
    },
    "date_format": "%Y%m%d"  # whatever format they use
}
```

That's it. No code changes needed, just config.

## Output

The pipeline creates `unified_eligibility.csv` with these columns:
- external_id
- first_name (Title Case)
- last_name (Title Case)
- dob (YYYY-MM-DD format)
- email (lowercase)
- phone (XXX-XXX-XXXX format)
- partner_code

## Notes

- Rows without external_id get dropped (with a warning)
- Bad dates become None instead of crashing
- Phone numbers with weird formats are kept as-is if they can't be parsed
