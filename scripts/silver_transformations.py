import pandas as pd
import re
from datetime import datetime

def clean_names(name):
    if pd.isna(name):
        return None
    
    return str(name).strip().title()
    

def clean_email(email):
    if pd.isna(email):
        return None

    email = str(email).strip().lower()

    if email == "" or email == "user_no_domain":
        return None

    known_domains = ["example.com", "example.net", "example.org"]

    if "@" not in email:
        for domain in known_domains:
            if email.endswith(domain):
                username = email[:-len(domain)]
                if username:
                    return f"{username}@{domain}"
        return None

    return email


def clean_phone(phone):
    if pd.isna(phone):
        return None

    phone = str(phone)
    digits = re.sub(r"\D", "", phone)

    if digits == "":
        return None

    return digits
    

def clean_salary(raw_salary):
    if pd.isna(raw_salary):
        return None

    salary = re.sub(r"[^\d]", "", str(raw_salary))

    if salary == "":
        return None

    return abs(int(salary))


def clean_employment_dates(status, raw_join, raw_term):
    def _fix_format(date_value):
        if pd.isna(date_value):
            return None
        
        date_str = str(date_value).strip()
        
        try:
            return datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y-%m-%d")
        except ValueError:
            return None

    clean_join = _fix_format(raw_join)
    clean_term = _fix_format(raw_term)

    status_flag = False

    if (clean_join and clean_term) and (clean_term < clean_join):
        return None, None, True
        
    if status == "Terminated" and (raw_join is None and raw_term is None):
        status_flag = True
    else:
        status_flag = False

    return clean_join, clean_term, status_flag


def clean_address(raw_address):
    if pd.isna(raw_address):
        return None
    
    address = str(raw_address).replace("\n", " ").strip()
    if address.lower() == "unknown" or address == "":
        return None
    
    return address