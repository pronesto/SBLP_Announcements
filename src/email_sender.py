"""
This script reads a CSV file plus a text template, filters for Brazilian 
authors, generates personalized emails and send them over.
"""

import sys
import csv
import smtplib
import getpass
import argparse
import time
from email.message import EmailMessage

# --- I/O & Template Functions ---

def read_csv_data(filename):
    """Reads the CSV data with the list of emails and names. Each file has
    the following columns: Nome,FirstName,Surname,Email,Country """
    try:
        with open(filename, mode='r', encoding='utf-8') as f:
            data = list(csv.DictReader(f))
    except FileNotFoundError:
        print(f"Error: {filename} not found.", file=sys.stderr)
        sys.exit(1)
    else:
        return data

def read_template(filename):
    """Reads the email template. This template has a string <FirstName>
    that we can use to add the name of the recipient."""
    try:
        with open(filename, mode='r', encoding='utf-8') as f:
            content = f.read()
    except FileNotFoundError:
        print(f"Error: {filename} not found.", file=sys.stderr)
        sys.exit(1)
    else:
        return content

# --- Transformation & Filtering ---

def filter_brazilians(data):
    """Returns an iterator for rows where Country is 'br'. We shall be
    sending emails in Portuguese."""
    return filter(lambda row: row['Country'] == 'br', data)

def create_email(row, template, sender_email):
    """Composes the EmailMessage object for a specific recipient."""
    msg = EmailMessage()
    msg['Subject'] = "Chamada de Trabalhos: SBLP 2026"
    msg['From'] = sender_email
    msg['To'] = row['Email']
    body = template.replace('<FirstName>', row['FirstName'])
    msg.set_content(body)
    return msg

# --- Side Effect Functions ---

def send_batch(messages, host, user, password, dry_run=False):
    """Sends emails or prints them, handling authentication errors."""
    if dry_run:
        print("--- DRY RUN MODE ACTIVE (No emails will be sent) ---")
        for msg in messages:
            print(f"TO: {msg['To']} | SUBJ: {msg['Subject']}")
    else:
        try:
            with smtplib.SMTP(host, 587) as server:
                server.starttls()
                server.login(user, password)
                for msg in messages:
                    server.send_message(msg)
                    print(f"Sent to: {msg['To']}")
                    # Wait 2 seconds between emails to respect server limits
                    time.sleep(2)
        except smtplib.SMTPAuthenticationError:
            print("Invalid password", file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            print(f"An unexpected error occurred: {e}", file=sys.stderr)
            sys.exit(1)

# --- Pipeline Execution ---

# This is the address of the mail sender:
SENDER_ADDR = "fernando@dcc.ufmg.br"

def run_pipeline(csv_file, template_file, sender, dry_run):
    """Orchestrates the data flow and execution mode."""
    data = read_csv_data(csv_file)
    template = read_template(template_file)
    brazilians = filter_brazilians(data)
    email_objs = map(lambda r: create_email(r, template, sender), brazilians)
    pwd = None if dry_run else getpass.getpass(f"Password for {sender}: ")
    send_batch(email_objs, "smtp.dcc.ufmg.br", sender, pwd, dry_run)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SBLP Email Pipeline")
    parser.add_argument("csv", help="Input CSV file")
    parser.add_argument("template", help="Email template text file")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Display summaries instead of sending"
    )
    args = parser.parse_args()
    run_pipeline(args.csv, args.template, SENDER_ADDR, args.dry_run)
