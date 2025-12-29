import csv
import sqlite3
import os
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), 'unitedutilitiesdb.db')
CSV_FILE = os.path.join(os.path.dirname(__file__), 'jobs.csv')

def parse_date(date_str):
    """Try to normalize various Excel-style date formats into YYYY-MM-DD."""
    if not date_str or not date_str.strip():
        return None
    date_str = date_str.strip().split(' ')[0]  # remove time part if exists
    formats = [
        "%d/%m/%Y", "%m/%d/%Y", "%Y-%m-%d",
        "%d/%m/%y", "%m/%d/%y"
    ]
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None

def get_site_id(conn, site_name):
    """Look up SiteID from tblSites based on Site Description."""
    cur = conn.cursor()
    cur.execute("SELECT ID FROM tblSites WHERE Name = ? COLLATE NOCASE", (site_name.strip(),))
    row = cur.fetchone()
    return row[0] if row else None

def create_table(conn):
    """Create tblJobs if not exists."""
    conn.execute("""
    CREATE TABLE IF NOT EXISTS tblJobs (
        ID INTEGER PRIMARY KEY AUTOINCREMENT,
        SiteID INTEGER,
        FLKDesc TEXT,
        FLK TEXT,
        LocationDesc TEXT,
        PlantDesc TEXT,
        WorkCenterDesc TEXT,
        EAgI TEXT,
        EAgIDesc TEXT,
        TaskDesc TEXT,
        CycleDaysRCM INTEGER,
        Frequency TEXT,
        NextCallDate TEXT,
        DueDate TEXT,
        DaysOverdue INTEGER,
        Reason TEXT,
        FacilityKeyDesc TEXT,
        FOREIGN KEY(SiteID) REFERENCES tblSites(ID)
    )
    """)
    conn.commit()

def import_csv():
    conn = sqlite3.connect(DB_PATH)
    create_table(conn)

    with open(CSV_FILE, newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        print("Detected columns:", reader.fieldnames)

        for row in reader:
            # Strip whitespace
            row = {k.strip(): (v.strip() if v else None) for k, v in row.items()}

            site_name = row.get("Site Description") or ""
            site_id = get_site_id(conn, site_name)
            if not site_id:
                print(f"⚠️ Skipping row — site not found: {site_name}")
                continue

            conn.execute("""
                INSERT INTO tblJobs (
                    SiteID, FLKDesc, FLK, LocationDesc, PlantDesc,
                    WorkCenterDesc, EAgI, EAgIDesc, TaskDesc,
                    CycleDaysRCM, Frequency, NextCallDate, DueDate,
                    DaysOverdue, Reason, FacilityKeyDesc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                site_id,
                row.get("Functional Location Key/Description") or "",
                row.get("Functional Location Key") or "",
                row.get("Functional Location Description") or "",
                row.get("Maintenance Plant Description") or "",
                row.get("MAINTENANCE_WORK_CENTER_DESCRIPTION") or "",
                row.get("E Agi") or "",
                row.get("E Agi Description") or "",
                row.get("Task Description") or "",
                int(row.get("Cycle in Days RCM") or 0),
                row.get("Frequency") or "",
                parse_date(row.get("NEXT_CALL_DATE")),
                parse_date(row.get("Due Date")),
                int(row.get("Days Overdue") or 0),
                row.get("Reason") or "",
                row.get("Facility Key - Desc") or ""
            ))

    conn.commit()
    conn.close()
    print("✅ Import complete!")

if __name__ == "__main__":
    import_csv()
