from flask import Flask, render_template, jsonify, request, session, redirect, url_for
from functools import wraps
from collections import Counter
from supabase import create_client, Client
from datetime import datetime

# -----------------------
# Flask App
# -----------------------
app = Flask(__name__)
app.secret_key = "dev-secret-key"  # Required for sessions

# -----------------------
# Supabase config
# -----------------------
import os

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# -----------------------
# Login required decorator
# -----------------------
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

# -----------------------
# Login route
# -----------------------
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')

        result = supabase.table("tblusers")\
            .select("*")\
            .eq("username", username)\
            .eq("password", password)\
            .eq("isactive", True)\
            .execute()

        user = result.data[0] if result.data else None

        if user:
            session.clear()
            session['user_id'] = user['id']
            session['username'] = user['username']
            return redirect(url_for('index'))

        return render_template('login.html', error="Invalid credentials")

    return render_template('login.html')

# -----------------------
# Helper functions
# -----------------------
def get_all_sites():
    """Fetch all sites, sorted by name."""
    response = supabase.table("tblsites").select("*").order("name", desc=False).execute()
    if not hasattr(response, 'data') or response.data is None:
        print("Error fetching sites")
        return []
    return response.data  # list of dicts

def get_all_jobs():
    # Join tbljobs with tblsites for site info
    jobs_result = supabase.table("tbljobs").select("*").execute()
    sites_result = supabase.table("tblsites").select("*").execute()

    sites_map = {s["id"]: s for s in sites_result.data or []}

    jobs = []
    for j in jobs_result.data or []:
        site = sites_map.get(j["siteid"], {})
        jobs.append({
            "FLK": j.get("flk"),
            "FLKDesc": j.get("flkdesc"),
            "LocationDesc": j.get("locationdesc"),
            "PlantDesc": j.get("plantdesc"),
            "WorkCenterDesc": j.get("workcentredesc"),
            "EAgI": j.get("eagi"),
            "EAgIDesc": j.get("eagidesc"),
            "TaskDesc": j.get("taskdesc"),
            "CycleDaysRCM": j.get("cycledaysrcm"),
            "Frequency": j.get("frequency"),
            "NextCallDate": j.get("nextcalldate"),
            "DueDate": j.get("duedate"),
            "DaysOverdue": j.get("daysoverdue"),
            "Reason": j.get("reason"),
            "FacilityKeyDesc": j.get("facilitykeydesc"),
            "SiteName": site.get("name"),
            "SiteLocation": site.get("locationdescription")
        })
    return jobs

# -----------------------
# Routes
# -----------------------
@app.route('/')
@login_required
def index():
    return render_template('index.html')

@app.route("/api/sites")
@login_required
def get_sites():
    try:
        response = supabase.table("tblsites").select("id, name, locationdescription").execute()
        # Supabase returns a dict with 'data'
        sites = response.data if hasattr(response, 'data') else []
        # Convert to the format expected by JS
        result = [{"id": s["id"], "name": s["name"], "location": s["locationdescription"]} for s in sites]
        return jsonify(result)
    except Exception as e:
        print("Error fetching sites:", e)
        return jsonify([]), 500

@app.route("/api/jobs/<int:site_id>")
@login_required
def get_jobs(site_id):
    try:
        response = supabase.table("tbljobs").select(
            "id, siteid, flkdesc, flk, locationdesc, plantdesc, workcenterdesc, "
            "eagi, eagidesc, taskdesc, cycledaysrcm, frequency, nextcalldate, "
            "duedate, daysoverdue, reason, facilitykeydesc"
        ).eq("siteid", site_id).execute()

        jobs = response.data if hasattr(response, 'data') else []

        # convert column names to match JS expectations
        result = []
        for j in jobs:
            result.append({
                "id": j["id"],
                "FLK": j.get("flk", ""),
                "FLKDesc": j.get("flkdesc", ""),
                "LocationDesc": j.get("locationdesc", ""),
                "PlantDesc": j.get("plantdesc", ""),
                "WorkCenterDesc": j.get("workcenterdesc", ""),
                "EAgI": j.get("eagi", ""),
                "EAgIDesc": j.get("eagidesc", ""),
                "TaskDesc": j.get("taskdesc", ""),
                "CycleDaysRCM": j.get("cycledaysrcm", ""),
                "Frequency": j.get("frequency", ""),
                "NextCallDate": j.get("nextcalldate", ""),
                "DueDate": j.get("duedate", ""),
                "DaysOverdue": j.get("daysoverdue", ""),
                "Reason": j.get("reason", ""),
                "FacilityKeyDesc": j.get("facilitykeydesc", "")
            })

        return jsonify(result)
    except Exception as e:
        print("Error fetching jobs:", e)
        return jsonify([]), 500


@app.route('/api/jobs')
@login_required
def api_all_jobs():
    return jsonify(get_all_jobs())

@app.route('/jobs')
@login_required
def jobs_page():
    site_name = request.args.get("site")
    eagi_desc = request.args.get("eagidesc")

    jobs = get_all_jobs()
    if site_name:
        jobs = [j for j in jobs if j.get("SiteName") == site_name]
    if eagi_desc:
        jobs = [j for j in jobs if j.get("EAgIDesc") == eagi_desc]

    return render_template("index.html", jobs=jobs)

@app.route('/instruments')
@login_required
def instruments():
    jobs = get_all_jobs()  # get_all_jobs returns list of dicts with lowercase keys
    sites = get_all_sites()  # same

    # Use lowercase keys consistently
    locations = sorted({j['eagidesc'] for j in jobs if j.get('eagidesc')})
    regions = sorted({s['locationdescription'] for s in sites if s.get('locationdescription')})

    # Count instruments by eagidesc
    instrument_counts = Counter(j['eagidesc'] for j in jobs if j.get('eagidesc'))

    return render_template(
        'instruments.html',
        locations=locations,
        regions=regions,
        instrument_counts=instrument_counts
    )


@app.route('/sites')
@login_required
def sites():
    return render_template('sites.html')

@app.route('/api/sites/coords')
@login_required
def api_sites_coords():
    sites_result = supabase.table("tblsites").select("*").execute()
    locations_result = supabase.table("tbllocations").select("*").execute()
    owners_result = supabase.table("tblsiteowners").select("*").execute()

    owners_map = {o["siteid"]: o for o in owners_result.data or []}
    sites_map = {l["siteid"]: l for l in locations_result.data or []}

    result = []
    for s in sites_result.data or []:
        loc = sites_map.get(s["id"])
        owner = owners_map.get(s["id"])
        if loc and loc.get("latitude") and loc.get("longitude"):
            result.append({
                "id": s["id"],
                "name": s["name"],
                "Latitude": loc["latitude"],
                "Longitude": loc["longitude"],
                "OwnerName": owner.get("ownername") if owner else None,
                "OwnerPhone": owner.get("ownerphone") if owner else None,
                "OwnerEmail": owner.get("owneremail") if owner else None
            })
    return jsonify(result)

@app.route('/api/instruments/counts')
@login_required
def instrument_counts_api():
    jobs = get_all_jobs()
    counts = Counter(j['EAgIDesc'] for j in jobs if j.get('EAgIDesc'))
    return jsonify(counts)

# -----------------------
# Run app
# -----------------------
if __name__ == '__main__':
    app.run(debug=True, port=5001)
