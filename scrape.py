#!/usr/bin/env python3
"""
IPL Fantasy 2026 — ESPN Cricinfo Scraper
Scrapes match scorecards from ESPN Cricinfo HTML pages.
Writes data.json which the frontend reads.

Run locally:  python3 scrape.py
Runs automatically via GitHub Actions every 6 hours.
"""

import json
import re
import time
from datetime import datetime, timezone

from curl_cffi import requests
from bs4 import BeautifulSoup

# ── Config ────────────────────────────────────────────────────
SERIES_ID   = '1510719'
SERIES_SLUG = 'ipl-2026-1510719'

# ── Points system ─────────────────────────────────────────────
PTS = {'run': 1, 'wicket': 25, 'catch': 10, 'stumping': 5, 'run_out': 5}

# ── Team rosters ──────────────────────────────────────────────
TEAMS = {
    'Adi': {
        'displayName': "Adi's Team",
        'color': '#1565c0',
        'players': [
            {'nick': 'Bumrah',     'full': 'Jasprit Bumrah',      'aliases': ['Jasprit Bumrah']},
            {'nick': 'Sai Su',     'full': 'Sai Sudharsan',       'aliases': ['Sai Sudharsan', 'B Sai Sudharsan']},
            {'nick': 'Kishan',     'full': 'Ishan Kishan',        'aliases': ['Ishan Kishan']},
            {'nick': 'Shreyas',    'full': 'Shreyas Iyer',        'aliases': ['Shreyas Iyer']},
            {'nick': 'Jansen',     'full': 'Marco Jansen',        'aliases': ['Marco Jansen']},
            {'nick': 'Krunal',     'full': 'Krunal Pandya',       'aliases': ['Krunal Pandya']},
            {'nick': 'Axar',       'full': 'Axar Patel',          'aliases': ['Axar Patel']},
            {'nick': 'Noor Ahmed', 'full': 'Noor Ahmad',          'aliases': ['Noor Ahmad', 'Noor Ahmed']},
            {'nick': 'Pant',       'full': 'Rishabh Pant',        'aliases': ['Rishabh Pant']},
            {'nick': 'K Ahmed',    'full': 'Khaleel Ahmed',       'aliases': ['Khaleel Ahmed']},
            {'nick': 'Markram',    'full': 'Aiden Markram',       'aliases': ['Aiden Markram']},
            {'nick': 'R Sharma',   'full': 'Rohit Sharma',        'aliases': ['Rohit Sharma']},
        ],
    },
    'Kahaan': {
        'displayName': "Kahaan's Team",
        'color': '#c62828',
        'players': [
            {'nick': 'Head',        'full': 'Travis Head',          'aliases': ['Travis Head']},
            {'nick': 'Jaiswal',     'full': 'Yashasvi Jaiswal',     'aliases': ['Yashasvi Jaiswal']},
            {'nick': 'Dube',        'full': 'Shivam Dube',          'aliases': ['Shivam Dube']},
            {'nick': 'Buttler',     'full': 'Jos Buttler',          'aliases': ['Jos Buttler']},
            {'nick': 'Stoinis',     'full': 'Marcus Stoinis',       'aliases': ['Marcus Stoinis']},
            {'nick': 'NKR',         'full': 'Nitish Kumar Reddy',   'aliases': ['Nitish Kumar Reddy', 'Nitish Reddy']},
            {'nick': 'Siraj',       'full': 'Mohammed Siraj',       'aliases': ['Mohammed Siraj', 'Mohammad Siraj']},
            {'nick': 'KL Rahul',    'full': 'KL Rahul',             'aliases': ['KL Rahul', 'Lokesh Rahul']},
            {'nick': 'Chahal',      'full': 'Yuzvendra Chahal',     'aliases': ['Yuzvendra Chahal']},
            {'nick': 'Lockie',      'full': 'Lockie Ferguson',      'aliases': ['Lockie Ferguson']},
            {'nick': 'Mohsin Khan', 'full': 'Mohsin Khan',          'aliases': ['Mohsin Khan']},
            {'nick': 'Suryavanshi', 'full': 'Vaibhav Suryavanshi', 'aliases': ['Vaibhav Suryavanshi']},
        ],
    },
    'Anmol': {
        'displayName': "Anmol's Team",
        'color': '#e65100',
        'players': [
            {'nick': 'Pandya',   'full': 'Hardik Pandya',     'aliases': ['Hardik Pandya']},
            {'nick': 'Arshdeep', 'full': 'Arshdeep Singh',    'aliases': ['Arshdeep Singh']},
            {'nick': 'Gill',     'full': 'Shubman Gill',      'aliases': ['Shubman Gill']},
            {'nick': 'Hetmyer',  'full': 'Shimron Hetmyer',   'aliases': ['Shimron Hetmyer']},
            {'nick': 'Varma',    'full': 'Tilak Varma',       'aliases': ['Tilak Varma']},
            {'nick': 'Narine',   'full': 'Sunil Narine',      'aliases': ['Sunil Narine']},
            {'nick': 'Jadeja',   'full': 'Ravindra Jadeja',   'aliases': ['Ravindra Jadeja']},
            {'nick': 'Allen',    'full': 'Finn Allen',        'aliases': ['Finn Allen']},
            {'nick': 'Bishnoi',  'full': 'Ravi Bishnoi',      'aliases': ['Ravi Bishnoi']},
            {'nick': 'Bhuvi',    'full': 'Bhuvneshwar Kumar', 'aliases': ['Bhuvneshwar Kumar']},
            {'nick': 'Brevis',   'full': 'Dewald Brevis',     'aliases': ['Dewald Brevis']},
            {'nick': 'Gaikwad',  'full': 'Ruturaj Gaikwad',  'aliases': ['Ruturaj Gaikwad']},
        ],
    },
    'Krish': {
        'displayName': "Krish's Team",
        'color': '#6a1b9a',
        'players': [
            {'nick': 'Sky',          'full': 'Suryakumar Yadav',    'aliases': ['Suryakumar Yadav']},
            {'nick': 'Virat',        'full': 'Virat Kohli',         'aliases': ['Virat Kohli']},
            {'nick': 'Rinku',        'full': 'Rinku Singh',         'aliases': ['Rinku Singh']},
            {'nick': 'Pooran',       'full': 'Nicholas Pooran',     'aliases': ['Nicholas Pooran']},
            {'nick': 'R Khan',       'full': 'Rashid Khan',         'aliases': ['Rashid Khan']},
            {'nick': 'Shami',        'full': 'Mohammed Shami',      'aliases': ['Mohammed Shami', 'Mohammad Shami']},
            {'nick': 'Chakravarthy', 'full': 'Varun Chakravarthy',  'aliases': ['Varun Chakravarthy']},
            {'nick': 'Pathirana',    'full': 'Matheesha Pathirana', 'aliases': ['Matheesha Pathirana']},
            {'nick': 'Umran Malik',  'full': 'Umran Malik',         'aliases': ['Umran Malik']},
            {'nick': 'Holder',       'full': 'Jason Holder',        'aliases': ['Jason Holder']},
            {'nick': 'Prabhsimran',  'full': 'Prabhsimran Singh',   'aliases': ['Prabhsimran Singh', 'Prabsimran Singh']},
            {'nick': 'Hooda',        'full': 'Deepak Hooda',        'aliases': ['Deepak Hooda']},
        ],
    },
    'Shyam': {
        'displayName': "Shyam's Team",
        'color': '#2e7d32',
        'players': [
            {'nick': 'Abhishek',    'full': 'Abhishek Sharma',   'aliases': ['Abhishek Sharma']},
            {'nick': 'Samson',      'full': 'Sanju Samson',      'aliases': ['Sanju Samson']},
            {'nick': 'Marsh',       'full': 'Mitchell Marsh',    'aliases': ['Mitchell Marsh']},
            {'nick': 'Klaasen',     'full': 'Heinrich Klaasen',  'aliases': ['Heinrich Klaasen']},
            {'nick': 'Kuldeep',     'full': 'Kuldeep Yadav',     'aliases': ['Kuldeep Yadav']},
            {'nick': 'Sai Kishore', 'full': 'R Sai Kishore',     'aliases': ['R Sai Kishore', 'Sai Kishore', 'Ravisrinivasan Sai Kishore']},
            {'nick': 'Boult',       'full': 'Trent Boult',       'aliases': ['Trent Boult']},
            {'nick': 'Rabada',      'full': 'Kagiso Rabada',     'aliases': ['Kagiso Rabada']},
            {'nick': 'Prasidh',     'full': 'Prasidh Krishna',   'aliases': ['Prasidh Krishna']},
            {'nick': 'Shashank',    'full': 'Shashank Singh',    'aliases': ['Shashank Singh']},
            {'nick': 'Parag',       'full': 'Riyan Parag',       'aliases': ['Riyan Parag']},
            {'nick': 'Thakur',      'full': 'Shardul Thakur',    'aliases': ['Shardul Thakur']},
        ],
    },
}

# ── Name lookup — exact match only, no last-name guessing ─────
def build_lookup():
    lookup = {}
    for team_key, team in TEAMS.items():
        for p in team['players']:
            for alias in p['aliases']:
                lookup[norm(alias)] = {'team_key': team_key, 'player': p}
    return lookup

def norm(s):
    return re.sub(r'\s+', ' ', s.lower()).strip()

def clean_name(s):
    return re.sub(r'\(.*?\)', '', s.replace('†', '').replace('(c)', '')).strip()

def find_player(name, lookup):
    if not name:
        return None
    return lookup.get(norm(clean_name(name)))

def find_player_by_lastname(name, lookup):
    """Last-name-only fallback — only used for fielder matching from short dismissal text."""
    if not name:
        return None
    last = norm(clean_name(name)).split()[-1] if norm(clean_name(name)).split() else ''
    if not last or len(last) < 3:  # skip very short tokens to avoid false matches
        return None
    matches = [v for k, v in lookup.items() if k.split()[-1] == last]
    return matches[0] if len(matches) == 1 else None  # only match if unambiguous

# ── HTTP session ──────────────────────────────────────────────
SESSION = requests.Session(impersonate='chrome')

def get_html(url, retries=3):
    for attempt in range(retries):
        try:
            r = SESSION.get(url, timeout=25)
            r.raise_for_status()
            return r.text
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(3 * (attempt + 1))
            else:
                raise

def extract_next_data(html):
    m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError:
            pass
    return None

def is_match_complete(nd):
    """Return True if the match is finished (state=POST), False if live/pre."""
    if not nd:
        return False
    raw = json.dumps(nd)
    states = re.findall(r'"state"\s*:\s*"([^"]+)"', raw)
    # POST = finished, LIVE = in progress, PRE = not started
    return 'POST' in states and 'LIVE' not in states

# ── Match list ────────────────────────────────────────────────
def get_completed_matches():
    """
    Two-stage match discovery:
    1. Scrape the results page for confirmed scorecard links (always reliable)
    2. Probe sequential match IDs starting from the last known one to catch
       matches whose scorecards are published but not yet linked on the results page.
    Returns list of {id, slug, name, date}.
    """
    print(f"Fetching match list for IPL 2026...")

    seen  = set()
    matches = []

    # ── Stage 1: results page ─────────────────────────────────
    try:
        html = get_html(
            f'https://www.espncricinfo.com/series/{SERIES_SLUG}/match-schedule-fixtures-and-results'
        )
        soup = BeautifulSoup(html, 'html.parser')
        for a in soup.find_all('a', href=True):
            href = a['href']
            m = re.search(
                rf'/series/{re.escape(SERIES_SLUG)}/([^/]+-(\d{{6,}}))/full-scorecard',
                href
            )
            if m:
                slug, mid = m.group(1), m.group(2)
                if mid not in seen:
                    seen.add(mid)
                    matches.append(_make_match(mid, slug))
        print(f"  Results page: {len(matches)} matches")
    except Exception as e:
        print(f"  Results page failed: {e}")

    # ── Stage 2: probe sequential IDs beyond what we found ────
    # IPL 2026 match IDs start at 1527674; try up to 20 ahead of the last found
    FIRST_MATCH_ID = 1527674
    known_ids = sorted(int(m['id']) for m in matches) if matches else [FIRST_MATCH_ID - 1]
    probe_start = max(known_ids) + 1
    probe_end   = probe_start + 20
    consecutive_misses = 0

    print(f"  Probing IDs {probe_start}–{probe_end} for new matches...")
    for mid_int in range(probe_start, probe_end + 1):
        mid = str(mid_int)
        if mid in seen:
            continue
        try:
            url  = f'https://www.espncricinfo.com/series/{SERIES_SLUG}/match-{mid}/full-scorecard'
            html = get_html(url)
            nd   = extract_next_data(html)
            if not nd:
                consecutive_misses += 1
                if consecutive_misses >= 3:
                    break
                continue
            # Check if the match is complete (has innings data)
            raw = json.dumps(nd)
            if '"inningBatsmen"' not in raw:
                consecutive_misses += 1
                if consecutive_misses >= 3:
                    break
                continue
            # Extract slug from canonical URL in __NEXT_DATA__ if possible
            slug_m = re.search(
                rf'"/{re.escape(SERIES_SLUG)}/([^/"]+)/full-scorecard"', raw
            )
            slug = slug_m.group(1) if slug_m else f'match-{mid}'
            seen.add(mid)
            matches.append(_make_match(mid, slug))
            print(f"    Found new match via probe: {mid}")
            consecutive_misses = 0
            time.sleep(1.5)
        except Exception:
            consecutive_misses += 1
            if consecutive_misses >= 3:
                break

    print(f"  Total completed matches found: {len(matches)}")
    return sorted(matches, key=lambda m: int(m['id']))

def _make_match(mid, slug):
    """Build a clean match dict from its ID and URL slug."""
    raw_name = slug.replace('-', ' ')
    raw_name = re.sub(r'\s+' + re.escape(mid) + r'\s*$', '', raw_name)
    raw_name = re.sub(r'\b(\d+)(St|Nd|Rd|Th)\b',
                      lambda x: x.group(1) + x.group(2).lower(),
                      raw_name.title())
    return {'id': mid, 'slug': slug, 'name': raw_name, 'date': ''}

# ── Scorecard parsing ─────────────────────────────────────────
def parse_run_out_fielder(text):
    """
    Parse 'run out (Roy)' or 'run out (Hetmyer/†Jurel)' → first named fielder.
    Returns fielder name string or None.
    """
    t = (text or '').strip()
    m = re.match(r'^run\s+out\s*\(([^)]+)\)', t, re.I)
    if not m:
        return None
    # Take the first fielder before any '/' separator
    fielder = m.group(1).split('/')[0]
    return clean_name(fielder).strip()

def parse_dismissal_text(text):
    """
    Parse both short ('c Pant b Bumrah') and long ('caught Rishabh Pant bowled Jasprit Bumrah')
    dismissal strings. Returns ('catch'|'stumping', fielder_name) or None.
    """
    t = (text or '').strip()
    if not t or t.lower() in ('not out', 'did not bat', 'absent', 'retired hurt', ''):
        return None

    # c & b / caught and bowled — bowler gets catch, skip (already counted via bowling)
    if re.match(r'^(c\s*&\s*b|caught and bowled)\s+', t, re.I):
        return None

    # Long format: "caught Rishabh Pant bowled ..."
    m = re.match(r'^caught\s+(.+?)\s+bowled\b', t, re.I)
    if m:
        return ('catch', m.group(1).strip())

    # Long format: "stumped Rishabh Pant bowled ..."
    m = re.match(r'^stumped\s+(.+?)\s+bowled\b', t, re.I)
    if m:
        return ('stumping', m.group(1).strip())

    # Short format: "c †Pant b Bumrah" (strip † before passing in)
    m = re.match(r'^c\s+(.+?)\s+b\s+', t, re.I)
    if m:
        return ('catch', m.group(1).strip())

    # Short format: "st †Pant b Chahal"
    m = re.match(r'^st\s+(.+?)\s+b\s+', t, re.I)
    if m:
        return ('stumping', m.group(1).strip())

    return None

def parse_scorecard_next_data(nd, lookup):
    """Extract stats from ESPN's __NEXT_DATA__ JSON blob."""
    result = {}

    def add(full_name, key, val=1):
        if full_name not in result:
            result[full_name] = {'runs': 0, 'wickets': 0, 'catches': 0, 'stumpings': 0, 'run_outs': 0}
        result[full_name][key] += val

    def walk(obj):
        """Recursively find innings arrays in the Next.js data tree."""
        if isinstance(obj, dict):
            # Look for inningBatsmen / inningBowlers keys (ESPN's scorecard format)
            if 'inningBatsmen' in obj or 'inningBowlers' in obj:
                process_inning(obj)
            else:
                for v in obj.values():
                    walk(v)
        elif isinstance(obj, list):
            for item in obj:
                walk(item)

    def process_inning(inning):
        # Batting
        for b in inning.get('inningBatsmen', []):
            player = b.get('player', {})
            bat_name = player.get('longName') or player.get('name', '')
            runs = int(b.get('runs', 0) or 0)
            p = find_player(bat_name, lookup)
            if p:
                add(p['player']['full'], 'runs', runs)

            # Fielding credit — try structured fielders list first, then dismissal text
            # Prefer long dismissal text (full names) over short (abbreviated names)
            dt = b.get('dismissalText')
            dismissal_long  = ''
            dismissal_short = ''
            if isinstance(dt, dict):
                dismissal_long  = dt.get('long', '')  or ''
                dismissal_short = dt.get('short', '') or ''
            elif isinstance(dt, str):
                dismissal_long = dt

            if dismissal_long or dismissal_short:
                print(f'      [dismissal] {bat_name}: long={dismissal_long!r}  short={dismissal_short!r}')

            fielders = b.get('inningFielders', []) or []
            credited = False

            # Structured fielders (most reliable — full names from player objects)
            for f in fielders:
                fp_obj = f.get('player', {})
                fname  = fp_obj.get('longName') or fp_obj.get('name', '')
                # Log the raw dismissal type so we know what ESPN sends
                how_raw = f.get('dismissalType') or f.get('type') or ''
                how     = how_raw.lower()
                if how_raw:
                    print(f'      [fielder] {fname!r} dismissalType={how_raw!r}')
                fp = find_player(fname, lookup)
                if fp:
                    add(fp['player']['full'], 'stumpings' if 'stump' in how else 'catches')
                    credited = True

            # Try long text first (has full names), then short text
            if not credited:
                for dtext in [dismissal_long, dismissal_short]:
                    if not dtext:
                        continue
                    # Check for run out first
                    ro_fielder = parse_run_out_fielder(dtext)
                    if ro_fielder:
                        fp = find_player(ro_fielder, lookup)
                        if not fp:
                            fp = find_player_by_lastname(ro_fielder, lookup)
                        if fp:
                            add(fp['player']['full'], 'run_outs')
                        credited = True
                        break
                    # Catch / stumping
                    credit = parse_dismissal_text(dtext)
                    if not credit:
                        continue
                    kind, fielder_name = credit
                    fp = find_player(fielder_name, lookup)
                    if not fp:
                        fp = find_player_by_lastname(fielder_name, lookup)
                    if fp:
                        add(fp['player']['full'], 'catches' if kind == 'catch' else 'stumpings')
                        credited = True
                        break

        # Bowling
        for b in inning.get('inningBowlers', []):
            player  = b.get('player', {})
            bowl_name = player.get('longName') or player.get('name', '')
            wickets = int(b.get('wickets', 0) or 0)
            p = find_player(bowl_name, lookup)
            if p:
                add(p['player']['full'], 'wickets', wickets)

    walk(nd)
    return result

def parse_scorecard_html_tables(html, lookup):
    """
    Fallback: parse ESPN's classic scorecard HTML.
    ESPN scorecard tables have class 'table batsman' and 'table bowling'.
    """
    soup = BeautifulSoup(html, 'html.parser')
    result = {}

    def add(full_name, key, val=1):
        if full_name not in result:
            result[full_name] = {'runs': 0, 'wickets': 0, 'catches': 0, 'stumpings': 0, 'run_outs': 0}
        result[full_name][key] += val

    # ESPN wraps each innings in a div; batting and bowling tables are siblings
    # Strategy: find all <tr> rows, determine type from column headers
    for table in soup.find_all('table'):
        rows = table.find_all('tr')
        if len(rows) < 2:
            continue
        header_cells = rows[0].find_all(['th', 'td'])
        headers = [c.get_text(strip=True).upper() for c in header_cells]

        # ── Batting table: has a 'R' or 'RUNS' column ─────────
        if 'R' in headers or 'RUNS' in headers:
            r_idx = next((i for i,h in enumerate(headers) if h in ('R','RUNS')), None)
            for row in rows[1:]:
                cells = row.find_all('td')
                if len(cells) < 3:
                    continue
                # col 0 = batsman name, col 1 = dismissal, col r_idx = runs
                bat_name  = cells[0].get_text(separator=' ', strip=True)
                dismissal = cells[1].get_text(separator=' ', strip=True) if len(cells) > 1 else ''
                runs = 0
                if r_idx is not None and r_idx < len(cells):
                    raw = re.sub(r'[^\d]', '', cells[r_idx].get_text(strip=True))
                    runs = int(raw) if raw else 0

                p = find_player(bat_name, lookup)
                if p:
                    add(p['player']['full'], 'runs', runs)

                credit = parse_dismissal_text(dismissal)
                if credit:
                    kind, fielder_name = credit
                    fp = find_player(fielder_name, lookup)
                    if fp:
                        add(fp['player']['full'], 'catches' if kind == 'catch' else 'stumpings')

        # ── Bowling table: has a 'W' or 'WKTS' column ─────────
        elif 'W' in headers or 'WKTS' in headers or 'WICKETS' in headers:
            w_idx = next((i for i,h in enumerate(headers) if h in ('W','WKTS','WICKETS')), None)
            for row in rows[1:]:
                cells = row.find_all('td')
                if len(cells) < 3:
                    continue
                bowl_name = cells[0].get_text(separator=' ', strip=True)
                wickets = 0
                if w_idx is not None and w_idx < len(cells):
                    raw = re.sub(r'[^\d]', '', cells[w_idx].get_text(strip=True))
                    wickets = int(raw) if raw else 0
                p = find_player(bowl_name, lookup)
                if p:
                    add(p['player']['full'], 'wickets', wickets)

    return result

def extract_match_date(nd):
    """Pull the match date out of __NEXT_DATA__."""
    try:
        raw = json.dumps(nd)
        for field in ('"startDate"', '"matchDate"', '"date"'):
            m = re.search(field + r'\s*:\s*"(\d{4}-\d{2}-\d{2})', raw)
            if m:
                return m.group(1)
    except Exception:
        pass
    return ''

def extract_match_name(nd):
    """
    Pull a short 'RCB vs SRH' name from __NEXT_DATA__.
    Team abbreviations appear near the inningBatsmen data.
    """
    try:
        raw = json.dumps(nd)
        idx = raw.find('inningBatsmen')
        if idx < 0:
            return ''
        # Look in the 3000 chars before the first innings for team abbreviations
        chunk = raw[max(0, idx - 3000):idx]
        # IPL team codes are 2-4 uppercase letters; exclude country codes (IND, AUS, PAK etc.)
        COUNTRY_CODES = {'IND','AUS','ENG','PAK','NZ','SA','WI','SL','BAN','ZIM','AFG','IRE','SCO','UAE','NED'}
        abbrevs = re.findall(r'"abbreviation"\s*:\s*"([A-Z]{2,4})"', chunk)
        seen_t, unique = set(), []
        for a in abbrevs:
            if a not in seen_t and a not in COUNTRY_CODES:
                seen_t.add(a)
                unique.append(a)
            if len(unique) == 2:
                break
        if len(unique) == 2:
            return f"{unique[0]} vs {unique[1]}"
    except Exception:
        pass
    return ''

def format_date(iso):
    """Convert 2026-03-28 → 28 Mar"""
    if not iso:
        return ''
    try:
        dt = datetime.strptime(iso, '%Y-%m-%d')
        return dt.strftime('%-d %b')
    except Exception:
        return iso

def scrape_scorecard(match, lookup):
    mid  = match['id']
    slug = match['slug']
    url  = f'https://www.espncricinfo.com/series/{SERIES_SLUG}/{slug}/full-scorecard'

    html = get_html(url)
    nd   = extract_next_data(html)

    if nd:
        if not match.get('date'):
            match['date'] = extract_match_date(nd)
        # Always prefer the extracted short name over the slug-derived one
        extracted_name = extract_match_name(nd)
        if extracted_name:
            match['name'] = extracted_name
        result = parse_scorecard_next_data(nd, lookup)
        if result:
            return result, '__NEXT_DATA__'

    # Fallback to HTML tables
    result = parse_scorecard_html_tables(html, lookup)
    return result, 'HTML tables'

# ── Build data.json ───────────────────────────────────────────
def build_output(matches_data, existing_ids=None):
    players = {}
    for team in TEAMS.values():
        for p in team['players']:
            players[p['full']] = {'runs': 0, 'wickets': 0, 'catches': 0, 'stumpings': 0, 'run_outs': 0, 'matches': []}

    for match, stats in matches_data:
        for full_name, delta in stats.items():
            if full_name not in players:
                continue
            s = players[full_name]
            s['runs']      += delta['runs']
            s['wickets']   += delta['wickets']
            s['catches']   += delta['catches']
            s['stumpings'] += delta['stumpings']
            s['run_outs']  += delta.get('run_outs', 0)
            s['matches'].append({
                'name':      match['name'],
                'date':      format_date(match['date']),
                'runs':      delta['runs'],
                'wickets':   delta['wickets'],
                'catches':   delta['catches'],
                'stumpings': delta['stumpings'],
                'run_outs':  delta.get('run_outs', 0),
            })

    teams_out = {}
    for key, team in TEAMS.items():
        teams_out[key] = {
            'displayName': team['displayName'],
            'color':       team['color'],
            'players':     [{'nick': p['nick'], 'full': p['full']} for p in team['players']],
        }

    return {
        'lastUpdated':     datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'matchesLoaded':   len(matches_data),
        'scrapedMatchIds': sorted(
            (existing_ids or set()) | {m['id'] for m, _ in matches_data if m.get('id')}
        ),
        'teams':           teams_out,
        'players':         players,
    }

# ── Load existing data.json ───────────────────────────────────
def load_existing():
    """
    Returns (already_scraped_ids, existing_matches_data) from data.json.
    already_scraped_ids: set of match ID strings to skip this run.
    existing_matches_data: list of (match_meta, stats_dict) to carry forward.
    """
    try:
        with open('data.json') as f:
            existing = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return set(), []

    # Only keep real string IDs — filter out any nulls from previous bugs
    already_ids = {i for i in existing.get('scrapedMatchIds', []) if i}

    # If IDs were corrupted/lost, start fresh to avoid double-counting
    if not already_ids:
        return set(), []

    # Reconstruct per-match stats from each player's match history
    match_stats = {}  # (name, date) → {full_name: stats_dict}
    for full_name, pdata in existing.get('players', {}).items():
        for m in pdata.get('matches', []):
            key = (m['name'], m['date'])
            if key not in match_stats:
                match_stats[key] = {}
            match_stats[key][full_name] = {
                'runs':      m.get('runs', 0),
                'wickets':   m.get('wickets', 0),
                'catches':   m.get('catches', 0),
                'stumpings': m.get('stumpings', 0),
                'run_outs':  m.get('run_outs', 0),  # preserve run outs
            }

    existing_matches_data = [
        ({'id': None, 'name': name, 'date': date}, stats)
        for (name, date), stats in match_stats.items()
    ]

    return already_ids, existing_matches_data

# ── Main ──────────────────────────────────────────────────────
def main():
    lookup = build_lookup()

    # Load what we already have — skip those match IDs this run
    already_ids, existing_matches_data = load_existing()
    print(f"Already scraped: {len(already_ids)} matches")

    matches = get_completed_matches()

    # Separate into new (never seen) and previously-cached
    new_matches = [m for m in matches if m['id'] not in already_ids]

    new_matches_data = []
    live_matches_data = []   # in-progress: re-scraped every run, never cached
    newly_completed_ids = set()

    all_to_scrape = new_matches  # always scrape new ones

    if not new_matches:
        print("No new matches found on results page.")
    else:
        print(f"\nScraping {len(new_matches)} new match(es)...")

    for match in all_to_scrape:
        print(f"  [{match['id']}] {match['name']}")
        try:
            html = SESSION.get(
                f'https://www.espncricinfo.com/series/{SERIES_SLUG}/{match["slug"]}/full-scorecard'
            ).text
            nd = extract_next_data(html)
            complete = is_match_complete(nd)

            if nd:
                if not match.get('date'):
                    match['date'] = extract_match_date(nd)
                extracted_name = extract_match_name(nd)
                if extracted_name:
                    match['name'] = extracted_name

            stats = {}
            if nd:
                stats = parse_scorecard_next_data(nd, lookup)
            if not stats:
                stats, _ = parse_scorecard_html_tables(html, lookup), 'HTML tables'

            hits = [n for n, s in stats.items() if any(s[k] for k in ('runs','wickets','catches','stumpings','run_outs'))]
            status_label = 'complete' if complete else 'LIVE'
            print(f"    → {status_label}: {len(hits)} fantasy players — {', '.join(hits) or 'none'}")

            if stats:
                if complete:
                    new_matches_data.append((match, stats))
                    newly_completed_ids.add(match['id'])
                else:
                    live_matches_data.append((match, stats))
        except Exception as e:
            print(f"    Failed: {e}")
        time.sleep(2)

    # For previously-cached matches, check if any are actually live
    # (edge case: a match was scraped mid-game last run and cached incorrectly)
    # We don't handle this here since we only cache POST matches going forward.

    if live_matches_data:
        print(f"\n{len(live_matches_data)} live match(es) scraped (not cached):")
        for m, _ in live_matches_data:
            print(f"  [{m['id']}] {m['name']} — will re-scrape next run")

    # existing = cached completed; new_matches_data = newly completed; live = current game
    all_matches_data = existing_matches_data + new_matches_data + live_matches_data
    updated_ids = already_ids | newly_completed_ids  # only cache completed matches

    output = build_output(all_matches_data, updated_ids)

    with open('data.json', 'w') as f:
        json.dump(output, f, indent=2)

    print(f"\nDone. {output['matchesLoaded']} total matches in data.json.")
    print("\nFantasy player stats:")
    for name, s in sorted(output['players'].items(), key=lambda x: -(x[1]['runs'] + x[1]['wickets']*25 + x[1]['catches']*10 + x[1]['stumpings']*5 + x[1].get('run_outs',0)*5)):
        pts = s['runs'] + s['wickets']*25 + s['catches']*10 + s['stumpings']*5 + s.get('run_outs',0)*5
        if pts > 0:
            print(f"  {name:30s}  {s['runs']}r  {s['wickets']}w  {s['catches']}c  {s['stumpings']}st  {s.get('run_outs',0)}ro  → {pts}pts")

if __name__ == '__main__':
    main()
