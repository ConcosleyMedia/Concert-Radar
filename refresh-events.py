#!/usr/bin/env python3
"""Refresh Ticketmaster event snapshot, enrich with Spotify + optional Claude price search."""
import base64
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone


def load_env():
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
    if not os.path.exists(path):
        return
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            k, v = line.split('=', 1)
            os.environ.setdefault(k.strip(), v.strip())


load_env()

TM_KEY = os.environ.get('TICKETMASTER_API_KEY', '').strip()
SPOTIFY_ID = os.environ.get('SPOTIFY_CLIENT_ID', '').strip()
SPOTIFY_SECRET = os.environ.get('SPOTIFY_CLIENT_SECRET', '').strip()
ANTHROPIC_KEY = os.environ.get('ANTHROPIC_API_KEY', '').strip()
MAX_PRICE_SEARCHES_PER_RUN = 40

CITY = 'Philadelphia'
COUNTRY = 'US'
MONTHS_AHEAD = 6
PAGE_SIZE = 200
MAX_PAGES = 12


def fetch_tm_page(classification, page):
    now = datetime.now(timezone.utc)
    params = {
        'apikey': TM_KEY,
        'city': CITY,
        'classificationName': classification,
        'countryCode': COUNTRY,
        'size': str(PAGE_SIZE),
        'page': str(page),
        'sort': 'date,asc',
        'startDateTime': now.strftime('%Y-%m-%dT%H:%M:%SZ'),
        'endDateTime': (now + timedelta(days=30 * MONTHS_AHEAD)).strftime('%Y-%m-%dT%H:%M:%SZ'),
    }
    url = 'https://app.ticketmaster.com/discovery/v2/events.json?' + urllib.parse.urlencode(params)
    with urllib.request.urlopen(url, timeout=30) as resp:
        return json.loads(resp.read())


def normalize(e, is_sports):
    venue = (e.get('_embedded', {}).get('venues') or [{}])[0]
    attractions = e.get('_embedded', {}).get('attractions') or []
    attraction = attractions[0] if attractions else {}
    performer_name = attraction.get('name') or e.get('name')

    imgs = attraction.get('images') or e.get('images') or []
    ranked = sorted(imgs, key=lambda i: -(i.get('width') or 0))
    best = next(
        (i for i in ranked if i.get('ratio') == '16_9' and (i.get('width') or 0) >= 640),
        ranked[0] if ranked else None,
    )

    dates = (e.get('dates') or {}).get('start', {})
    datetime_local = dates.get('dateTime')
    if not datetime_local and dates.get('localDate'):
        datetime_local = f"{dates['localDate']}T{dates.get('localTime') or '20:00:00'}"

    prs = e.get('priceRanges') or []
    lows = [p['min'] for p in prs if p.get('min') is not None]
    highs = [p['max'] for p in prs if p.get('max') is not None]
    lowest = min(lows) if lows else None
    highest = max(highs) if highs else None

    cls = (e.get('classifications') or [{}])[0]
    genre = (cls.get('genre') or {}).get('name') or 'Other'
    sub_genre = (cls.get('subGenre') or {}).get('name') or ''

    return {
        'id': e.get('id'),
        'url': e.get('url'),
        'type': 'sports' if is_sports else 'music',
        'genre': genre,
        'subGenre': sub_genre,
        'league': (sub_genre or genre) if is_sports else None,
        'datetime_local': datetime_local,
        'venue': {'name': venue.get('name') or 'Venue TBA'},
        'stats': {
            'lowest_price': round(lowest) if lowest is not None else None,
            'highest_price': round(highest) if highest is not None else None,
        },
        'performers': [{
            'name': (e.get('name') or performer_name) if is_sports else performer_name,
            'short_name': performer_name,
            'image': best.get('url') if best else None,
        }],
        'spotifyTrackId': None,
        'searchedPrice': None,
    }


def fetch_all_tm(classification):
    is_sports = classification == 'sports'
    results = []
    for page in range(MAX_PAGES):
        data = fetch_tm_page(classification, page)
        events = (data.get('_embedded') or {}).get('events') or []
        if not events:
            break
        results.extend(normalize(ev, is_sports) for ev in events)
        page_info = data.get('page') or {}
        if page + 1 >= (page_info.get('totalPages') or 1):
            break
        time.sleep(0.6)
    return results


# ── SPOTIFY ───────────────────────────────────────────
def spotify_token():
    print(f'  (Spotify creds: id={len(SPOTIFY_ID)} chars, secret={len(SPOTIFY_SECRET)} chars)', flush=True)
    creds = base64.b64encode(f'{SPOTIFY_ID}:{SPOTIFY_SECRET}'.encode()).decode()
    req = urllib.request.Request(
        'https://accounts.spotify.com/api/token',
        data=b'grant_type=client_credentials',
        headers={
            'Authorization': f'Basic {creds}',
            'Content-Type': 'application/x-www-form-urlencoded',
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read())['access_token']
    except urllib.error.HTTPError as ex:
        body = ex.read().decode(errors='replace')
        raise RuntimeError(f'HTTP {ex.code}: {body[:300]}')


class SpotifyRateLimit(Exception):
    pass


def spotify_lookup(artist_name, token, cache):
    key = artist_name.strip().lower()
    if key in cache:
        return cache[key]
    q = urllib.parse.quote(f'artist:"{artist_name}"')
    url = f'https://api.spotify.com/v1/search?q={q}&type=track&limit=1&market=US'
    try:
        req = urllib.request.Request(url, headers={'Authorization': f'Bearer {token}'})
        with urllib.request.urlopen(req, timeout=15) as resp:
            tracks = json.loads(resp.read()).get('tracks', {}).get('items', [])
        track_id = tracks[0]['id'] if tracks else None
        cache[key] = track_id
        return track_id
    except urllib.error.HTTPError as ex:
        if ex.code == 429:
            wait = int(ex.headers.get('Retry-After', '10'))
            raise SpotifyRateLimit(wait)
        print(f'  spotify miss for {artist_name}: {ex}', flush=True)
        cache[key] = None
        return None
    except Exception as ex:
        print(f'  spotify miss for {artist_name}: {ex}', flush=True)
        cache[key] = None
        return None


def enrich_with_spotify(events):
    if not SPOTIFY_ID or not SPOTIFY_SECRET:
        print('Skipping Spotify enrichment (no creds).')
        return
    needed = [e for e in events if not e.get('spotifyTrackId')]
    existing_hits = sum(1 for e in events if e.get('spotifyTrackId'))
    print(f'Spotify: {existing_hits} already enriched, {len(needed)} to fetch...', flush=True)
    if not needed:
        return
    try:
        token = spotify_token()
    except Exception as ex:
        print(f'Spotify token request failed: {ex}. Skipping enrichment.', flush=True)
        return
    cache = {}
    hits = 0
    rl_count = 0
    for i, ev in enumerate(needed):
        name = ev['performers'][0]['short_name']
        try:
            tid = spotify_lookup(name, token, cache)
            rl_count = 0
            if tid:
                ev['spotifyTrackId'] = tid
                hits += 1
        except SpotifyRateLimit as rl:
            rl_count += 1
            wait = min(int(str(rl)) if str(rl).isdigit() else 10, 15)
            print(f'  rate-limited, sleeping {wait}s ({rl_count} consecutive)', flush=True)
            time.sleep(wait)
            if rl_count >= 3:
                print(f'  bailing on Spotify after {rl_count} consecutive 429s; {hits} new hits this pass', flush=True)
                break
            continue
        if i and i % 25 == 0:
            print(f'  {i}/{len(needed)} processed, {hits} new hits', flush=True)
        time.sleep(0.25)
    print(f'  Spotify: {hits} new hits ({existing_hits + hits}/{len(events)} total)')


# ── CLAUDE PRICE SEARCH ───────────────────────────────
def enrich_with_claude_prices(events):
    if not ANTHROPIC_KEY:
        print('Skipping Claude price search (no ANTHROPIC_API_KEY).')
        return
    try:
        import anthropic
    except ImportError:
        print('anthropic SDK not installed - skipping price search.')
        return
    client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
    candidates = [e for e in events if not e['stats']['lowest_price'] and not e.get('searchedPrice') and e.get('datetime_local')]
    to_search = candidates[:MAX_PRICE_SEARCHES_PER_RUN]
    print(f'Searching prices for {len(to_search)}/{len(candidates)} events via Claude web search (cap {MAX_PRICE_SEARCHES_PER_RUN}/run)...', flush=True)
    hits = 0
    rate_limit_hits = 0
    for i, ev in enumerate(to_search):
        artist = ev['performers'][0]['name']
        venue = ev['venue']['name']
        date = ev['datetime_local'][:10] if ev.get('datetime_local') else ''
        prompt = (
            f'Find the current ticket price range for {artist} at {venue}, Philadelphia on {date}. '
            'Respond with ONLY the price range as: LOW-HIGH (numbers only, no $ or words). '
            'If unavailable, respond exactly: UNAVAILABLE'
        )
        try:
            msg = client.messages.create(
                model='claude-haiku-4-5',
                max_tokens=150,
                tools=[{'type': 'web_search_20250305', 'name': 'web_search', 'max_uses': 1}],
                messages=[{'role': 'user', 'content': prompt}],
            )
            text = ''
            for block in msg.content:
                if hasattr(block, 'text'):
                    text += block.text
            text = text.strip()
            rate_limit_hits = 0
            if text and text != 'UNAVAILABLE' and '-' in text:
                parts = [p.strip() for p in text.split('-', 1)]
                lo = int(''.join(c for c in parts[0] if c.isdigit()) or 0)
                hi = int(''.join(c for c in parts[1] if c.isdigit()) or 0)
                if lo and hi:
                    ev['searchedPrice'] = {'low': lo, 'high': hi}
                    hits += 1
        except Exception as ex:
            err = str(ex)
            if '429' in err or 'rate_limit' in err:
                rate_limit_hits += 1
                print(f'  rate-limited on {artist}, sleeping 30s', flush=True)
                time.sleep(30)
                if rate_limit_hits >= 3:
                    print(f'  bailing after {rate_limit_hits} rate limits', flush=True)
                    break
                continue
            print(f'  price miss for {artist}: {err[:120]}', flush=True)
        if i and i % 5 == 0:
            print(f'  {i}/{len(to_search)} processed, {hits} hits', flush=True)
        time.sleep(3)
    print(f'  Claude prices: {hits}/{len(to_search)} resolved')


def load_existing():
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'events.json')
    if not os.path.exists(path):
        return {}
    try:
        with open(path) as f:
            data = json.load(f)
        by_id = {}
        for e in (data.get('music') or []) + (data.get('sports') or []):
            by_id[e['id']] = e
        return by_id
    except Exception:
        return {}


def carry_over_enrichment(events, existing):
    for ev in events:
        prev = existing.get(ev['id'])
        if prev:
            if prev.get('spotifyTrackId') and not ev.get('spotifyTrackId'):
                ev['spotifyTrackId'] = prev['spotifyTrackId']
            if prev.get('searchedPrice') and not ev.get('searchedPrice'):
                ev['searchedPrice'] = prev['searchedPrice']


def main():
    if not TM_KEY:
        print('TICKETMASTER_API_KEY is required (set in .env or env).', file=sys.stderr)
        sys.exit(1)

    existing = load_existing()
    print(f'Loaded {len(existing)} existing enriched events for carry-over.', flush=True)

    print('Fetching music...', flush=True)
    music = fetch_all_tm('music')
    print(f'  -> {len(music)} events')
    print('Fetching sports...', flush=True)
    sports = fetch_all_tm('sports')
    print(f'  -> {len(sports)} events')

    carry_over_enrichment(music, existing)
    carry_over_enrichment(sports, existing)

    enrich_with_spotify(music)
    enrich_with_claude_prices(music)

    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'events.json')
    with open(out_path, 'w') as f:
        json.dump({
            'generatedAt': datetime.now(timezone.utc).isoformat(),
            'city': CITY,
            'monthsAhead': MONTHS_AHEAD,
            'music': music,
            'sports': sports,
        }, f, indent=2)
    print(f'Wrote {out_path}')


if __name__ == '__main__':
    try:
        main()
    except urllib.error.HTTPError as e:
        print(f'HTTP {e.code}: {e.reason}', file=sys.stderr)
        sys.exit(1)
