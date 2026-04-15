#!/usr/bin/env python3
"""Refresh Ticketmaster event snapshot for concert-radar.html."""
import json
import os
import sys
import time
import urllib.request
import urllib.parse
from datetime import datetime, timedelta, timezone

API_KEY = 'N3MI6yDJM4Gn6clJtZxGxYG81fBCKsIB'
CITY = 'Philadelphia'
COUNTRY = 'US'
MONTHS_AHEAD = 6
PAGE_SIZE = 200
MAX_PAGES = 12


def fetch_page(classification, page):
    now = datetime.now(timezone.utc)
    params = {
        'apikey': API_KEY,
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
    }


def fetch_all(classification):
    is_sports = classification == 'sports'
    results = []
    for page in range(MAX_PAGES):
        data = fetch_page(classification, page)
        events = (data.get('_embedded') or {}).get('events') or []
        if not events:
            break
        results.extend(normalize(ev, is_sports) for ev in events)
        page_info = data.get('page') or {}
        if page + 1 >= (page_info.get('totalPages') or 1):
            break
        time.sleep(0.6)
    return results


def main():
    print('Fetching music...', flush=True)
    music = fetch_all('music')
    print(f'  \u2192 {len(music)} events')
    print('Fetching sports...', flush=True)
    sports = fetch_all('sports')
    print(f'  \u2192 {len(sports)} events')

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
