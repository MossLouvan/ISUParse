import json
import re
import sqlite3
import time
from datetime import datetime
from typing import Any, Dict, List, Set
import requests

BASE = "https://www.dining.iastate.edu/wp-json/dining/menu-hours"
WARMUP = "https://www.dining.iastate.edu/hours-menus/"

MENU_TIME = 1771903500  

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/122.0.0.0 Safari/537.36")

DB_PATH = "/Users/mosslouvan/Documents/ISUParse/isu_dining.db"


def iso_date_from_epoch(ts: int) -> str:
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d")


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.clear()
    s.headers.update({
        "User-Agent": UA,
        "Referer": WARMUP,
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
    })
    s.get(WARMUP, timeout=30)
    return s


def fetch_text(session: requests.Session, url: str) -> str:
    r = session.get(url, timeout=30)
    r.raise_for_status()
    return r.text


def fetch_json(session: requests.Session, url: str) -> Any:
    r = session.get(url, timeout=30)
    if r.status_code == 403:
        raise RuntimeError(f"403 Forbidden for {url}\nBody: {r.text[:300]}")
    r.raise_for_status()
    return r.json()


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript("""
    PRAGMA foreign_keys = ON;

    CREATE TABLE IF NOT EXISTS locations (
      id INTEGER PRIMARY KEY,
      slug TEXT UNIQUE,
      title TEXT,
      facility TEXT,
      location_type TEXT,
      address TEXT,
      lat REAL,
      lng REAL
    );

    CREATE TABLE IF NOT EXISTS menu_items (
      item_id INTEGER PRIMARY KEY AUTOINCREMENT,
      location_id INTEGER NOT NULL,
      menu_section TEXT,
      station TEXT,
      category TEXT,
      name TEXT,
      serving_size TEXT,
      calories INTEGER,
      ingredients TEXT,
      is_halal INTEGER,
      is_vegetarian INTEGER,
      is_vegan INTEGER,
      fetched_for_date TEXT,
      fetched_at INTEGER,
      FOREIGN KEY(location_id) REFERENCES locations(id)
    );

    CREATE INDEX IF NOT EXISTS idx_menu_items_loc_date
      ON menu_items(location_id, fetched_for_date);

    CREATE TABLE IF NOT EXISTS item_nutrients (
      item_id INTEGER NOT NULL,
      name TEXT NOT NULL,
      qty TEXT,
      rounded_qty TEXT,
      percent TEXT,
      FOREIGN KEY(item_id) REFERENCES menu_items(item_id)
    );

    CREATE INDEX IF NOT EXISTS idx_item_nutrients_item
      ON item_nutrients(item_id);

    CREATE TABLE IF NOT EXISTS item_traits (
      item_id INTEGER NOT NULL,
      oid TEXT,
      name TEXT,
      type_name TEXT,
      FOREIGN KEY(item_id) REFERENCES menu_items(item_id)
    );

    CREATE INDEX IF NOT EXISTS idx_item_traits_item
      ON item_traits(item_id);
    """)


def upsert_location(conn: sqlite3.Connection, loc: Dict[str, Any]) -> None:
    conn.execute("""
      INSERT INTO locations (id, slug, title, facility, location_type, address, lat, lng)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(id) DO UPDATE SET
        slug=excluded.slug,
        title=excluded.title,
        facility=excluded.facility,
        location_type=excluded.location_type,
        address=excluded.address,
        lat=excluded.lat,
        lng=excluded.lng
    """, (
        int(loc["id"]),
        loc.get("slug"),
        loc.get("title"),
        loc.get("facility"),
        json.dumps(loc.get("locationType") or []),
        loc.get("address"),
        loc.get("lat"),
        loc.get("lng"),
    ))


def parse_json_string_list(s: Any) -> List[Dict[str, Any]]:
    if s is None:
        return []
    if isinstance(s, list):
        return s
    if isinstance(s, str):
        s = s.strip()
        if not s:
            return []
        try:
            parsed = json.loads(s)
            return parsed if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            return []
    return []


def scrape_location_slugs_from_sitemap(session: requests.Session) -> List[str]:
    """
    Pull *all* /location/<slug>/ URLs from WordPress sitemap.
    """
    sitemap_urls = [
        "https://www.dining.iastate.edu/wp-sitemap.xml",
        "https://www.dining.iastate.edu/sitemap.xml",
        "https://www.dining.iastate.edu/sitemap_index.xml",
    ]

    index_xml = None
    used = None
    for u in sitemap_urls:
        try:
            index_xml = fetch_text(session, u)
            used = u
            break
        except Exception:
            continue

    if not index_xml:
        raise RuntimeError("Could not fetch any sitemap index")

    print("Using sitemap:", used)

    locs = re.findall(r"<loc>\s*([^<\s]+)\s*</loc>", index_xml)

    # Sometimes the index already contains content URLs
    direct_location_urls = [u for u in locs if "/location/" in u]
    if direct_location_urls:
        slugs = sorted({u.split("/location/")[1].split("/")[0] for u in direct_location_urls})
        return slugs

    # Otherwise: treat locs as sitemap files and scan them
    sitemap_files = [u for u in locs if u.endswith(".xml")]

    slugs: Set[str] = set()
    for sm in sitemap_files:
        try:
            body = fetch_text(session, sm)
        except Exception:
            continue

        urls = re.findall(r"<loc>\s*([^<\s]+)\s*</loc>", body)
        for u in urls:
            if "/location/" in u:
                slugs.add(u.split("/location/")[1].split("/")[0])

    return sorted(slugs)


def ingest_all(db_path: str = DB_PATH) -> None:
    session = make_session()

    ts = MENU_TIME
    day = iso_date_from_epoch(ts)
    print(f"Using menu timestamp: {ts} (date={day})")

    slugs = scrape_location_slugs_from_sitemap(session)
    print(f"Found {len(slugs)} location slugs from sitemap")

    conn = sqlite3.connect(db_path)
    try:
        ensure_schema(conn)

        total_locations = 0
        total_items = 0

        for i, slug in enumerate(slugs, start=1):
            url = f"{BASE}/get-single-location/?slug={slug}&time={ts}"

            try:
                data = fetch_json(session, url)
            except Exception as e:
                print(f"[WARN] ({i}/{len(slugs)}) slug={slug} failed: {e}")
                continue

            if not (isinstance(data, list) and data):
                print(f"[WARN] ({i}/{len(slugs)}) slug={slug} empty payload")
                continue

            payload = data[0]
            menus = payload.get("menus") or []

            # insert/update location
            upsert_location(conn, payload)
            total_locations += 1

            loc_id = int(payload["id"])
            title = payload.get("title") or slug

            # deterministic reruns
            conn.execute(
                "DELETE FROM menu_items WHERE location_id=? AND fetched_for_date=?",
                (loc_id, day),
            )

            inserted_here = 0

            for menu in menus:
                section = menu.get("section")
                for display in menu.get("menuDisplays") or []:
                    station = display.get("name")
                    for cat in display.get("categories") or []:
                        category = cat.get("category")
                        for item in cat.get("menuItems") or []:
                            cals_raw = item.get("totalCal")
                            try:
                                calories = int(float(cals_raw)) if cals_raw not in (None, "", "0") else 0
                            except ValueError:
                                calories = 0

                            cur = conn.execute("""
                              INSERT INTO menu_items (
                                location_id, menu_section, station, category, name,
                                serving_size, calories, ingredients,
                                is_halal, is_vegetarian, is_vegan,
                                fetched_for_date, fetched_at
                              ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, (
                                loc_id,
                                section,
                                station,
                                category,
                                item.get("name"),
                                item.get("servingSize"),
                                calories,
                                item.get("ingredients"),
                                int(item.get("isHalal") or 0),
                                int(item.get("isVegetarian") or 0),
                                int(item.get("isVegan") or 0),
                                day,
                                ts
                            ))
                            item_id = cur.lastrowid
                            inserted_here += 1

                            for n in parse_json_string_list(item.get("nutrients")):
                                conn.execute("""
                                  INSERT INTO item_nutrients (item_id, name, qty, rounded_qty, percent)
                                  VALUES (?, ?, ?, ?, ?)
                                """, (
                                    item_id,
                                    n.get("name"),
                                    str(n.get("qty")) if n.get("qty") is not None else None,
                                    str(n.get("roundedQty")) if n.get("roundedQty") is not None else None,
                                    str(n.get("roundedPercentOfGoal")) if n.get("roundedPercentOfGoal") is not None else None,
                                ))

                            for t in parse_json_string_list(item.get("traits")):
                                conn.execute("""
                                  INSERT INTO item_traits (item_id, oid, name, type_name)
                                  VALUES (?, ?, ?, ?)
                                """, (
                                    item_id,
                                    str(t.get("oid")) if t.get("oid") is not None else None,
                                    t.get("name"),
                                    t.get("typeName"),
                                ))

            conn.commit()
            total_items += inserted_here

            print(f"[OK] ({i}/{len(slugs)}) {title} ({slug}) menus={len(menus)} items={inserted_here}")

            time.sleep(0.15)

        # summary counts
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM locations")
        loc_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM menu_items")
        item_count = cur.fetchone()[0]

        print("-----")
        print(f"Locations upserted this run: {total_locations}")
        print(f"Menu items inserted:         {total_items}")
        print(f"DB totals -> locations={loc_count}, menu_items={item_count}")
        print(f"DB saved to: {db_path}")

    finally:
        conn.close()


if __name__ == "__main__":
    ingest_all()