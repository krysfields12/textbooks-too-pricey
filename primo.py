import requests
import requests
import json

API_KEY = "l8xxf07a64b9d807466ab47f987b6545d028"
API_GATEWAY = "https://api-na.hosted.exlibrisgroup.com"

def search_primo(isbn, vid, tab, scope):
    params = {
        "q": f"isbn,exact,{isbn}",
        "vid": vid,
        "tab": tab,
        "scope": scope,
        "apikey": API_KEY,
        "fields": "pnx,delivery,links,control",
        "view": "full",
        "limit": 50,
        "offset": 0,
        "lang": "en",
    }

    url = f"{API_GATEWAY}/primo/v1/search"

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print("ERROR during Primo search:", e)
        return None

def extract_permalink(article):
    pnx = article.get("pnx", {})
    control = pnx.get("control", {})
    recordid_list = control.get("recordid", [])

    if isinstance(recordid_list, list) and recordid_list:
        recordid = recordid_list[0]
        return f"https://usmai-tu.primo.exlibrisgroup.com/permalink/01USMAI_TU/dg0og1/{recordid}"

    return "None"

def filter_book_records(docs):
    """
    Keep only true BOOK-level Alma records.
    """
    book_docs = []
    for d in docs:
        pnx = d.get("pnx", {})
        control = pnx.get("control", {})
        recordid = control.get("recordid", [""])[0]

        # Record type (if present)
        recordtype = control.get("recordtype", [""])[0]

        # 1) Prefer explicit recordtype == 'book'
        if recordtype == "book":
            book_docs.append(d)
            continue

        # 2) Alma bib records normally begin with 'alma...'
        if recordid.startswith("alma"):
            book_docs.append(d)
            continue

    return book_docs

def extract_article_info(article):
    pnx = article.get("pnx", {})
    addata = pnx.get("addata", {})
    display = pnx.get("display", {})
    delivery = article.get("delivery", {})

    # --- Title & authors ---
    title = display.get("title", [""])[0]

    # Prefer display.creator, fall back to addata.au
    authors = display.get("creator", []) or addata.get("au", [])

    # --- Publication (this is what was "Unknown" before) ---
    # Prefer MARC pub (cleaner), then display.publisher, then display.source
    publication = "Unknown"
    if addata.get("pub"):
        publication = addata["pub"][0]
    elif display.get("publisher"):
        publication = display["publisher"][0]
    elif display.get("source"):
        publication = display["source"][0]

    # --- Date & description ---
    if addata.get("date"):
        date = addata["date"][0]
    else:
        date = display.get("creationdate", [""])[0]

    description = display.get("description", [""])[0]

    # --- Permalink (already used in main.py to derive mms_id) ---
    permalink = extract_permalink(article)

    # --- Format type (Print vs eBook) ---
    format_type = "Unknown"

    # Look at format fields + deliveryCategory
    fmt_list = (display.get("format") or []) + (addata.get("format") or [])
    fmt_text = " ".join(fmt_list).lower()

    delivery_cats = [c.lower() for c in (delivery.get("deliveryCategory") or [])]

    # Basic heuristics
    if "online resource" in fmt_text or "electronic" in fmt_text:
        format_type = "eBook"
    elif any("alma-e" in c for c in delivery_cats):
        format_type = "eBook"
    elif any("alma-p" in c for c in delivery_cats):
        format_type = "Print Book"
    elif fmt_list:
        # If it has a physical-looking format description, treat as print
        format_type = "Print Book"
    else:
        # Fallback to description text
        desc_lower = description.lower()
        if "online" in desc_lower or "ebook" in desc_lower or "electronic" in desc_lower:
            format_type = "eBook"

    # --- Location (this was "Unknown Location" before) ---
    location = "Unknown Location"
    bestloc = delivery.get("bestlocation") or {}

    if bestloc:
        main = bestloc.get("mainLocation", "")
        sub = bestloc.get("subLocation", "")
        callnum = bestloc.get("callNumber", "")
        pieces = [p for p in [main, sub, callnum] if p]
        if pieces:
            location = ", ".join(pieces)

    return {
        "title": title,
        "authors": authors,
        "publication": publication,
        "date": date,
        "description": description,
        "permalink": permalink,
        "format_type": format_type,
        "location": location,
    }


def textbook_search_by_isbn_post(isbn, vid, tab, scope, semester=None):
    """
    Returns:
      total_results: int
      enriched_docs: list of article dicts with:
        - original Primo 'pnx', 'delivery', etc.
        - PLUS:
          'title', 'authors', 'publication', 'date',
          'description', 'permalink', 'format_type', 'location'
    """
    data = search_primo(isbn, vid, tab, scope)
    if not data:
        return 0, []

    docs = data.get("docs", [])

    # Filter to real book records (Alma bibs)
    books_only = filter_book_records(docs)

    enriched = []
    for d in books_only:
        # Shallow copy base article (keeps pnx/delivery intact)
        a_copy = d.copy()

        # Extract human-readable fields (publisher, format_type, location, etc.)
        info = extract_article_info(d)

        # Merge them into the article
        a_copy.update(info)

        enriched.append(a_copy)

    return len(enriched), enriched


