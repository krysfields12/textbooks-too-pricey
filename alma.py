import requests
import xml.etree.ElementTree as ET

# Alma API Configuration
BASE_URL = "https://api-na.hosted.exlibrisgroup.com/almaws/v1"
API_KEY = "l8xx05045e3791e143d5ac4288a1ef850719"
HEADERS = {"Accept": "application/xml", "Authorization": f"apikey {API_KEY}"}

def safe_get(url, headers, timeout=10):
    try:
        return requests.get(url, headers=headers, timeout=timeout)
    except requests.exceptions.Timeout:
        print(f"TIMEOUT for URL: {url}\nRetrying once...")
        try:
            return requests.get(url, headers=headers, timeout=timeout)
        except Exception as e:
            print(f"FAILED AFTER RETRY for URL: {url}\nError: {e}")
            raise

def fetch_portfolios_by_mms_id(mms_id):
    portfolios_endpoint = f"{BASE_URL}/bibs/{mms_id}/portfolios?view=full"

    try:
        print(f"\n====== Fetching Portfolios for MMS ID: {mms_id} ======")

        response = safe_get(portfolios_endpoint, headers=HEADERS)
        response.raise_for_status()

        # Debug raw XML
        print(f"DEBUG XML Response for MMS ID {mms_id}:\n{response.text}")

        root = ET.fromstring(response.text)

        portfolios = root.findall(".//portfolio")
        print(f"Found {len(portfolios)} portfolios for MMS ID {mms_id}")

        if not portfolios:
            print(f"No portfolios found for MMS ID {mms_id}")
            return {}

        detailed_portfolio_data = {}

        for portfolio in portfolios:

            # Debug raw element
            print(f"\n--- Portfolio XML ---\n{ET.tostring(portfolio, encoding='unicode')}")

            # Portfolio ID
            portfolio_id_elem = portfolio.find("id")
            portfolio_id = portfolio_id_elem.text if portfolio_id_elem is not None else "Unknown"

            # Resource Metadata
            resource_meta = portfolio.find("resource_metadata")
            title_elem = resource_meta.find("title") if resource_meta is not None else None
            title = title_elem.text if title_elem is not None else "Unknown"

            # Collection ID
            electronic_collection = portfolio.find("electronic_collection")
            collection_id_elem = electronic_collection.find("id") if electronic_collection is not None else None
            collection_id = collection_id_elem.text if collection_id_elem is not None else "Unknown"

            # Availability
            availability_elem = portfolio.find("availability")
            availability_desc = (
                availability_elem.attrib.get("desc", "Unknown") if availability_elem is not None
                else "Unknown"
            )

            # Public Note
            public_note_elem = portfolio.find("public_note")
            public_note_text = public_note_elem.text if public_note_elem is not None else None

            # If missing: fetch detailed data
            if not public_note_text:
                print(f"⚠️ Missing public note in bulk response. Fetching details for portfolio {portfolio_id}...")
                details = fetch_portfolio_details(mms_id, portfolio_id)
                public_note_text = details.get("public_note", "Not Found")

            # License Mapping
            license = map_license_terms(public_note_text)

            # Debug summary
            print(f"Extracted portfolio info:")
            print(f"  Portfolio ID: {portfolio_id}")
            print(f"  Title: {title}")
            print(f"  Collection ID: {collection_id}")
            print(f"  Availability: {availability_desc}")
            print(f"  Public Note: {public_note_text}")
            print(f"  License: {license}")

            detailed_portfolio_data[portfolio_id] = {
                "portfolio_id": portfolio_id,
                "title": title,
                "electronic_collection_id": collection_id,
                "availability": availability_desc,
                "public_note": public_note_text,
                "license": license,
            }

        return detailed_portfolio_data

    except requests.RequestException as e:
        print(f"Request error while fetching portfolios for MMS ID {mms_id}: {e}")
        log_failure("failed_mms_ids.txt", mms_id)
        return {}

    except ET.ParseError as e:
        print(f"XML parsing error for MMS ID {mms_id}: {e}")
        log_failure("failed_mms_ids.txt", mms_id)
        return {}

def fetch_portfolio_details(mms_id, portfolio_id):
    portfolio_endpoint = f"{BASE_URL}/bibs/{mms_id}/portfolios/{portfolio_id}"

    try:
        print(f"\nFetching Detailed Portfolio {portfolio_id} for MMS ID {mms_id}")

        response = safe_get(portfolio_endpoint, headers=HEADERS)
        response.raise_for_status()

        print(f"DEBUG Detailed Portfolio XML:\n{response.text}")

        root = ET.fromstring(response.text)

        # Public Note
        public_note_elem = root.find(".//public_note")
        public_note = (
            public_note_elem.text.strip()
            if public_note_elem is not None and public_note_elem.text
            else "Not Found"
        )

        # Title
        title_elem = root.find(".//resource_metadata/title")
        title = title_elem.text.strip() if title_elem is not None else "Unknown"

        # License mapping
        license = map_license_terms(public_note)

        print(f"Detailed Extracted Data:")
        print(f"  Public Note: {public_note}")
        print(f"  Title: {title}")
        print(f"  License: {license}")

        return {
            "public_note": public_note,
            "license": license,
            "title": title,
        }

    except requests.RequestException as e:
        print(f"Error fetching portfolio details (MMS {mms_id}, Portfolio {portfolio_id}): {e}")
        log_failure("failed_portfolio_ids.txt", f"{mms_id}:{portfolio_id}")
        return {"public_note": "Not Found", "license": "Unknown", "title": "Unknown"}

    except ET.ParseError as e:
        print(f"XML parsing error for detailed portfolio {portfolio_id}: {e}")
        log_failure("failed_portfolio_ids.txt", f"{mms_id}:{portfolio_id}")
        return {"public_note": "Not Found", "license": "Unknown", "title": "Unknown"}


def map_license_terms(public_note):
    if not public_note:
        return "Unknown"
    note = public_note.lower()
    if "unlimited" in note:
        return "unlimited"
    if any(term in note for term in ["one", "three", "four", "six"]):
        return "limited"
    return "Unknown"


def log_failure(filename, value):
    with open(filename, "a") as f:
        f.write(str(value) + "\n")



if __name__ == "__main__":
    test_mms = "9963902671508249"  # sample MMS ID
    print(fetch_portfolios_by_mms_id(test_mms))

