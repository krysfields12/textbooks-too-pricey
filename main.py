import requests
import json
import alma
import primo
import parse_csv
import math
import argparse
import pandas as pd
import os
import time
from datetime import datetime
import traceback

# Set up argument parser
parser = argparse.ArgumentParser(description="Process CSV input file for Primo and Alma enrichment")
parser.add_argument("input", metavar="I", type=str, help="Path to the CSV input file")
parser.add_argument("semester", metavar="S", type=str, help="Semester and year (e.g., S25)")
args = parser.parse_args()

# Process CSV input file
unique_isbn, indata = parse_csv.process(args.input, args.semester)
res_dict = {}

# Debug: Check initial columns in indata
print("DEBUG: Columns in indata:", indata.columns)

# Primo API parameters
vid = "01USMAI_TU:01USMAI_TU"
tab = "TULibraryCatalog"
scope = "MyInst_and_CI"
API_KEY = "l8xxf07a64b9d807466ab47f987b6545d028"

# Helper Functions
def safe_convert_to_int(item):
    try:
        # Handle NaN floats
        if isinstance(item, float) and math.isnan(item):
            return None
        # Normalize to string and strip
        s = str(item).strip()
        if s == "":
            return None
        return int(s)
    except (ValueError, TypeError):
        return None

def extract_user_limit(public_note):
    number_mapping = {
        "one": "1", "two": "2", "three": "3", "four": "4", "five": "5",
        "six": "6", "seven": "7", "eight": "8", "nine": "9", "ten": "10"
    }
    words = public_note.lower().split()
    for word in words:
        if word in number_mapping:
            return number_mapping[word]
    return ""

def safe_get_location(article_list, index):
    if index < len(article_list):
        location = article_list[index].get("location", "Unknown Location")
        return location
    return "None"

def enrich_article_with_default(article):
    article["license"] = article.get("license", "")
    article["public_note"] = article.get("public_note", "")
    article["field_user_limit"] = extract_user_limit(article["public_note"])
    article["publication"] = article.get("publication", "Unknown")
    article["permalink"] = article.get("permalink", "None")
    article["format_type"] = article.get("format_type", "Unknown")
    article["location"] = article.get("location", "Unknown Location")
    return article

def enrich_article_with_actual_values(article, portfolio_data):
    article["license"] = portfolio_data.get("license", "")
    article["public_note"] = portfolio_data.get("public_note", "")
    article["field_user_limit"] = extract_user_limit(portfolio_data.get("public_note", ""))

    delivery = article.get("delivery", {})
    if not delivery:
        article["location"] = "Unknown"
        return article

    best_location = delivery.get("bestlocation", {})
    if not best_location:
        article["location"] = "Best Location Missing"
        return article

    main_location = best_location.get("mainLocation", "Unknown")
    sub_location = best_location.get("subLocation", "Unknown")
    call_number = best_location.get("callNumber", "Unknown")

    article["location"] = f"{main_location}, {sub_location}, {call_number}".strip(", ")
    return article

def extract_multiple_versions(article, vid):
    try:
        pnx = article.get('pnx', {})
        facets = pnx.get('facets', {})
        frbrgroupid = facets.get('frbrgroupid', [None])[0]

        if not frbrgroupid:
            return ""

        group_query = (
            f"https://api-na.hosted.exlibrisgroup.com/primo/v1/search?"
            f"q=frbrgroupid,exact,{frbrgroupid}&vid={vid}&apikey={API_KEY}"
        )

        response = requests.get(group_query, timeout=10)
        response.raise_for_status()
        group_results = response.json()
        total_results = group_results.get('info', {}).get('total', 0)

        if total_results > 1:
            return "Multiple Versions"

    except Exception as e:
        print(f"Error checking multiple versions: {e}")
    return ""

print("Fetching results from Primo...")
mms_to_isbn = {}

for idx, item in enumerate(unique_isbn):
    print(f"Processing ISBN {idx + 1}/{len(unique_isbn)}: {item}")
    item_start = time.time()

    converted_item = safe_convert_to_int(item)
    if converted_item is None:
        print(f"  Skipping ISBN (cannot convert): {item}")
        continue

    item_str = str(converted_item)

    # ALWAYS initialize this ISBN entry so we never get KeyError later
    res_dict[item_str] = {
        "ArticlesInfo": [],
        "TotalResults": 0
    }

    try:
        total_results, articles_info = primo.textbook_search_by_isbn_post(
            item_str, vid, tab, scope, args.semester
        )

        # Store Primo's total result count (before filtering)
        res_dict[item_str]["TotalResults"] = total_results

        if not articles_info:
            # No TU-owned or no usable docs after filtering
            print(f"  No TU-owned / usable results for ISBN {item_str}")
            print(f"  Primo total results (all scopes/formats): {total_results}")
            continue

        for article in articles_info:
            permalink = article.get("permalink", "").strip()
            mms_id = None

            if permalink:
                possible_id = permalink.split("/")[-1]
                mms_id = possible_id.replace("alma", "") if "alma" in possible_id else possible_id

            # If we can extract an MMS ID, try Alma enrichment
            if mms_id and mms_id.isdigit():
                mms_to_isbn[mms_id] = item_str
                detailed_portfolio_data = alma.fetch_portfolios_by_mms_id(mms_id)

                if detailed_portfolio_data:
                    for _, details in detailed_portfolio_data.items():
                        enriched_article = enrich_article_with_actual_values(article.copy(), details)
                        enriched_article["multiple_versions"] = extract_multiple_versions(article, vid)
                        res_dict[item_str]["ArticlesInfo"].append(enriched_article)
                else:
                    enriched_article = enrich_article_with_default(article.copy())
                    enriched_article["multiple_versions"] = extract_multiple_versions(article, vid)
                    res_dict[item_str]["ArticlesInfo"].append(enriched_article)

            else:
                # No MMS ID, just keep the Primo data
                enriched_article = enrich_article_with_default(article.copy())
                enriched_article["multiple_versions"] = extract_multiple_versions(article, vid)
                res_dict[item_str]["ArticlesInfo"].append(enriched_article)

        print(f"  Processed ISBN {item_str} in {time.time() - item_start:.2f} seconds")

    except Exception as e:
        print(f"Error during Primo search for ISBN {item_str}: {e}")
        print(traceback.format_exc())
        # res_dict[item_str] still exists with default TotalResults = 0 and empty ArticlesInfo

# Save JSON
timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
json_output_file = f"data/primo_alma_enriched_{args.semester}_{timestamp}.json"
os.makedirs("data", exist_ok=True)
with open(json_output_file, "w", encoding="utf-8") as json_file:
    json.dump(res_dict, json_file, ensure_ascii=False, indent=4)
print(f"DEBUG: JSON output saved to: {json_output_file}")

# Map to DataFrame: ISBN -> list of ArticlesInfo
article_info_list = {
    isbn: results.get("ArticlesInfo", [])
    for isbn, results in res_dict.items()
}

# Add Primo result columns to indata (top 5 results)
for i in range(5):
    n = i + 1
    indata[f"Result_{n}_Permalink"] = indata["ISBN"].map(
        lambda x: article_info_list.get(str(safe_convert_to_int(x)), [{}])[i].get("permalink", "None")
        if i < len(article_info_list.get(str(safe_convert_to_int(x)), [])) else "None"
    )
    indata[f"Result_{n}_Publication"] = indata["ISBN"].map(
        lambda x: article_info_list.get(str(safe_convert_to_int(x)), [{}])[i].get("publication", "Unknown")
        if i < len(article_info_list.get(str(safe_convert_to_int(x)), [])) else "None"
    )
    indata[f"Result_{n}_FormatType"] = indata["ISBN"].map(
        lambda x: article_info_list.get(str(safe_convert_to_int(x)), [{}])[i].get("format_type", "Unknown")
        if i < len(article_info_list.get(str(safe_convert_to_int(x)), [])) else "None"
    )
    indata[f"Result_{n}_License"] = indata["ISBN"].map(
        lambda x: article_info_list.get(str(safe_convert_to_int(x)), [{}])[i].get("license", "")
        if i < len(article_info_list.get(str(safe_convert_to_int(x)), [])) else "None"
    )
    indata[f"Result_{n}_FieldUserLimit"] = indata["ISBN"].map(
        lambda x: article_info_list.get(str(safe_convert_to_int(x)), [{}])[i].get("field_user_limit", "")
        if i < len(article_info_list.get(str(safe_convert_to_int(x)), [])) else "None"
    )
    indata[f"Result_{n}_Location"] = indata["ISBN"].map(
        lambda x: safe_get_location(
            article_info_list.get(str(safe_convert_to_int(x)), [{}]), i
        )
    )
    indata[f"Result_{n}_MultipleVersions"] = indata["ISBN"].map(
        lambda x: article_info_list.get(str(safe_convert_to_int(x)), [{}])[i].get("multiple_versions", "")
        if i < len(article_info_list.get(str(safe_convert_to_int(x)), [])) else "None"
    )

# Add Primo Total Results
if "PRIMO_Total_Results" not in indata.columns:
    indata["PRIMO_Total_Results"] = indata["ISBN"].map(
        lambda x: res_dict.get(str(safe_convert_to_int(x)), {}).get("TotalResults", 0)
    )

# Sort and export (smallest total results first)
indata = indata.sort_values(by="PRIMO_Total_Results", ascending=True)
output_csv_file = parse_csv.close(indata)
print(f"CSV output saved to: {output_csv_file}")
