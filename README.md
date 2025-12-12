# textbooks-too-pricey

This repo processes bookstore export data (CSV) and enriches it with metadata from Primo and Alma APIs. For each row in the input file (identified by ISBN), the script performs a search and appends up to 5 enriched results with relevant details (title, format, license terms, permalink, etc.).

It was created to support textbook affordability by identifying which course books are already available to students in library-licensed eBook or print formats.

---

## Use Case

To promote textbook affordability, our library maintains a **Course Books in the Library** page. Using bookstore system data (exported as a CSV), this tool:

- Searches each ISBN against the **Primo API**
- Enriches results with **Alma portfolio metadata**
- Appends up to 5 results per ISBN to the output CSV
- Retains all original course-level fields and **price information** (new, used, rental, digital)
- Flags license terms (e.g., number of concurrent users)
- Includes permalink and location info

This allows us to assess which assigned course books are already licensed by the library and to deduplicate titles across sections.

---

## 🔧 Dependencies

- Python 3.8+
- `pandas`
- `requests`

Install via pip:

```bash
pip install pandas requests


