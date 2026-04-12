import asyncio
import httpx
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs

BASE_SEARCH = "https://www.pa.marion.fl.us/PropertySearch.aspx"
BASE_PRC    = "https://www.pa.marion.fl.us/PRC.aspx"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;"
              "q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://www.pa.marion.fl.us/",
}


async def lookup_parcel(
    client: httpx.AsyncClient,
    parcel_number: str,
    lookup_year: int
) -> dict:
    """
    Two-step lookup against Marion County Property
    Appraiser to get the FORMER owner — the person
    who owned the property before the tax sale.

    lookup_year should be (sale_date.year - 1) so we
    query the ownership record from the year BEFORE
    the auction. Querying the sale year itself may
    already show the new auction buyer depending on
    when the property appraiser updates their records.

    Example:
      Property sold at auction in March 2025
      → lookup_year = 2024
      → Returns the owner as of 2024 (former owner)

      Property sold at auction in 2023
      → lookup_year = 2022
      → Returns the owner as of 2022 (former owner)

    Returns dict:
      {
        owner_name: str,       # empty string if not found
        mailing_address: str,  # empty string if not found
        found: bool
      }

    Never raises. All exceptions are caught and logged.
    On any failure returns:
      { owner_name: "", mailing_address: "", found: False }
    """

    # STEP 1 — Search by parcel number to get internal Key
    try:
        search_url = (
            f"{BASE_SEARCH}"
            f"?SearchBy=ParcelR&Parms={parcel_number}"
        )
        resp = await client.get(
            search_url,
            headers=HEADERS,
            timeout=15.0,
            follow_redirects=True
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Find anchor tag linking to PRC.aspx with key param
        # Example href from live site:
        # /PRC.aspx?key=1658861&YR=2026&mName=False&mSitus=False
        link = soup.find(
            "a",
            href=lambda h: h and "PRC.aspx" in h
                          and "key=" in h.lower()
        )
        if not link:
            print(f"[marion_pa] No PRC link found for "
                  f"parcel {parcel_number}")
            return {"owner_name": "", "mailing_address": "",
                    "found": False}

        href = link["href"]
        parsed = urlparse(href)
        params = {k.lower(): v for k, v in parse_qs(parsed.query).items()}
        key = (params.get("key") or [None])[0]

        if not key:
            print(f"[marion_pa] Could not extract key from "
                  f"href: {href}")
            return {"owner_name": "", "mailing_address": "",
                    "found": False}

    except Exception as e:
        print(f"[marion_pa] Search step failed for "
              f"parcel {parcel_number}: {e}")
        return {"owner_name": "", "mailing_address": "",
                "found": False}

    # STEP 2 — Fetch ownership record at lookup_year
    # lookup_year = sale_date.year - 1 (the year BEFORE
    # the tax sale) to ensure we get the former owner,
    # not the auction buyer
    try:
        prc_url = (
            f"{BASE_PRC}"
            f"?key={key}&YR={lookup_year}"
            f"&mName=False&mSitus=False"
        )
        resp = await client.get(
            prc_url,
            headers=HEADERS,
            timeout=15.0,
            follow_redirects=True
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Log page preview for debugging selector accuracy
        page_text = soup.get_text(separator=" ", strip=True)
        print(f"[marion_pa] PRC preview "
              f"(key={key}, yr={lookup_year}): "
              f"{page_text[:300]}")

        owner_name, mailing_address = _extract_prc_owner_and_address(soup)

        found = bool(owner_name or mailing_address)
        return {
            "owner_name": owner_name,
            "mailing_address": mailing_address,
            "found": found
        }

    except Exception as e:
        print(f"[marion_pa] PRC fetch failed "
              f"(key={key}, yr={lookup_year}): {e}")
        return {"owner_name": "", "mailing_address": "",
                "found": False}


def _extract_prc_owner_and_address(soup) -> tuple[str, str]:
    """
    Extracts owner name and mailing address from the Marion
    County PRC page.

    The data lives in the first <td> of the <table> immediately
    following the <center>Property Information</center> element.
    That cell contains br-separated lines in this order:

      OWNER NAME LINE 1
      OWNER NAME LINE 2  (optional, for co-owners)
      STREET ADDRESS
      CITY STATE ZIP

    Owner lines are identified as those that come before the
    first line starting with a digit (the street number).
    Everything from that digit-starting line onward is the
    mailing address.

    Multiple owner names are joined with " / ".
    Address lines are joined with ", ".

    Returns ("", "") if the block cannot be found.
    """
    # Locate the specific "Property Information" center element
    # that links to INFOHELP.html (not the nav-menu occurrences)
    pi_center = None
    for tag in soup.find_all("center"):
        link = tag.find(
            "a", href=lambda h: h and "INFOHELP" in h
        )
        if link:
            pi_center = tag
            break

    if not pi_center:
        return "", ""

    next_table = pi_center.find_next_sibling("table")
    if not next_table:
        return "", ""

    first_td = next_table.find("td")
    if not first_td:
        return "", ""

    # Collect non-empty text lines split by <br> tags
    lines = []
    for node in first_td.children:
        if hasattr(node, "name") and node.name == "br":
            continue
        text = (
            node.get_text(strip=True)
            if hasattr(node, "get_text")
            else str(node).strip()
        )
        if text:
            lines.append(text)

    if not lines:
        return "", ""

    # Split at the first line that starts with a digit
    # (street number) — everything before = owner names,
    # everything from there = mailing address
    owner_lines: list[str] = []
    address_lines: list[str] = []
    in_address = False

    for line in lines:
        if not in_address and line and line[0].isdigit():
            in_address = True
        if in_address:
            address_lines.append(line)
        else:
            owner_lines.append(line)

    owner_name = " / ".join(owner_lines)
    mailing_address = ", ".join(address_lines)
    return owner_name, mailing_address


def _extract_field(soup, labels: list[str]) -> str:
    """
    Finds text near a label in a BeautifulSoup page.
    Tries multiple strategies to handle different
    HTML structures.

    Strategy 1: Find td/th/span/label containing the
    label text, return next sibling text.

    Strategy 2: Find label in td, return the next td
    in the same row (tr).

    Returns empty string if nothing found.
    """
    for label in labels:
        for tag in soup.find_all(
            ["td", "th", "span", "label", "div"]
        ):
            tag_text = tag.get_text(strip=True).lower()
            if label.lower() in tag_text:

                # Strategy 1: next sibling element
                sibling = tag.find_next_sibling()
                if sibling:
                    text = sibling.get_text(strip=True)
                    if text and len(text) > 2:
                        return text

                # Strategy 2: next td in same row
                parent_row = tag.find_parent("tr")
                if parent_row:
                    cells = parent_row.find_all("td")
                    for i, cell in enumerate(cells):
                        if label.lower() in cell.get_text(
                            strip=True
                        ).lower():
                            if i + 1 < len(cells):
                                text = cells[i + 1].get_text(strip=True)
                                if text and len(text) > 2:
                                    return text

                # Strategy 3: next row in same table
                parent_row = tag.find_parent("tr")
                if parent_row:
                    next_row = parent_row.find_next_sibling("tr")
                    if next_row:
                        text = next_row.get_text(strip=True)
                        if text and len(text) > 2:
                            return text

    return ""


async def enrich_with_pa_lookup(
    records: list
) -> list:
    """
    Enriches a list of SurplusRecord objects from
    Marion County by looking up each parcel number
    against the Marion County property appraiser.

    For each record:
    - lookup_year = record.sale_date.year - 1
      (the year BEFORE the tax sale to guarantee
      we retrieve the former owner, not the buyer)
    - Fills record.owner_name if currently empty
    - Fills record.property_address with the
      MAILING address from the appraiser record
      (not the situs/property address — the mailing
      address is where the former owner actually
      lived and is what Tracerfy needs for skip
      tracing)

    Sleeps 1 second between requests to avoid
    overloading the government site.

    Parcel number is read from record.case_number —
    Marion County stores the parcel number there.
    Verify this mapping is correct against the
    actual scraped data.

    Returns the enriched list. Records that could
    not be looked up are returned unchanged.
    """
    enriched = []
    found_count = 0

    async with httpx.AsyncClient(timeout=20.0) as client:
        for i, record in enumerate(records):

            parcel = record.case_number
            if not parcel:
                print(f"[marion_pa] [{i+1}/{len(records)}] "
                      f"No parcel number — skipping")
                enriched.append(record)
                continue

            # Use year BEFORE sale to get former owner
            lookup_year = record.sale_date.year - 1

            result = await lookup_parcel(
                client, parcel, lookup_year
            )

            if result["found"]:
                if not record.owner_name and result["owner_name"]:
                    record.owner_name = (
                        result["owner_name"].strip().title()
                    )
                if result["mailing_address"]:
                    record.property_address = (
                        result["mailing_address"].strip()
                    )
                found_count += 1
                print(
                    f"[marion_pa] [{i+1}/{len(records)}] "
                    f"✓ {record.owner_name} | "
                    f"{record.property_address}"
                )
            else:
                print(
                    f"[marion_pa] [{i+1}/{len(records)}] "
                    f"✗ Not found — parcel: {parcel} "
                    f"lookup_year: {lookup_year}"
                )

            enriched.append(record)
            await asyncio.sleep(1.0)

    print(
        f"[marion_pa] Complete. "
        f"{found_count}/{len(enriched)} records enriched."
    )
    return enriched
