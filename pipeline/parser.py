"""
Parse SEC 13F-HR XML information table documents.

SEC 13F XML namespace:
  https://www.sec.gov/Archives/edgar/data/.../form13fInfoTable.xml
"""

import re
import xml.etree.ElementTree as ET
from typing import Any

# The namespace used in 13F information table XML (may vary slightly across years)
_NS_PATTERNS = [
    "com/ns/edgar/document/thirteenf/informationtable",
    "thirteenf/informationtable",
]

_TEXT_RE = re.compile(r"\s+")


def _clean(text: str | None) -> str | None:
    if text is None:
        return None
    return _TEXT_RE.sub(" ", text.strip()) or None


def _int(text: str | None) -> int | None:
    if not text:
        return None
    try:
        return int(text.replace(",", "").strip())
    except ValueError:
        return None


def _find(el: ET.Element, tag: str, ns: str) -> ET.Element | None:
    """Case-insensitive local-name search (SEC occasionally varies casing)."""
    tag_lower = tag.lower()
    for child in el:
        local = child.tag.split("}")[-1].lower() if "}" in child.tag else child.tag.lower()
        if local == tag_lower:
            return child
    return None


def _text(el: ET.Element, tag: str, ns: str = "") -> str | None:
    child = _find(el, tag, ns)
    return _clean(child.text) if child is not None else None


def _detect_namespace(root: ET.Element) -> str:
    """Extract namespace URI from the root element tag."""
    if root.tag.startswith("{"):
        return root.tag[1:root.tag.index("}")]
    return ""


def parse_information_table(xml_text: str) -> list[dict[str, Any]]:
    """
    Parse a 13F information table XML string.

    Returns a list of holding dicts with keys matching the holdings table columns
    (minus filing_id, which is assigned by the caller).
    """
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        raise ValueError(f"Invalid XML: {exc}") from exc

    ns = _detect_namespace(root)

    holdings: list[dict[str, Any]] = []

    # Root is <informationTable>; children are <infoTable> entries
    for entry in root:
        local = entry.tag.split("}")[-1].lower() if "}" in entry.tag else entry.tag.lower()
        if local != "infotable":
            continue

        cusip         = _text(entry, "cusip", ns)
        name_of_issuer = _text(entry, "nameOfIssuer", ns)
        title_of_class = _text(entry, "titleOfClass", ns)
        value_str      = _text(entry, "value", ns)
        investment_dis = _text(entry, "investmentDiscretion", ns)
        put_call       = _text(entry, "putCall", ns)

        # <shrsOrPrnAmt> sub-element
        amt_el = _find(entry, "shrsOrPrnAmt", ns)
        shares = None
        principal_amount = None
        share_type = None
        if amt_el is not None:
            share_type = _text(amt_el, "sshPrnamtType", ns)
            raw_amt = _text(amt_el, "sshPrnamt", ns)
            if share_type == "PRN":
                try:
                    principal_amount = float(raw_amt.replace(",", "")) if raw_amt else None
                except ValueError:
                    pass
            else:
                shares = _int(raw_amt)

        # <votingAuthority> sub-element
        vote_el = _find(entry, "votingAuthority", ns)
        voting_sole = voting_shared = voting_none = None
        if vote_el is not None:
            voting_sole   = _int(_text(vote_el, "Sole", ns))
            voting_shared = _int(_text(vote_el, "Shared", ns))
            voting_none   = _int(_text(vote_el, "None", ns))

        # value is reported in thousands
        value_thousands = _int(value_str)

        if not cusip or value_thousands is None:
            continue  # skip malformed rows

        holdings.append({
            "cusip":                 cusip,
            "name_of_issuer":        name_of_issuer or "",
            "title_of_class":        title_of_class,
            "value_thousands":       value_thousands,
            "shares":                shares,
            "principal_amount":      principal_amount,
            "share_type":            share_type,
            "investment_discretion": investment_dis,
            "put_call":              put_call,
            "voting_sole":           voting_sole,
            "voting_shared":         voting_shared,
            "voting_none":           voting_none,
        })

    return holdings
