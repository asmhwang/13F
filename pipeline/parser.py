"""
Parse SEC 13F-HR information table documents — both XML (post-2013) and
legacy fixed-width SGML text (pre-2013).

XML namespace:  https://www.sec.gov/Archives/edgar/data/.../form13fInfoTable.xml
Legacy format:  EDGAR SGML .txt submission with <TABLE> / fixed-width columns
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


# ---------------------------------------------------------------------------
# Legacy fixed-width text parser (pre-2013 SGML .txt submissions)
# ---------------------------------------------------------------------------
#
# Column layout (0-indexed, consistent across filers):
#   0 –16  Name of Issuer   (17 chars; may continue on indented lines)
#  17 –33  Title of Class   (17 chars)
#  34 –42  CUSIP            (9 chars, alphanumeric)
#  43 –53  Value ($ thousands, right-aligned, comma-formatted)
#  54 –65  Shares / Principal Amount (right-aligned, comma-formatted, or "-")
#  66+     Investment Discretion … Other Managers … Voting Authority
#

# Matches any line that contains a solid 9-char CUSIP followed by numeric data.
# Group 1 = text before CUSIP, Group 2 = CUSIP, Group 3 = value, Group 4 = shares/prn
# re.MULTILINE so ^ matches per-line when used with .search() on a block.
_DATA_LINE_RE = re.compile(
    r"^(.*?)\s+([A-Z0-9]{9})\s+([\d,]+)\s+([\d,]+|-)",
    re.MULTILINE,
)

# Format C (pre-2012): CUSIP printed with spaces, e.g. "025816 10 9"
# Group 1 = text before CUSIP, Group 2 = full spaced CUSIP, Group 3 = value, Group 4 = shares
_DATA_LINE_C_RE = re.compile(
    r"^(.*?)\s+([A-Z0-9]{6}\s[A-Z0-9]{2}\s[A-Z0-9])\s+([\d,]+)\s+([\d,]+|-)",
    re.MULTILINE,
)

# Continuation row in Format C: heavily indented line with only value + shares
_CONT_LINE_C_RE = re.compile(r"^\s{35,}([\d,]+)\s+([\d,]+|-)")
_VOTING_RE = re.compile(r"([\d,]+|-)\s+([\d,]+|-)\s+([\d,]+|-)\s*$")
_DISCRETION_RE = re.compile(r"(Sole|Shared(?:-Defined)?|Other)\b", re.IGNORECASE)


def _num(s: str) -> int | None:
    s = s.strip().replace(",", "")
    if not s or s == "-":
        return None
    try:
        return int(s)
    except ValueError:
        return None


def _data_lines(text: str) -> tuple[list[str], str]:
    """
    Extract holding data lines from the <TABLE> block in an SGML text filing.
    Starts collecting after the <S> marker row to skip <CAPTION> headers.

    Returns (lines, fmt) where fmt is 'A_or_B' or 'C'.
    """
    table_re = re.compile(r"<TABLE>(.*?)</TABLE>", re.DOTALL | re.IGNORECASE)
    for match in table_re.finditer(text):
        block = match.group(1)
        is_ab = bool(_DATA_LINE_RE.search(block))
        is_c  = bool(_DATA_LINE_C_RE.search(block))
        if not is_ab and not is_c:
            continue
        fmt = "A_or_B" if is_ab else "C"
        lines: list[str] = []
        in_data = False
        for line in block.splitlines():
            s = line.rstrip()
            if not s:
                continue
            if s.lstrip().upper().startswith("<S>"):
                in_data = True
                continue
            if s.lstrip().startswith("<"):
                continue
            if in_data:
                lines.append(s)
        return lines, fmt
    return [], "A_or_B"


def _parse_format_c(lines: list[str]) -> list[dict[str, Any]]:
    """
    Parse Format C (pre-2012) where CUSIPs are printed with spaces
    (e.g. '025816 10 9') and continuation rows carry additional value/shares
    for the same security.
    """
    holdings: list[dict[str, Any]] = []
    pending_name_parts: list[str] = []
    last_cusip: str | None = None
    last_name: str | None = None
    last_class: str | None = None

    for line in lines:
        # Continuation row: same security, different manager slice
        cont = _CONT_LINE_C_RE.match(line)
        if cont and last_cusip:
            val = _num(cont.group(1))
            if val:
                holdings.append({
                    "cusip":                 last_cusip,
                    "name_of_issuer":        last_name or "",
                    "title_of_class":        last_class,
                    "value_thousands":       val,
                    "shares":                _num(cont.group(2)),
                    "principal_amount":      None,
                    "share_type":            "SH",
                    "investment_discretion": None,
                    "put_call":              None,
                    "voting_sole":           None,
                    "voting_shared":         None,
                    "voting_none":           None,
                })
            continue

        m = _DATA_LINE_C_RE.match(line)
        if not m:
            bit = line.strip().rstrip(".")
            if bit:
                pending_name_parts.append(bit)
            continue

        pre_cusip  = m.group(1).strip().rstrip(".")
        spaced     = m.group(2)
        cusip      = spaced.replace(" ", "")
        value_raw  = m.group(3)
        shares_raw = m.group(4)

        pre_parts = pre_cusip.split()
        if len(pre_parts) >= 2:
            title_of_class = pre_parts[-1].rstrip(".")
            name_fragment  = " ".join(pre_parts[:-1]).rstrip(".")
        elif len(pre_parts) == 1:
            title_of_class = pre_parts[0].rstrip(".") if pending_name_parts else None
            name_fragment  = "" if pending_name_parts else pre_parts[0].rstrip(".")
        else:
            title_of_class = None
            name_fragment  = ""

        name_of_issuer = " ".join(
            p for p in pending_name_parts + ([name_fragment] if name_fragment else []) if p
        ).strip()
        pending_name_parts = []

        val = _num(value_raw)
        if val is None:
            continue

        last_cusip = cusip
        last_name  = name_of_issuer
        last_class = title_of_class

        holdings.append({
            "cusip":                 cusip,
            "name_of_issuer":        name_of_issuer,
            "title_of_class":        title_of_class,
            "value_thousands":       val,
            "shares":                _num(shares_raw),
            "principal_amount":      None,
            "share_type":            "SH",
            "investment_discretion": None,
            "put_call":              None,
            "voting_sole":           None,
            "voting_shared":         None,
            "voting_none":           None,
        })

    return holdings


def parse_legacy_text_table(text: str) -> list[dict[str, Any]]:
    """
    Parse a legacy SGML 13F .txt filing that contains a fixed-width
    <TABLE> information table.

    Handles two column layouts used by EDGAR across different years:
      • Format A (2013+): CUSIP at col 34; name continuation on lines AFTER data row.
      • Format B (2010–2012): CUSIP at col 23; name lines appear BEFORE data row.

    Returns the same list-of-dicts structure as parse_information_table().
    """
    lines, fmt = _data_lines(text)
    if not lines:
        return []

    if fmt == "C":
        return _parse_format_c(lines)

    # Detect layout by checking whether the first data line is indented.
    # Format A (2013+): first data line starts at col 0 — name continuation follows AFTER.
    # Format B (2010-2012): first data line is indented — name lines precede it.
    format_b = False
    for line in lines:
        if _DATA_LINE_RE.match(line):
            format_b = line.startswith(" ")
            break

    holdings: list[dict[str, Any]] = []
    pending_name_parts: list[str] = []

    for line in lines:
        m = _DATA_LINE_RE.match(line)

        if not m:
            bit = line.strip()
            if not bit:
                continue
            if format_b:
                # All non-data lines are pre-data name fragments
                pending_name_parts.append(bit)
            else:
                # Format A: indented non-data lines are post-data name continuations
                if line.startswith(" ") and holdings:
                    holdings[-1]["name_of_issuer"] = (
                        holdings[-1]["name_of_issuer"] + " " + bit
                    ).strip()
                else:
                    pending_name_parts.append(bit)
            continue

        pre_cusip  = m.group(1).strip()
        cusip      = m.group(2)
        value_raw  = m.group(3)
        shares_raw = m.group(4)
        tail       = line[m.end():]

        # Split pre_cusip into (name_fragment, title_of_class).
        # The class is the last token; everything before is name.
        pre_parts = pre_cusip.split()
        if len(pre_parts) >= 2:
            title_of_class = pre_parts[-1]
            name_fragment  = " ".join(pre_parts[:-1])
        elif len(pre_parts) == 1:
            title_of_class = pre_parts[0] if pending_name_parts else None
            name_fragment  = "" if pending_name_parts else pre_parts[0]
        else:
            title_of_class = None
            name_fragment  = ""

        name_of_issuer = " ".join(
            p for p in pending_name_parts + ([name_fragment] if name_fragment else []) if p
        ).strip()
        pending_name_parts = []

        value_thousands = _num(value_raw)
        if value_thousands is None:
            continue

        dis_match = _DISCRETION_RE.search(tail)
        investment_discretion = dis_match.group(1).capitalize() if dis_match else None

        voting_sole = voting_shared = voting_none = None
        vote_match = _VOTING_RE.search(tail)
        if vote_match:
            voting_sole   = _num(vote_match.group(1))
            voting_shared = _num(vote_match.group(2))
            voting_none   = _num(vote_match.group(3))

        holdings.append({
            "cusip":                 cusip,
            "name_of_issuer":        name_of_issuer,
            "title_of_class":        title_of_class,
            "value_thousands":       value_thousands,
            "shares":                _num(shares_raw),
            "principal_amount":      None,
            "share_type":            "SH",
            "investment_discretion": investment_discretion,
            "put_call":              None,
            "voting_sole":           voting_sole,
            "voting_shared":         voting_shared,
            "voting_none":           voting_none,
        })

    return holdings


def parse_auto(content: str) -> list[dict[str, Any]]:
    """
    Detect format and dispatch to the correct parser.
    Tries XML first; falls back to legacy text if XML parsing fails or
    yields no results.
    """
    content_stripped = content.lstrip()
    if content_stripped.startswith("<") and (
        "informationTable" in content[:500]
        or "infoTable" in content[:500]
        or "InformationTable" in content[:500]
    ):
        try:
            result = parse_information_table(content)
            if result:
                return result
        except ValueError:
            pass

    # Fall back to legacy SGML text format
    return parse_legacy_text_table(content)
