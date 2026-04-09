"""Tests for search_filers_by_name()."""
import pytest
from unittest.mock import patch, MagicMock


def _make_table_response(rows: list[tuple[str, str]]) -> MagicMock:
    """Build a mock response with EDGAR multi-result table HTML."""
    rows_html = "".join(
        f'<td><a href="?action=getcompany&CIK={cik}&type=13F-HR">{cik}</a></td>'
        f'<td scope="row">{name}</td>'
        for cik, name in rows
    )
    html = f"<table>{rows_html}</table>"
    mock = MagicMock()
    mock.text = html
    return mock


def _make_profile_response(cik: str, name: str) -> MagicMock:
    """Build a mock response with EDGAR single-company profile page HTML."""
    html = (
        f'<span class="companyName">{name} '
        f'<acronym title="Central Index Key">CIK</acronym># '
        f'<a href="?action=getcompany&CIK={cik}&type=13F-HR">{cik} (see all...)</a>'
        f'</span>'
    )
    mock = MagicMock()
    mock.text = html
    return mock


def test_search_returns_list_of_dicts():
    """search_filers_by_name returns [{cik, name}, ...] on a multi-result response."""
    from pipeline.edgar import search_filers_by_name

    mock_response = _make_table_response([
        ("0001336528", "Pershing Square Capital Management, L.P."),
        ("0002026053", "Pershing Square Holdings"),
    ])

    with patch("pipeline.edgar._http_get", return_value=mock_response):
        results = search_filers_by_name("pershing")

    assert isinstance(results, list)
    assert len(results) == 2
    assert results[0]["name"] == "Pershing Square Capital Management, L.P."
    assert results[0]["cik"] == "1336528"   # leading zeros stripped


def test_search_single_match_profile_page():
    """Single-match company profile page (Case B) is parsed correctly."""
    from pipeline.edgar import search_filers_by_name

    mock_response = _make_profile_response("0001336528", "Pershing Square Capital Management, L.P.")

    with patch("pipeline.edgar._http_get", return_value=mock_response):
        results = search_filers_by_name("pershing")

    assert len(results) == 1
    assert results[0]["name"] == "Pershing Square Capital Management, L.P."
    assert results[0]["cik"] == "1336528"


def test_search_empty_query_returns_empty():
    """Queries shorter than 3 chars return [] without hitting the API."""
    from pipeline.edgar import search_filers_by_name

    with patch("pipeline.edgar._http_get") as mock_get:
        result = search_filers_by_name("ab")
    mock_get.assert_not_called()
    assert result == []


def test_search_network_error_returns_empty():
    """Network errors are swallowed and return []."""
    from pipeline.edgar import search_filers_by_name
    import requests

    with patch("pipeline.edgar._http_get", side_effect=requests.RequestException("timeout")):
        result = search_filers_by_name("berkshire")

    assert result == []


def test_search_respects_max_results():
    """max_results caps the returned list."""
    from pipeline.edgar import search_filers_by_name

    rows = [(f"000{i:07d}", f"Fund {i}") for i in range(1, 11)]
    mock_response = _make_table_response(rows)

    with patch("pipeline.edgar._http_get", return_value=mock_response):
        results = search_filers_by_name("fund", max_results=3)

    assert len(results) == 3


def test_search_strips_amp_entity():
    """HTML &amp; in company names is decoded to &."""
    from pipeline.edgar import search_filers_by_name

    mock_response = _make_table_response([("0000038777", "Franklin Resources &amp; Co")])

    with patch("pipeline.edgar._http_get", return_value=mock_response):
        results = search_filers_by_name("franklin")

    assert results[0]["name"] == "Franklin Resources & Co"


def test_search_no_results_returns_empty():
    """An HTML response with no matching table rows or profile page returns []."""
    from pipeline.edgar import search_filers_by_name

    mock_response = MagicMock()
    mock_response.text = "<html><body>No results found.</body></html>"

    with patch("pipeline.edgar._http_get", return_value=mock_response):
        results = search_filers_by_name("xyznonexistent")

    assert results == []
