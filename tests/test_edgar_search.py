"""Tests for search_filers_by_name()."""
import pytest
from unittest.mock import patch, MagicMock


def test_search_returns_list_of_dicts():
    """search_filers_by_name returns [{cik, name}, ...] on a successful response."""
    from pipeline.edgar import search_filers_by_name

    mock_response = MagicMock()
    mock_response.json.return_value = {
        "hits": {
            "hits": [
                {"_source": {"entity_name": "Pershing Square Capital Management", "file_num": "028-12345"}},
                {"_source": {"entity_name": "Pershing Square Holdings", "file_num": "028-67890"}},
            ]
        }
    }
    mock_response.raise_for_status = MagicMock()

    with patch("pipeline.edgar._http_get", return_value=mock_response):
        results = search_filers_by_name("pershing")

    assert isinstance(results, list)
    assert len(results) == 2
    assert results[0]["name"] == "Pershing Square Capital Management"
    assert results[0]["cik"] == "12345"  # file_num "028-12345" -> strip "028-" -> lstrip "0"


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

    hits = [
        {"_source": {"entity_name": f"Fund {i}", "file_num": f"028-{i:05d}"}}
        for i in range(1, 11)
    ]
    mock_response = MagicMock()
    mock_response.json.return_value = {"hits": {"hits": hits}}
    mock_response.raise_for_status = MagicMock()

    with patch("pipeline.edgar._http_get", return_value=mock_response):
        results = search_filers_by_name("fund", max_results=3)

    assert len(results) == 3


def test_search_skips_entries_with_missing_cik():
    """Entries with empty or missing file_num are skipped."""
    from pipeline.edgar import search_filers_by_name

    mock_response = MagicMock()
    mock_response.json.return_value = {
        "hits": {
            "hits": [
                {"_source": {"entity_name": "Valid Fund", "file_num": "028-00099"}},
                {"_source": {"entity_name": "No CIK Fund", "file_num": ""}},
                {"_source": {"entity_name": "Missing file_num Fund"}},
            ]
        }
    }
    mock_response.raise_for_status = MagicMock()

    with patch("pipeline.edgar._http_get", return_value=mock_response):
        results = search_filers_by_name("fund")

    assert len(results) == 1
    assert results[0]["name"] == "Valid Fund"
    assert results[0]["cik"] == "99"
