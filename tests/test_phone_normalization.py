"""Normalization must never turn an overlong number into a different contact."""
import pytest
from apps.api.services.leadgen.enrichment.normalize import normalize_fields, normalize_phone


@pytest.mark.parametrize("phone", ["+1234567890123456", "+12345678901234567", "+1 (234) 567-890123456"])
def test_overlong_international_number_is_preserved(phone):
    assert normalize_phone(phone) == phone
    assert normalize_fields({"phone": phone, "mobile_phone": phone}) == {"phone": phone, "mobile_phone": phone}


@pytest.mark.parametrize(("phone", "expected"), [
    ("+123456789012345", "+123456789012345"),
    ("+1 (415) 555-0132", "+14155550132"),
    ("0044 20 7946 0958", "+442079460958"),
    ("415.555.0132", "4155550132"),
])
def test_valid_phone_normalization_is_unchanged(phone, expected):
    assert normalize_phone(phone) == expected
