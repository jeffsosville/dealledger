import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scrapers"))
from crawl_run_log import is_truncated, _norm_domain


def test_reputed_case_is_truncated():
    assert is_truncated(2, [333, 330, 328]) == (True, 333)


def test_normal_variation_is_ok():
    assert is_truncated(300, [333])[0] is False
    assert is_truncated(67, [333])[0] is False   # 20% exactly is not below


def test_small_brokers_never_flagged():
    assert is_truncated(1, [12, 15])[0] is False


def test_no_history_is_ok():
    assert is_truncated(5, []) == (False, 0)


def test_only_last_three_count():
    assert is_truncated(50, [60, 55, 58, 1000])[0] is False


def test_norm_domain():
    assert _norm_domain("WWW.ReputedBrokerage.com") == "reputedbrokerage.com"
