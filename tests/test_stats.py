from crypto_sentinel.stats import robust_zscore


def test_robust_zscore_resists_single_outlier() -> None:
    baseline = [0.9, 1.0, 1.1, 0.95, 1.05, 1.02, 200.0]
    score = robust_zscore(1.3, baseline)
    assert score is not None
    assert score > 2


def test_robust_zscore_requires_enough_data() -> None:
    assert robust_zscore(1.0, [1.0, 1.1, 0.9]) is None
