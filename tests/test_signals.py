"""Unit tests for each fraud signal with synthetic data."""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.signals import (
    signal_excluded_provider,
    signal_billing_outlier,
    signal_rapid_escalation,
    signal_workforce_impossibility,
    signal_shared_official,
    signal_geographic_implausibility,
    run_all_signals,
)
from src.output import generate_report, STATUTE_MAP, NEXT_STEPS_MAP


class TestSignalExcludedProvider:
    """Signal 1: Excluded Provider Still Billing."""

    def test_detects_excluded_provider(self, con):
        results = signal_excluded_provider(con)
        assert len(results) >= 1
        npis = [r["npi"] for r in results]
        assert "2222222222" in npis

    def test_excluded_provider_severity_is_critical(self, con):
        results = signal_excluded_provider(con)
        for r in results:
            assert r["severity"] == "critical"

    def test_excluded_provider_has_overpayment(self, con):
        results = signal_excluded_provider(con)
        excluded = [r for r in results if r["npi"] == "2222222222"]
        assert len(excluded) == 1
        assert excluded[0]["estimated_overpayment_usd"] > 0

    def test_does_not_flag_non_excluded_provider(self, con):
        results = signal_excluded_provider(con)
        npis = [r["npi"] for r in results]
        assert "1111111111" not in npis


class TestSignalBillingOutlier:
    """Signal 2: Billing Volume Outlier."""

    def test_detects_outlier(self, con):
        results = signal_billing_outlier(con)
        assert len(results) >= 1
        npis = [r["npi"] for r in results]
        assert "3333333333" in npis

    def test_outlier_has_peer_stats(self, con):
        results = signal_billing_outlier(con)
        outlier = [r for r in results if r["npi"] == "3333333333"]
        assert len(outlier) == 1
        ev = outlier[0]["evidence"]
        assert "peer_median" in ev
        assert "peer_99th_percentile" in ev
        assert "ratio_to_median" in ev
        assert ev["ratio_to_median"] > 1.0

    def test_outlier_severity_high_when_above_5x(self, con):
        results = signal_billing_outlier(con)
        outlier = [r for r in results if r["npi"] == "3333333333"]
        if outlier and outlier[0]["evidence"]["ratio_to_median"] > 5:
            assert outlier[0]["severity"] == "high"

    def test_does_not_flag_normal_providers(self, con):
        results = signal_billing_outlier(con)
        npis = [r["npi"] for r in results]
        assert "3333333334" not in npis


class TestSignalRapidEscalation:
    """Signal 3: Rapid Billing Escalation."""

    def test_detects_rapid_escalation(self, con):
        results = signal_rapid_escalation(con)
        assert len(results) >= 1
        npis = [r["npi"] for r in results]
        assert "4444444444" in npis

    def test_escalation_has_growth_data(self, con):
        results = signal_rapid_escalation(con)
        rapid = [r for r in results if r["npi"] == "4444444444"]
        assert len(rapid) == 1
        ev = rapid[0]["evidence"]
        assert "peak_3_month_growth_rate" in ev
        assert ev["peak_3_month_growth_rate"] > 200
        assert "monthly_amounts_first_12" in ev

    def test_does_not_flag_established_provider(self, con):
        results = signal_rapid_escalation(con)
        npis = [r["npi"] for r in results]
        assert "1111111111" not in npis


class TestSignalWorkforceImpossibility:
    """Signal 4: Workforce Impossibility."""

    def test_detects_impossible_volume(self, con):
        results = signal_workforce_impossibility(con)
        assert len(results) >= 1
        npis = [r["npi"] for r in results]
        assert "5555555555" in npis

    def test_workforce_has_claims_per_hour(self, con):
        results = signal_workforce_impossibility(con)
        wf = [r for r in results if r["npi"] == "5555555555"]
        assert len(wf) == 1
        ev = wf[0]["evidence"]
        assert "implied_claims_per_hour" in ev
        assert ev["implied_claims_per_hour"] > 6.0
        assert "peak_month" in ev
        assert "peak_claims_count" in ev

    def test_overpayment_calculation(self, con):
        results = signal_workforce_impossibility(con)
        wf = [r for r in results if r["npi"] == "5555555555"]
        assert len(wf) == 1
        assert wf[0]["estimated_overpayment_usd"] > 0


class TestSignalSharedOfficial:
    """Signal 5: Shared Authorized Official."""

    def test_detects_shared_official(self, con):
        results = signal_shared_official(con)
        assert len(results) >= 1
        found = False
        for r in results:
            ev = r["evidence"]
            if "ROBERT SMITH" in ev["authorized_official_name"]:
                found = True
                assert ev["npi_count"] >= 5
                assert ev["combined_total_paid"] > 1_000_000
        assert found

    def test_shared_official_has_npi_list(self, con):
        results = signal_shared_official(con)
        for r in results:
            ev = r["evidence"]
            assert len(ev["controlled_npis"]) >= 5

    def test_shared_official_overpayment_is_zero(self, con):
        results = signal_shared_official(con)
        for r in results:
            assert r["estimated_overpayment_usd"] == 0.0


class TestSignalGeographicImplausibility:
    """Signal 6: Geographic Implausibility."""

    def test_detects_geographic_implausibility(self, con):
        results = signal_geographic_implausibility(con)
        assert len(results) >= 1
        npis = [r["npi"] for r in results]
        assert "7777777777" in npis

    def test_geo_has_ratio(self, con):
        results = signal_geographic_implausibility(con)
        geo = [r for r in results if r["npi"] == "7777777777"]
        assert len(geo) == 1
        ev = geo[0]["evidence"]
        assert "beneficiary_claims_ratio" in ev
        assert ev["beneficiary_claims_ratio"] < 0.1
        assert "flagged_hcpcs_codes" in ev

    def test_does_not_flag_normal_home_health(self, con):
        results = signal_geographic_implausibility(con)
        npis = [r["npi"] for r in results]
        assert "1111111111" not in npis


class TestRunAllSignals:
    """Test the run_all_signals orchestrator."""

    def test_returns_all_signal_types(self, con):
        results = run_all_signals(con)
        expected_types = [
            "excluded_provider", "billing_outlier", "rapid_escalation",
            "workforce_impossibility", "shared_official", "geographic_implausibility"
        ]
        for t in expected_types:
            assert t in results


class TestOutputGeneration:
    """Test the report generation."""

    def test_report_has_required_fields(self, con):
        signal_results = run_all_signals(con)
        report = generate_report(signal_results, con, 100)
        assert "generated_at" in report
        assert "tool_version" in report
        assert "total_providers_scanned" in report
        assert "total_providers_flagged" in report
        assert "signal_counts" in report
        assert "flagged_providers" in report

    def test_provider_has_required_fields(self, con):
        signal_results = run_all_signals(con)
        report = generate_report(signal_results, con, 100)
        for p in report["flagged_providers"]:
            assert "npi" in p
            assert "provider_name" in p
            assert "entity_type" in p
            assert "taxonomy_code" in p
            assert "state" in p
            assert "enumeration_date" in p
            assert "total_paid_all_time" in p
            assert "total_claims_all_time" in p
            assert "total_unique_beneficiaries_all_time" in p
            assert "signals" in p
            assert "estimated_overpayment_usd" in p
            assert "fca_relevance" in p

    def test_fca_relevance_has_required_fields(self, con):
        signal_results = run_all_signals(con)
        report = generate_report(signal_results, con, 100)
        for p in report["flagged_providers"]:
            fca = p["fca_relevance"]
            assert "claim_type" in fca
            assert "statute_reference" in fca
            assert "suggested_next_steps" in fca
            assert len(fca["suggested_next_steps"]) >= 2

    def test_statute_references_are_correct(self):
        assert STATUTE_MAP["excluded_provider"] == "31 U.S.C. section 3729(a)(1)(A)"
        assert STATUTE_MAP["billing_outlier"] == "31 U.S.C. section 3729(a)(1)(A)"
        assert STATUTE_MAP["rapid_escalation"] == "31 U.S.C. section 3729(a)(1)(A)"
        assert STATUTE_MAP["workforce_impossibility"] == "31 U.S.C. section 3729(a)(1)(B)"
        assert STATUTE_MAP["shared_official"] == "31 U.S.C. section 3729(a)(1)(C)"
        assert STATUTE_MAP["geographic_implausibility"] == "31 U.S.C. section 3729(a)(1)(G)"

    def test_next_steps_have_two_per_signal(self):
        for signal_type, steps in NEXT_STEPS_MAP.items():
            assert len(steps) >= 2, f"{signal_type} has fewer than 2 next steps"
