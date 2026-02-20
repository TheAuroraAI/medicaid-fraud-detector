"""JSON report generation module."""

import json
from datetime import datetime, timezone

VERSION = "1.0.0"

# Statute reference mapping per spec
STATUTE_MAP = {
    "excluded_provider": "31 U.S.C. section 3729(a)(1)(A)",
    "billing_outlier": "31 U.S.C. section 3729(a)(1)(A)",
    "rapid_escalation": "31 U.S.C. section 3729(a)(1)(A)",
    "workforce_impossibility": "31 U.S.C. section 3729(a)(1)(B)",
    "shared_official": "31 U.S.C. section 3729(a)(1)(C)",
    "geographic_implausibility": "31 U.S.C. section 3729(a)(1)(G)",
}

# Claim type descriptions
CLAIM_TYPE_MAP = {
    "excluded_provider": "Presenting false claims — excluded provider cannot legally bill federal healthcare programs",
    "billing_outlier": "Potential overbilling — provider billing significantly exceeds peer group norms",
    "rapid_escalation": "Potential bust-out scheme — newly enumerated provider with rapid billing escalation",
    "workforce_impossibility": "False records — billing volume implies physically impossible claim fabrication",
    "shared_official": "Conspiracy — coordinated billing through multiple entities controlled by same individual",
    "geographic_implausibility": "Reverse false claims — repeated billing on same patients suggests fabricated home health services",
}

# Next steps templates
NEXT_STEPS_MAP = {
    "excluded_provider": [
        "Verify provider exclusion status on OIG LEIE database and confirm dates",
        "Request itemized claims data from state Medicaid agency for post-exclusion period",
        "Determine which managed care organizations processed claims for this excluded provider",
    ],
    "billing_outlier": [
        "Request detailed claims data and compare procedure code distribution to peer group",
        "Verify provider is actively practicing at registered address through site visit or public records",
        "Cross-reference with patient records to verify services were actually rendered",
    ],
    "rapid_escalation": [
        "Investigate provider ownership changes around enumeration date",
        "Request detailed claims data for first 12 months of billing activity",
        "Check if provider entity was previously associated with excluded individuals",
    ],
    "workforce_impossibility": [
        "Request employment records showing number of licensed practitioners at this entity",
        "Compare staffing levels to claims volume to determine if services could have been physically rendered",
        "Review claims for patterns of identical procedures billed on same dates",
    ],
    "shared_official": [
        "Investigate corporate structure and beneficial ownership of all entities controlled by this individual",
        "Check for cross-referrals between the controlled entities suggesting kickback arrangements",
        "Review claims for overlapping patients across entities that would indicate coordinated billing",
    ],
    "geographic_implausibility": [
        "Verify patient addresses to confirm home health services were geographically feasible",
        "Request patient visit logs and compare to billed service dates",
        "Cross-reference with other payers to check for duplicate billing of same home health services",
    ],
}


def build_provider_record(npi: str, signals: list[dict], con) -> dict:
    """Build a complete provider record from their signals."""
    # Get provider info from NPPES
    try:
        provider_info = con.execute("""
            SELECT
                npi,
                COALESCE(org_name, first_name || ' ' || last_name) AS provider_name,
                CASE WHEN entity_type_code = '1' THEN 'individual' ELSE 'organization' END AS entity_type,
                taxonomy_code,
                state,
                enumeration_date
            FROM nppes
            WHERE npi = ?
            LIMIT 1
        """, [npi]).fetchone()
    except Exception:
        provider_info = None

    if provider_info:
        provider_name = provider_info[1] or "Unknown"
        entity_type = provider_info[2] or "unknown"
        taxonomy_code = provider_info[3] or "Unknown"
        state = provider_info[4] or "Unknown"
        enum_date = str(provider_info[5]) if provider_info[5] else "Unknown"
    else:
        provider_name = "Unknown"
        entity_type = "unknown"
        taxonomy_code = "Unknown"
        state = "Unknown"
        enum_date = "Unknown"

    # Override with signal-specific info if available
    for sig in signals:
        ev = sig.get("evidence", {})
        if "state" in ev and ev["state"]:
            state = ev["state"]
        if "taxonomy_code" in ev and ev["taxonomy_code"]:
            taxonomy_code = ev["taxonomy_code"]

    # Compute totals from spending
    try:
        totals = con.execute("""
            SELECT
                SUM(total_paid) AS total_paid,
                SUM(total_claims) AS total_claims,
                SUM(unique_beneficiaries) AS total_beneficiaries
            FROM spending
            WHERE billing_npi = ?
        """, [npi]).fetchone()
        total_paid = float(totals[0] or 0)
        total_claims = int(totals[1] or 0)
        total_beneficiaries = int(totals[2] or 0)
    except Exception:
        total_paid = 0.0
        total_claims = 0
        total_beneficiaries = 0

    # Build signal list
    signal_records = []
    total_overpayment = 0.0
    for sig in signals:
        sig_type = sig["signal_type"]
        overpayment = sig.get("estimated_overpayment_usd", 0.0)
        total_overpayment += overpayment
        signal_records.append({
            "signal_type": sig_type,
            "severity": sig["severity"],
            "evidence": sig["evidence"],
        })

    return {
        "npi": npi,
        "provider_name": provider_name,
        "entity_type": entity_type,
        "taxonomy_code": taxonomy_code,
        "state": state,
        "enumeration_date": enum_date,
        "total_paid_all_time": round(total_paid, 2),
        "total_claims_all_time": total_claims,
        "total_unique_beneficiaries_all_time": total_beneficiaries,
        "signals": signal_records,
        "estimated_overpayment_usd": round(total_overpayment, 2),
        "fca_relevance": {
            "claim_type": CLAIM_TYPE_MAP.get(signals[0]["signal_type"], "Unknown violation pattern"),
            "statute_reference": STATUTE_MAP.get(signals[0]["signal_type"], "31 U.S.C. section 3729"),
            "suggested_next_steps": NEXT_STEPS_MAP.get(signals[0]["signal_type"], [
                "Request detailed claims data from state Medicaid agency",
                "Verify provider information through public records",
            ]),
        },
    }


def generate_report(signal_results: dict, con, total_providers_scanned: int) -> dict:
    """Generate the final fraud_signals.json report."""
    # Group signals by NPI
    npi_signals: dict[str, list[dict]] = {}
    signal_counts = {}

    for signal_type, signals in signal_results.items():
        signal_counts[signal_type] = len(signals)
        for sig in signals:
            npi = sig["npi"]
            if npi not in npi_signals:
                npi_signals[npi] = []
            npi_signals[npi].append(sig)

    # Build provider records
    flagged_providers = []
    for npi, signals in npi_signals.items():
        provider = build_provider_record(npi, signals, con)
        flagged_providers.append(provider)

    # Sort by estimated overpayment descending
    flagged_providers.sort(key=lambda p: p["estimated_overpayment_usd"], reverse=True)

    report = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "tool_version": VERSION,
        "total_providers_scanned": total_providers_scanned,
        "total_providers_flagged": len(flagged_providers),
        "signal_counts": signal_counts,
        "flagged_providers": flagged_providers,
    }

    return report


def write_report(report: dict, output_path: str) -> None:
    """Write the report to a JSON file."""
    with open(output_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"\nReport written to: {output_path}")
    print(f"  Providers scanned: {report['total_providers_scanned']:,}")
    print(f"  Providers flagged: {report['total_providers_flagged']:,}")
    for sig_type, count in report['signal_counts'].items():
        print(f"  {sig_type}: {count}")
