"""Shared ticker group and factor exposure contract.

Primary groups are unique and are used by governance flows such as basket
reviews. Factor tags can be many-to-one and are used for independent exposure
calculations.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass


@dataclass(frozen=True)
class GroupDefinition:
    name: str
    tickers: tuple[str, ...]
    limit_pct: float
    loss_review_threshold: float
    asset_type: str


PRIMARY_GROUP: dict[str, str] = {
    "QQQ": "tech_growth",
    "XLK": "tech_growth",
    "AIQ": "tech_growth",
    "BOTZ": "tech_growth",
    "CIBR": "tech_growth",
    "SOXX": "semiconductors",
    "PSI": "semiconductors",
    "FTXL": "semiconductors",
    "XSD": "semiconductors",
    "SMH": "semiconductors",
    "SOXL": "semiconductors",
    "SOXS": "semiconductors",
    "DRAM": "semiconductors",
    "BND": "defensive_bonds",
    "IEF": "defensive_bonds",
    "TLT": "defensive_bonds",
    "SGOV": "defensive_bonds",
    "BIL": "defensive_bonds",
    "XLU": "defensive_bonds",
    "XLP": "defensive_bonds",
    "XLE": "cyclicals",
    "XLI": "cyclicals",
    "IWM": "cyclicals",
    "XLRE": "real_estate",
    "SPY": "broad_market",
}


FACTOR_TAGS: dict[str, tuple[str, ...]] = {
    "QQQ": ("tech_growth", "broad_market"),
    "XLK": ("tech_growth",),
    "AIQ": ("tech_growth", "ai_thematic"),
    "BOTZ": ("tech_growth", "ai_thematic"),
    "CIBR": ("tech_growth", "cybersecurity"),
    "SOXX": ("tech_growth", "semiconductors"),
    "PSI": ("tech_growth", "semiconductors"),
    "FTXL": ("tech_growth", "semiconductors"),
    "XSD": ("tech_growth", "semiconductors"),
    "SMH": ("tech_growth", "semiconductors"),
    "SOXL": ("tech_growth", "semiconductors", "leveraged"),
    "SOXS": ("semiconductors", "inverse_leveraged"),
    "DRAM": ("tech_growth", "semiconductors"),
    "BND": ("defensive_bonds", "rates"),
    "IEF": ("defensive_bonds", "rates"),
    "TLT": ("defensive_bonds", "rates"),
    "SGOV": ("defensive_bonds", "cash_proxy"),
    "BIL": ("defensive_bonds", "cash_proxy"),
    "XLU": ("defensive_bonds", "utilities"),
    "XLP": ("defensive_bonds", "consumer_staples"),
    "XLE": ("cyclicals", "energy"),
    "XLI": ("cyclicals", "industrials"),
    "IWM": ("cyclicals", "small_cap"),
    "XLRE": ("real_estate",),
    "SPY": ("broad_market",),
}


GROUP_DEFINITIONS: dict[str, GroupDefinition] = {
    "tech_growth": GroupDefinition(
        name="tech_growth",
        tickers=("QQQ", "XLK", "AIQ", "BOTZ", "CIBR", "SOXX", "PSI", "FTXL", "XSD", "SMH", "SOXL", "DRAM"),
        limit_pct=0.35,
        loss_review_threshold=-0.04,
        asset_type="sector_thematic_mix",
    ),
    "semiconductors": GroupDefinition(
        name="semiconductors",
        tickers=("SOXX", "PSI", "FTXL", "XSD", "SMH", "SOXL", "SOXS", "DRAM"),
        limit_pct=0.25,
        loss_review_threshold=-0.04,
        asset_type="thematic",
    ),
    "defensive_bonds": GroupDefinition(
        name="defensive_bonds",
        tickers=("BND", "IEF", "TLT", "SGOV", "BIL", "XLU", "XLP"),
        limit_pct=0.35,
        loss_review_threshold=-0.05,
        asset_type="defensive",
    ),
    "cyclicals": GroupDefinition(
        name="cyclicals",
        tickers=("XLE", "XLI", "IWM"),
        limit_pct=0.30,
        loss_review_threshold=-0.05,
        asset_type="sector",
    ),
    "real_estate": GroupDefinition(
        name="real_estate",
        tickers=("XLRE",),
        limit_pct=0.15,
        loss_review_threshold=-0.04,
        asset_type="sector",
    ),
    "broad_market": GroupDefinition(
        name="broad_market",
        tickers=("SPY", "QQQ"),
        limit_pct=0.50,
        loss_review_threshold=-0.05,
        asset_type="core",
    ),
}


def get_primary_group(ticker: str) -> str | None:
    return PRIMARY_GROUP.get(str(ticker or "").upper().strip())


def get_factor_tags(ticker: str) -> tuple[str, ...]:
    return FACTOR_TAGS.get(str(ticker or "").upper().strip(), ())


def get_group_definition(group_name: str) -> GroupDefinition | None:
    return GROUP_DEFINITIONS.get(str(group_name or "").strip())


def get_default_group_limit(group_name: str) -> float | None:
    definition = get_group_definition(group_name)
    return definition.limit_pct if definition else None


def calc_primary_group_exposure(weights: dict[str, float]) -> dict[str, float]:
    exposure = {name: 0.0 for name in GROUP_DEFINITIONS}
    for ticker, weight in weights.items():
        group = get_primary_group(ticker)
        if group:
            exposure[group] = exposure.get(group, 0.0) + float(weight or 0.0)
    return exposure


def calc_factor_exposure(weights: dict[str, float]) -> dict[str, float]:
    exposures: dict[str, float] = defaultdict(float)
    for ticker, weight in weights.items():
        for tag in set(get_factor_tags(ticker)):
            exposures[tag] += float(weight or 0.0)
    return dict(exposures)
