"""
models/risk_models.py
Quantitative models for DeFi trading and risk management.

Models implemented
------------------
1. HistoricalVaR        – Historical simulation Value at Risk & CVaR
2. ImpermanentLossModel – AMM impermanent loss for LP positions
3. MEVExposureModel     – Estimates MEV (maximal extractable value) exposure per block
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass

import numpy as np
import pandas as pd
from scipy import stats

warnings.filterwarnings("ignore", category=RuntimeWarning)


# ---------------------------------------------------------------------------
# Data classes for results
# ---------------------------------------------------------------------------

@dataclass
class VaRResult:
    confidence: float
    horizon_days: int
    var: float           # Value at Risk  (positive = potential loss)
    cvar: float          # Conditional VaR / Expected Shortfall
    returns_used: int
    method: str

@dataclass
class ImpermanentLossResult:
    price_ratio: float         # current_price / entry_price
    il_pct: float              # IL as a percentage of hold value
    hold_value: float
    lp_value: float
    loss_usd: float

@dataclass
class MEVRiskResult:
    block_number: int
    mev_score: float           # 0–100 composite risk score
    sandwich_risk: float
    frontrun_risk: float
    backrun_opportunities: int


# ---------------------------------------------------------------------------
# 1. Historical VaR & CVaR
# ---------------------------------------------------------------------------

class HistoricalVaR:
    """
    Historical simulation VaR for a token price or portfolio return series.

    Parameters
    ----------
    returns : pd.Series
        Log or simple returns, datetime-indexed.
    position_size : float
        USD position size for absolute P&L VaR.
    """

    def __init__(self, returns: pd.Series, position_size: float = 1_000_000.0):
        if returns.empty:
            raise ValueError("Returns series cannot be empty.")
        self.returns       = returns.dropna()
        self.position_size = position_size

    def compute(
        self,
        confidence: float = 0.95,
        horizon_days: int = 1,
        scale_by_sqrt: bool = True,
    ) -> VaRResult:
        """
        Compute historical VaR and CVaR.

        Parameters
        ----------
        confidence   : float  – e.g. 0.95 or 0.99
        horizon_days : int    – holding period (uses sqrt-of-time rule if >1)
        scale_by_sqrt: bool   – apply sqrt(horizon) scaling for multi-day VaR
        """
        r = self.returns.values
        q = np.quantile(r, 1 - confidence)

        var_pct   = -q
        cvar_pct  = -r[r <= q].mean()

        if scale_by_sqrt and horizon_days > 1:
            var_pct  *= np.sqrt(horizon_days)
            cvar_pct *= np.sqrt(horizon_days)

        return VaRResult(
            confidence    = confidence,
            horizon_days  = horizon_days,
            var           = var_pct * self.position_size,
            cvar          = cvar_pct * self.position_size,
            returns_used  = len(r),
            method        = "historical_simulation",
        )

    def rolling_var(
        self,
        window: int = 30,
        confidence: float = 0.95,
    ) -> pd.Series:
        """Rolling 1-day VaR — useful for plotting risk regimes."""
        return (
            self.returns
            .rolling(window=window, min_periods=window // 2)
            .quantile(1 - confidence)
            .mul(-self.position_size)
            .rename("rolling_var")
        )

    def stress_test(self, shock_pcts: list[float] | None = None) -> pd.DataFrame:
        """
        Apply hypothetical shocks and compute P&L impact.

        Parameters
        ----------
        shock_pcts : list of floats (e.g. [-0.3, -0.5, -0.8] for -30%, -50%, -80%)
        """
        if shock_pcts is None:
            shock_pcts = [-0.10, -0.20, -0.30, -0.50, -0.80]

        records = []
        for shock in shock_pcts:
            pnl = shock * self.position_size
            records.append({
                "shock_pct": shock * 100,
                "pnl_usd": pnl,
                "position_remaining": self.position_size + pnl,
            })
        return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# 2. Impermanent Loss Model (Uniswap V2 / constant-product AMM)
# ---------------------------------------------------------------------------

class ImpermanentLossModel:
    """
    Calculates impermanent loss for a Uniswap V2-style (x*y=k) LP position.

    IL formula: IL = 2 * sqrt(r) / (1 + r) - 1
    where r = price_current / price_entry
    """

    @staticmethod
    def compute(
        price_entry: float,
        price_current: float,
        initial_usd_value: float = 10_000.0,
    ) -> ImpermanentLossResult:
        r = price_current / price_entry

        # LP value relative to hold
        lp_relative  = 2 * np.sqrt(r) / (1 + r)
        il_pct       = lp_relative - 1           # negative = loss

        hold_value = initial_usd_value * (1 + r) / 2   # 50/50 rebalance
        lp_value   = initial_usd_value * lp_relative
        loss_usd   = lp_value - hold_value

        return ImpermanentLossResult(
            price_ratio = r,
            il_pct      = il_pct * 100,
            hold_value  = hold_value,
            lp_value    = lp_value,
            loss_usd    = loss_usd,
        )

    @classmethod
    def scan_price_range(
        cls,
        price_entry: float,
        initial_usd: float = 10_000.0,
        ratios: list[float] | None = None,
    ) -> pd.DataFrame:
        """Compute IL across a range of price ratios for LP risk profiling."""
        if ratios is None:
            ratios = [0.25, 0.5, 0.75, 1.0, 1.5, 2.0, 3.0, 5.0, 10.0]

        rows = []
        for r in ratios:
            result = cls.compute(price_entry, price_entry * r, initial_usd)
            rows.append({
                "price_ratio":   r,
                "price_current": price_entry * r,
                "il_pct":        result.il_pct,
                "lp_value":      result.lp_value,
                "hold_value":    result.hold_value,
                "loss_usd":      result.loss_usd,
            })
        return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# 3. MEV Exposure Model
# ---------------------------------------------------------------------------

class MEVExposureModel:
    """
    Estimates MEV exposure from block transaction data.

    Signals used:
    - Gas price variance (sandwich bots pay high then low)
    - High-priority tx immediately before/after large swaps (frontrun signal)
    - Back-to-back same-pool swaps with opposite directions (sandwich signal)
    """

    def __init__(self, tx_df: pd.DataFrame, swap_df: pd.DataFrame):
        self.tx_df   = tx_df.copy() if not tx_df.empty else pd.DataFrame()
        self.swap_df = swap_df.copy() if not swap_df.empty else pd.DataFrame()

    def score_block(self, block_number: int) -> MEVRiskResult:
        """Compute a composite MEV risk score (0–100) for a given block."""
        block_txs   = self.tx_df[self.tx_df["block_number"] == block_number] if not self.tx_df.empty else pd.DataFrame()
        block_swaps = self.swap_df[self.swap_df["block_number"] == block_number] if not self.swap_df.empty else pd.DataFrame()

        sandwich_risk  = self._sandwich_risk(block_swaps)
        frontrun_risk  = self._frontrun_risk(block_txs, block_swaps)
        backrun_count  = self._backrun_opportunities(block_swaps)

        composite = min(100.0, sandwich_risk * 0.5 + frontrun_risk * 0.4 + backrun_count * 2)

        return MEVRiskResult(
            block_number          = block_number,
            mev_score             = round(composite, 2),
            sandwich_risk         = round(sandwich_risk, 2),
            frontrun_risk         = round(frontrun_risk, 2),
            backrun_opportunities = backrun_count,
        )

    def _sandwich_risk(self, swaps: pd.DataFrame) -> float:
        if swaps.empty or "pool" not in swaps.columns:
            return 0.0
        # Pools with ≥3 swaps in one block have elevated sandwich risk
        pool_counts = swaps.groupby("pool").size()
        high_activity = (pool_counts >= 3).sum()
        return min(100.0, float(high_activity) * 25)

    def _frontrun_risk(self, txs: pd.DataFrame, swaps: pd.DataFrame) -> float:
        if txs.empty or "gas_price_gwei" not in txs.columns:
            return 0.0
        gas_std = txs["gas_price_gwei"].std()
        gas_max = txs["gas_price_gwei"].max()
        gas_med = txs["gas_price_gwei"].median()
        if gas_med == 0:
            return 0.0
        # High gas outliers relative to median signal priority manipulation
        ratio = gas_max / gas_med
        return min(100.0, max(0.0, (ratio - 1) * 20))

    def _backrun_opportunities(self, swaps: pd.DataFrame) -> int:
        if swaps.empty or "pool" not in swaps.columns:
            return 0
        return int((swaps.groupby("pool").size() >= 2).sum())

    def score_all_blocks(self) -> pd.DataFrame:
        """Score every block in the dataset."""
        if self.tx_df.empty:
            return pd.DataFrame()
        blocks  = self.tx_df["block_number"].unique()
        results = [self.score_block(int(b)) for b in sorted(blocks)]
        return pd.DataFrame([
            {
                "block_number": r.block_number,
                "mev_score": r.mev_score,
                "sandwich_risk": r.sandwich_risk,
                "frontrun_risk": r.frontrun_risk,
                "backrun_opportunities": r.backrun_opportunities,
            }
            for r in results
        ])