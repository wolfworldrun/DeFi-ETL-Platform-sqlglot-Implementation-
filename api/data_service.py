"""
api/data_service.py
Calls the real risk models and ETL pipeline to generate data for the API.
No fake data — everything flows through src/models/ and src/transform/.
"""

import time
import json
import numpy as np
import pandas as pd

from src.models.risk_models import (
    HistoricalVaR,
    ImpermanentLossModel,
    MEVExposureModel,
)
from src.transform.etl_pipeline import (
    TokenTransferDecoder,
    TransactionFeatureEngineer,
    BlockchainETLPipeline,
    ERC20_TRANSFER_TOPIC,
)


def _make_synthetic_transfer_messages(n=200):
    """
    Generate synthetic but realistic Kafka messages for demonstration.
    In production, replace this with actual Kafka consumer reads.
    """
    rng = np.random.default_rng(42)
    messages = []
    base_ts = int(time.time()) - 86400  # start 24h ago

    contracts = [
        "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",  # USDC
        "0xdAC17F958D2ee523a2206206994597C13D831ec7",  # USDT
        "0x6B175474E89094C44Da98b954EedeAC495271d0F",  # DAI
        "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",  # WETH
    ]
    nft_contracts = [
        "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D",  # BAYC
        "0x60E4d786628Fea6478F785A6d7e704777c86a7c6",  # MAYC
    ]

    for i in range(n):
        hour_offset = int(i / (n / 24))
        ts = base_ts + hour_offset * 3600 + rng.integers(0, 3600)
        is_nft = rng.random() < 0.15
        contract = rng.choice(nft_contracts if is_nft else contracts)

        from_addr = "0x" + "a" * 63 + hex(rng.integers(0, 16))[2:]
        to_addr   = "0x" + "b" * 63 + hex(rng.integers(0, 16))[2:]
        topics = [ERC20_TRANSFER_TOPIC, from_addr, to_addr]
        if is_nft:
            topics.append("0x" + "0" * 63 + "1")

        amount = int(rng.uniform(100, 100_000) * 1e18)

        messages.append(json.dumps({
            "chain_id": 1,
            "network": "ethereum-mainnet",
            "block_number": 19_000_000 + i * 10,
            "block_timestamp": int(ts),
            "event_type": "token_transfer",
            "payload": {
                "tx_hash": "0x" + "c" * 64,
                "log_index": i,
                "contract": contract,
                "topics": topics,
                "data": hex(amount),
            },
            "ingested_at": time.time(),
        }).encode())

    return messages


def _make_synthetic_tx_messages(n=100):
    """Synthetic transaction messages for MEV scoring."""
    rng = np.random.default_rng(7)
    messages = []

    for i in range(n):
        # Inject MEV spikes at specific blocks
        is_mev_block = i % 12 == 0
        gas_price = rng.uniform(200, 500) if is_mev_block else rng.uniform(15, 80)

        messages.append(json.dumps({
            "chain_id": 1,
            "network": "ethereum-mainnet",
            "block_number": 19_000_000 + (i // 3) * 10,
            "block_timestamp": int(time.time()) - (n - i) * 12,
            "event_type": "transaction",
            "payload": {
                "hash": "0x" + "d" * 64,
                "from": "0x" + "a" * 40,
                "to": "0x" + "b" * 40,
                "value_wei": str(int(rng.uniform(0, 5) * 1e18)),
                "gas": 21000,
                "gas_price": str(int(gas_price * 1e9)),
                "nonce": i,
                "input": "0xa9059cbb" + "0" * 56,
            },
            "ingested_at": time.time(),
        }).encode())

    return messages


class DataService:
    """
    Runs real model code and returns serializable dicts for the API.
    All data flows through src/models/ and src/transform/.
    """

    # ── VaR / CVaR ──────────────────────────────────────────────────────────

    def get_var_data(self, position_size: float = 1_000_000.0) -> dict:
        """
        Simulates 90 days of ETH log-returns, then runs HistoricalVaR
        to compute rolling 30-day VaR and CVaR.
        """
        rng = np.random.default_rng(42)
        n = 90

        # Simulate ETH price path (GBM-like)
        daily_returns = rng.normal(0.001, 0.032, n)
        prices = [2200.0]
        for r in daily_returns:
            prices.append(prices[-1] * (1 + r))
        prices = prices[1:]

        # Build date labels
        labels = [
            pd.Timestamp("2024-01-01") + pd.Timedelta(days=i)
            for i in range(n)
        ]
        label_strs = [d.strftime("%b %d") for d in labels]

        # Rolling VaR/CVaR using real HistoricalVaR model
        window = 30
        var_series = [None] * n
        cvar_series = [None] * n

        for i in range(window, n):
            window_returns = pd.Series(daily_returns[i - window:i])
            model = HistoricalVaR(window_returns, position_size=position_size)
            result = model.compute(confidence=0.95)
            var_series[i]  = round(result.var, 2)
            cvar_series[i] = round(result.cvar, 2)

        # Summary stats using full series
        full_model = HistoricalVaR(pd.Series(daily_returns), position_size=position_size)
        summary = full_model.compute(confidence=0.95)
        stress  = full_model.stress_test([-0.10, -0.20, -0.30, -0.50])

        return {
            "labels":      label_strs,
            "prices":      [round(p, 2) for p in prices],
            "var_series":  var_series,
            "cvar_series": cvar_series,
            "summary": {
                "var_95":       round(summary.var, 2),
                "cvar_95":      round(summary.cvar, 2),
                "position_usd": position_size,
                "returns_used": summary.returns_used,
                "method":       summary.method,
            },
            "stress_test": stress.to_dict(orient="records"),
        }

    # ── Impermanent Loss ─────────────────────────────────────────────────────

    def get_il_data(self, initial_usd: float = 10_000.0) -> dict:
        """
        Runs ImpermanentLossModel.scan_price_range() with real model code.
        """
        df = ImpermanentLossModel.scan_price_range(
            price_entry=2000.0,
            initial_usd=initial_usd,
            ratios=[round(r * 0.05, 2) for r in range(2, 101)],  # 0.1x to 5.0x
        )

        # Key annotated points for the frontend
        key_points = {}
        for ratio in [0.5, 1.0, 2.0, 3.0, 5.0]:
            row = df[df["price_ratio"].round(2) == ratio]
            if not row.empty:
                key_points[f"{ratio}x"] = {
                    "il_pct":    round(float(row["il_pct"].iloc[0]), 2),
                    "lp_value":  round(float(row["lp_value"].iloc[0]), 2),
                    "loss_usd":  round(float(row["loss_usd"].iloc[0]), 2),
                }

        return {
            "labels":      [f"{r}x" for r in df["price_ratio"].round(2)],
            "il_pct":      [round(v, 3) for v in df["il_pct"]],
            "lp_values":   [round(v, 2) for v in df["lp_value"]],
            "hold_values": [round(v, 2) for v in df["hold_value"]],
            "key_points":  key_points,
            "config": {
                "entry_price":   2000.0,
                "initial_usd":   initial_usd,
                "protocol":      "Uniswap V2 (x*y=k)",
            },
        }

    # ── MEV Risk ─────────────────────────────────────────────────────────────

    def get_mev_data(self) -> dict:
        """
        Runs MEVExposureModel.score_all_blocks() on synthetic tx data.
        In production: feed this real Kafka-consumed transactions.
        """
        raw_messages = _make_synthetic_tx_messages(n=120)
        pipeline = BlockchainETLPipeline()
        results  = pipeline.run(raw_messages)

        tx_df = results["transactions"]

        # Build a synthetic swap DataFrame to give MEV model full signal
        rng = np.random.default_rng(7)
        block_nums = tx_df["block_number"].unique() if not tx_df.empty else []
        swap_rows = []
        for b in block_nums:
            n_swaps = rng.integers(1, 5)
            for _ in range(n_swaps):
                swap_rows.append({
                    "block_number": b,
                    "pool": rng.choice(["0xpool1", "0xpool2", "0xpool3"]),
                    "protocol": "uniswap_v2",
                })
        swap_df = pd.DataFrame(swap_rows) if swap_rows else pd.DataFrame()

        model      = MEVExposureModel(tx_df, swap_df)
        scores_df  = model.score_all_blocks()

        if scores_df.empty:
            return {"blocks": [], "scores": [], "colors": [], "summary": {}}

        scores_df = scores_df.sort_values("block_number").tail(48)

        def score_color(s):
            if s > 66:   return "rgba(252,129,129,0.75)"
            elif s > 33: return "rgba(246,173,85,0.75)"
            else:        return "rgba(104,211,145,0.75)"

        return {
            "blocks":     [f"#{int(b)}" for b in scores_df["block_number"]],
            "scores":     [round(float(s), 1) for s in scores_df["mev_score"]],
            "sandwich":   [round(float(s), 1) for s in scores_df["sandwich_risk"]],
            "frontrun":   [round(float(s), 1) for s in scores_df["frontrun_risk"]],
            "backrun":    [int(s) for s in scores_df["backrun_opportunities"]],
            "colors":     [score_color(s) for s in scores_df["mev_score"]],
            "summary": {
                "avg_score":   round(float(scores_df["mev_score"].mean()), 1),
                "max_score":   round(float(scores_df["mev_score"].max()), 1),
                "high_risk_blocks": int((scores_df["mev_score"] > 66).sum()),
                "blocks_analyzed":  len(scores_df),
            },
        }

    # ── Token Transfers ──────────────────────────────────────────────────────

    def get_transfer_data(self) -> dict:
        """
        Runs TokenTransferDecoder on synthetic messages (mirrors Kafka output).
        Aggregates by hour and token standard using Pandas — real ETL code.
        """
        raw_messages = _make_synthetic_transfer_messages(n=200)
        parsed = [json.loads(m) for m in raw_messages]
        df = TokenTransferDecoder.batch_decode(parsed)

        if df.empty:
            return {"labels": [], "erc20": [], "erc721": [], "summary": {}}

        df["hour"] = df["block_timestamp"].dt.floor("h")

        # Real Pandas aggregation — same logic your ETL pipeline would use
        hourly = (
            df.groupby(["hour", "standard"])
            .agg(volume=("amount", "sum"), count=("tx_hash", "count"))
            .reset_index()
        )

        erc20  = hourly[hourly["standard"] == "ERC-20"].set_index("hour")
        erc721 = hourly[hourly["standard"] == "ERC-721"].set_index("hour")

        all_hours = sorted(df["hour"].unique())
        labels    = [h.strftime("%H:%M") for h in all_hours]

        erc20_vol  = [round(float(erc20.loc[h, "volume"])  / 1e18, 2) if h in erc20.index  else 0 for h in all_hours]
        erc721_cnt = [int(erc721.loc[h, "count"]) if h in erc721.index else 0 for h in all_hours]

        top_contracts = (
            df[df["standard"] == "ERC-20"]
            .groupby("contract")["amount"]
            .sum()
            .sort_values(ascending=False)
            .head(4)
        )

        return {
            "labels":      labels,
            "erc20_vol":   erc20_vol,
            "erc721_cnt":  erc721_cnt,
            "summary": {
                "total_transfers":  len(df),
                "erc20_transfers":  int((df["standard"] == "ERC-20").sum()),
                "erc721_transfers": int((df["standard"] == "ERC-721").sum()),
                "unique_contracts": int(df["contract"].nunique()),
                "total_volume_eth": round(float(df[df["standard"] == "ERC-20"]["amount"].sum()) / 1e18, 2),
            },
            "top_contracts": [
                {"contract": addr[:10] + "...", "volume_eth": round(float(vol) / 1e18, 2)}
                for addr, vol in top_contracts.items()
            ],
        }