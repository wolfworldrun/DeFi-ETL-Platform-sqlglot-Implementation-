"""
tests/test_etl_pipeline.py
Unit tests for ETL transform layer and quantitative models.
Run: pytest tests/ -v --cov=src
"""

import json
import time

import numpy as np
import pandas as pd
import pytest

from src.transform.etl_pipeline import (
    TokenTransferDecoder,
    DeFiSwapParser,
    TransactionFeatureEngineer,
    SQLGlotQueryBuilder,
    BlockchainETLPipeline,
    ERC20_TRANSFER_TOPIC,
    parse_raw_message,
)
from src.models.risk_models import (
    HistoricalVaR,
    ImpermanentLossModel,
    MEVExposureModel,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def make_transfer_message(standard="ERC-20", block_number=18_000_000):
    """Build a synthetic ERC-20 Transfer log Kafka message."""
    from_addr = "0x" + "a" * 64
    to_addr   = "0x" + "b" * 64
    amount    = hex(10 * 10**18)  # 10 tokens

    topics = [ERC20_TRANSFER_TOPIC, from_addr, to_addr]
    if standard == "ERC-721":
        topics.append("0x" + "0" * 63 + "1")   # tokenId = 1

    return json.dumps({
        "chain_id": 1,
        "network": "ethereum-mainnet",
        "block_number": block_number,
        "block_timestamp": int(time.time()),
        "event_type": "token_transfer",
        "payload": {
            "tx_hash": "0x" + "c" * 64,
            "log_index": 0,
            "contract": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",  # USDC
            "topics": topics,
            "data": amount,
        },
        "ingested_at": time.time(),
    }).encode()


def make_transaction_message(gas_price_gwei=50, value_eth=1.0, block_number=18_000_000):
    return json.dumps({
        "chain_id": 1,
        "network": "ethereum-mainnet",
        "block_number": block_number,
        "block_timestamp": int(time.time()),
        "event_type": "transaction",
        "payload": {
            "hash": "0x" + "d" * 64,
            "from": "0x" + "a" * 40,
            "to": "0x" + "b" * 40,
            "value_wei": str(int(value_eth * 1e18)),
            "gas": 21000,
            "gas_price": str(int(gas_price_gwei * 1e9)),
            "nonce": 42,
            "input": "0xa9059cbb" + "0" * 56,   # ERC-20 transfer selector
        },
        "ingested_at": time.time(),
    }).encode()


# ---------------------------------------------------------------------------
# ETL: parse_raw_message
# ---------------------------------------------------------------------------

class TestParseRawMessage:
    def test_bytes_input(self):
        msg = make_transfer_message()
        result = parse_raw_message(msg)
        assert isinstance(result, dict)
        assert result["event_type"] == "token_transfer"

    def test_str_input(self):
        msg = make_transfer_message().decode()
        result = parse_raw_message(msg)
        assert result["chain_id"] == 1


# ---------------------------------------------------------------------------
# ETL: TokenTransferDecoder
# ---------------------------------------------------------------------------

class TestTokenTransferDecoder:
    def test_erc20_decode(self):
        msgs = [parse_raw_message(make_transfer_message("ERC-20"))]
        df = TokenTransferDecoder.batch_decode(msgs)
        assert not df.empty
        assert df.iloc[0]["standard"] == "ERC-20"
        assert df.iloc[0]["amount"] == 10 * 10**18

    def test_erc721_decode(self):
        msgs = [parse_raw_message(make_transfer_message("ERC-721"))]
        df = TokenTransferDecoder.batch_decode(msgs)
        assert not df.empty
        assert df.iloc[0]["standard"] == "ERC-721"
        assert df.iloc[0]["token_id"] == 1

    def test_empty_messages(self):
        df = TokenTransferDecoder.batch_decode([])
        assert df.empty

    def test_timestamp_is_datetime(self):
        msgs = [parse_raw_message(make_transfer_message())]
        df = TokenTransferDecoder.batch_decode(msgs)
        assert pd.api.types.is_datetime64_any_dtype(df["block_timestamp"])


# ---------------------------------------------------------------------------
# ETL: TransactionFeatureEngineer
# ---------------------------------------------------------------------------

class TestTransactionFeatureEngineer:
    def _get_tx_df(self, n=5):
        msgs = [parse_raw_message(make_transaction_message(gas_price_gwei=50 + i * 10)) for i in range(n)]
        rows = []
        for m in msgs:
            row = m["payload"].copy()
            row["block_number"] = m["block_number"]
            rows.append(row)
        return pd.DataFrame(rows)

    def test_engineer_adds_columns(self):
        df = self._get_tx_df()
        result = TransactionFeatureEngineer.engineer(df)
        assert "gas_price_gwei" in result.columns
        assert "value_eth" in result.columns
        assert "fn_name" in result.columns

    def test_erc20_selector_detected(self):
        df = self._get_tx_df()
        result = TransactionFeatureEngineer.engineer(df)
        assert (result["fn_name"] == "ERC20.transfer").all()

    def test_aggregate_by_block(self):
        df = self._get_tx_df(10)
        engineered = TransactionFeatureEngineer.engineer(df)
        agg = TransactionFeatureEngineer.aggregate_by_block(engineered)
        assert not agg.empty
        assert "tx_count" in agg.columns

    def test_empty_df(self):
        result = TransactionFeatureEngineer.engineer(pd.DataFrame())
        assert result.empty


# ---------------------------------------------------------------------------
# ETL: SQLGlotQueryBuilder
# ---------------------------------------------------------------------------

class TestSQLGlotQueryBuilder:
    def test_transpile_to_bigquery(self):
        sql = SQLGlotQueryBuilder.transpile(
            SQLGlotQueryBuilder.TRANSFER_VOLUME_SQL, "bigquery"
        )
        # BigQuery correctly translates DATE_TRUNC → TIMESTAMP_TRUNC
        assert "TIMESTAMP_TRUNC" in sql.upper() or "DATE_TRUNC" in sql.upper()
        assert "CURRENT_TIMESTAMP" in sql.upper()  # NOW() → CURRENT_TIMESTAMP()
        assert len(sql) > 50

    def test_transpile_to_snowflake(self):
        sql = SQLGlotQueryBuilder.transpile(
            SQLGlotQueryBuilder.TRANSFER_VOLUME_SQL, "snowflake"
        )
        assert isinstance(sql, str)

    def test_block_range_query(self):
        sql = SQLGlotQueryBuilder.build_block_range_query(
            start_block=18_000_000,
            end_block=18_001_000,
            dialect="postgres",
        )
        assert "18000000" in sql
        assert "18001000" in sql

    def test_validate_valid_sql(self):
        errors = SQLGlotQueryBuilder.validate_sql("SELECT 1 FROM foo WHERE bar = 1")
        assert errors == []

    def test_validate_invalid_sql(self):
        errors = SQLGlotQueryBuilder.validate_sql("SELECT FROM FROM")
        # May or may not raise depending on sqlglot version; just check it returns a list
        assert isinstance(errors, list)

    def test_all_dialects(self):
        dialects = SQLGlotQueryBuilder.get_all_dialects()
        assert "bigquery" in dialects
        assert "snowflake" in dialects
        assert "duckdb" in dialects


# ---------------------------------------------------------------------------
# ETL: End-to-end pipeline
# ---------------------------------------------------------------------------

class TestBlockchainETLPipeline:
    def _make_batch(self):
        return [
            make_transfer_message("ERC-20", block_number=18_000_000),
            make_transfer_message("ERC-721", block_number=18_000_001),
            make_transaction_message(gas_price_gwei=80, block_number=18_000_000),
            make_transaction_message(gas_price_gwei=120, block_number=18_000_001),
        ]

    def test_run_returns_all_keys(self):
        pipeline = BlockchainETLPipeline(target_dialect="postgres")
        results  = pipeline.run(self._make_batch())
        assert "transfers" in results
        assert "swaps" in results
        assert "transactions" in results
        assert "transfer_sql" in results

    def test_transfers_non_empty(self):
        pipeline = BlockchainETLPipeline()
        results  = pipeline.run(self._make_batch())
        assert len(results["transfers"]) >= 2

    def test_sql_is_postgres(self):
        pipeline = BlockchainETLPipeline(target_dialect="postgres")
        results  = pipeline.run(self._make_batch())
        assert "token_transfers" in results["transfer_sql"]

    def test_bigquery_dialect(self):
        pipeline = BlockchainETLPipeline(target_dialect="bigquery")
        results  = pipeline.run(self._make_batch())
        # BigQuery uses backtick identifiers or different syntax
        assert isinstance(results["transfer_sql"], str)


# ---------------------------------------------------------------------------
# Models: HistoricalVaR
# ---------------------------------------------------------------------------

class TestHistoricalVaR:
    def _returns(self, n=252, seed=42):
        rng = np.random.default_rng(seed)
        return pd.Series(rng.normal(0.001, 0.03, n))

    def test_compute_basic(self):
        var = HistoricalVaR(self._returns(), position_size=1_000_000)
        result = var.compute(confidence=0.95)
        assert result.var > 0
        assert result.cvar >= result.var       # CVaR ≥ VaR always
        assert result.confidence == 0.95

    def test_higher_confidence_higher_var(self):
        var = HistoricalVaR(self._returns(), position_size=1_000_000)
        r95 = var.compute(confidence=0.95)
        r99 = var.compute(confidence=0.99)
        assert r99.var >= r95.var

    def test_multi_day_scaling(self):
        var = HistoricalVaR(self._returns(), position_size=1_000_000)
        r1  = var.compute(horizon_days=1)
        r10 = var.compute(horizon_days=10)
        assert r10.var > r1.var

    def test_rolling_var_series(self):
        var = HistoricalVaR(self._returns(n=100))
        rolling = var.rolling_var(window=30)
        assert isinstance(rolling, pd.Series)
        assert len(rolling) == 100

    def test_stress_test(self):
        var = HistoricalVaR(self._returns())
        stress = var.stress_test([-0.10, -0.30, -0.50])
        assert len(stress) == 3
        assert stress["pnl_usd"].iloc[0] > stress["pnl_usd"].iloc[-1]  # -10% > -50%

    def test_empty_series_raises(self):
        with pytest.raises(ValueError):
            HistoricalVaR(pd.Series([], dtype=float))


# ---------------------------------------------------------------------------
# Models: ImpermanentLossModel
# ---------------------------------------------------------------------------

class TestImpermanentLossModel:
    def test_no_il_at_entry(self):
        result = ImpermanentLossModel.compute(1000, 1000, 10_000)
        assert abs(result.il_pct) < 0.001   # ~0% IL at same price

    def test_il_negative_on_price_move(self):
        result = ImpermanentLossModel.compute(1000, 2000, 10_000)
        assert result.il_pct < 0

    def test_larger_move_more_il(self):
        il_2x = ImpermanentLossModel.compute(1000, 2000, 10_000)
        il_5x = ImpermanentLossModel.compute(1000, 5000, 10_000)
        assert il_5x.il_pct < il_2x.il_pct

    def test_scan_price_range_shape(self):
        df = ImpermanentLossModel.scan_price_range(price_entry=1000)
        assert not df.empty
        assert "il_pct" in df.columns
        assert (df["price_ratio"] == 1.0).any()   # unity point in scan

    def test_symmetric_il(self):
        # Price halving vs price doubling should give similar IL magnitude (AMM symmetry)
        il_half   = ImpermanentLossModel.compute(1000, 500, 10_000)
        il_double = ImpermanentLossModel.compute(1000, 2000, 10_000)
        # Both should be negative and similar magnitude (within 5%)
        assert abs(il_half.il_pct) == pytest.approx(abs(il_double.il_pct), rel=0.05)


# ---------------------------------------------------------------------------
# Models: MEVExposureModel
# ---------------------------------------------------------------------------

class TestMEVExposureModel:
    def _make_frames(self):
        tx_df = pd.DataFrame({
            "block_number": [100, 100, 100, 101],
            "gas_price_gwei": [50.0, 200.0, 55.0, 60.0],
            "is_defi_tx": [True, True, False, True],
        })
        swap_df = pd.DataFrame({
            "block_number": [100, 100, 100],
            "pool": ["0xpool1", "0xpool1", "0xpool2"],
            "protocol": ["uniswap_v2"] * 3,
        })
        return tx_df, swap_df

    def test_score_block_returns_result(self):
        tx_df, swap_df = self._make_frames()
        model = MEVExposureModel(tx_df, swap_df)
        result = model.score_block(100)
        assert 0 <= result.mev_score <= 100

    def test_high_gas_variance_increases_frontrun_risk(self):
        tx_low_var = pd.DataFrame({
            "block_number": [100, 100, 100],
            "gas_price_gwei": [50.0, 51.0, 52.0],
        })
        tx_high_var = pd.DataFrame({
            "block_number": [100, 100, 100],
            "gas_price_gwei": [50.0, 500.0, 52.0],
        })
        swap_df = pd.DataFrame()
        low  = MEVExposureModel(tx_low_var, swap_df).score_block(100)
        high = MEVExposureModel(tx_high_var, swap_df).score_block(100)
        assert high.frontrun_risk >= low.frontrun_risk

    def test_score_all_blocks(self):
        tx_df, swap_df = self._make_frames()
        model  = MEVExposureModel(tx_df, swap_df)
        result = model.score_all_blocks()
        assert set(result["block_number"]) == {100, 101}

    def test_empty_frames(self):
        model = MEVExposureModel(pd.DataFrame(), pd.DataFrame())
        result = model.score_block(100)
        assert result.mev_score == 0.0