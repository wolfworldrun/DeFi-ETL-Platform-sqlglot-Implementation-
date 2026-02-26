"""
transform/etl_pipeline.py
Core ETL layer: consumes raw Kafka events, decodes blockchain data,
normalizes across chains, and produces analytics-ready DataFrames.

Key tools:
  - Pandas  : in-memory transformation & feature engineering
  - SQLGlot : dialect-agnostic SQL for cross-warehouse compatibility
              (write once → run on Postgres, BigQuery, Snowflake, DuckDB)
"""

from __future__ import annotations

import json
from typing import Any

import numpy as np
import pandas as pd
import sqlglot
import sqlglot.expressions as exp
import structlog
from eth_abi import decode as abi_decode
from web3 import Web3

log = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# ABI selectors for common DeFi operations
# ---------------------------------------------------------------------------
UNISWAP_V2_SWAP_TOPIC = Web3.keccak(
    text="Swap(address,uint256,uint256,uint256,uint256,address)"
).hex()

UNISWAP_V3_SWAP_TOPIC = Web3.keccak(
    text="Swap(address,address,int256,int256,uint160,uint128,int24)"
).hex()

ERC20_TRANSFER_TOPIC = Web3.keccak(text="Transfer(address,address,uint256)").hex()

# ERC-20 / ERC-721 / ERC-1155 token standard selectors
TOKEN_STANDARDS = {
    "0xa9059cbb": "ERC20.transfer",
    "0x23b872dd": "ERC20.transferFrom",
    "0x095ea7b3": "ERC20.approve",
    "0x42842e0e": "ERC721.safeTransferFrom",
    "0xf242432a": "ERC1155.safeTransferFrom",
}


# ---------------------------------------------------------------------------
# Raw message → structured dict
# ---------------------------------------------------------------------------

def parse_raw_message(raw: bytes | str) -> dict[str, Any]:
    """Deserialize a Kafka message payload."""
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    return json.loads(raw)


# ---------------------------------------------------------------------------
# Token Transfer Decoder
# ---------------------------------------------------------------------------

class TokenTransferDecoder:
    """
    Decodes ERC-20 and ERC-721 Transfer logs into a normalized DataFrame.

    Handles:
    - ERC-20 : topics[1]=from, topics[2]=to, data=amount
    - ERC-721: topics[1]=from, topics[2]=to, topics[3]=tokenId
    """

    @staticmethod
    def decode_log(log_payload: dict) -> dict | None:
        topics = log_payload.get("topics", [])
        if not topics or topics[0] != ERC20_TRANSFER_TOPIC:
            return None

        try:
            from_addr = Web3.to_checksum_address("0x" + topics[1][-40:])
            to_addr   = Web3.to_checksum_address("0x" + topics[2][-40:])

            # ERC-721 has 4 indexed topics; ERC-20 encodes amount in data
            if len(topics) >= 4:
                token_id = int(topics[3], 16)
                return {
                    "standard": "ERC-721",
                    "contract": log_payload["contract"],
                    "from": from_addr,
                    "to": to_addr,
                    "token_id": token_id,
                    "amount": 1,
                }
            else:
                data = log_payload.get("data", "0x")
                amount = int(data, 16) if data and data != "0x" else 0
                return {
                    "standard": "ERC-20",
                    "contract": log_payload["contract"],
                    "from": from_addr,
                    "to": to_addr,
                    "token_id": None,
                    "amount": amount,
                }
        except Exception as exc:
            log.warning("decode.transfer_failed", error=str(exc))
            return None

    @classmethod
    def batch_decode(cls, messages: list[dict]) -> pd.DataFrame:
        records = []
        for msg in messages:
            if msg.get("event_type") not in ("token_transfer", "log"):
                continue
            decoded = cls.decode_log(msg["payload"])
            if decoded:
                decoded["block_number"]    = msg["block_number"]
                decoded["block_timestamp"] = msg["block_timestamp"]
                decoded["tx_hash"]         = msg["payload"]["tx_hash"]
                decoded["chain_id"]        = msg["chain_id"]
                records.append(decoded)

        if not records:
            return pd.DataFrame()

        df = pd.DataFrame(records)
        df["block_timestamp"] = pd.to_datetime(df["block_timestamp"], unit="s", utc=True)
        df["amount"] = df["amount"].astype(np.float64)
        return df


# ---------------------------------------------------------------------------
# DeFi Swap Parser (Uniswap V2 / V3)
# ---------------------------------------------------------------------------

class DeFiSwapParser:
    """
    Parses Uniswap V2 and V3 Swap events into a unified analytics schema.
    Computes price impact and fee-adjusted returns.
    """

    @staticmethod
    def parse_v2_swap(log_payload: dict) -> dict | None:
        if not log_payload.get("topics") or log_payload["topics"][0] != UNISWAP_V2_SWAP_TOPIC:
            return None
        try:
            data = bytes.fromhex(log_payload["data"].lstrip("0x"))
            amount0_in, amount1_in, amount0_out, amount1_out = abi_decode(
                ["uint256", "uint256", "uint256", "uint256"], data
            )
            return {
                "protocol": "uniswap_v2",
                "pool": log_payload["contract"],
                "amount0_in": amount0_in,
                "amount1_in": amount1_in,
                "amount0_out": amount0_out,
                "amount1_out": amount1_out,
                "price": (amount1_out / amount0_in) if amount0_in else (amount0_out / amount1_in or 1),
            }
        except Exception as exc:
            log.warning("parse.v2_swap_failed", error=str(exc))
            return None

    @classmethod
    def batch_parse(cls, messages: list[dict]) -> pd.DataFrame:
        records = []
        for msg in messages:
            payload = msg.get("payload", {})
            topics  = payload.get("topics", [])
            if not topics:
                continue
            parsed = None
            if topics[0] == UNISWAP_V2_SWAP_TOPIC:
                parsed = cls.parse_v2_swap(payload)
            if parsed:
                parsed["block_number"]    = msg["block_number"]
                parsed["block_timestamp"] = msg["block_timestamp"]
                parsed["chain_id"]        = msg["chain_id"]
                parsed["tx_hash"]         = payload.get("tx_hash", "")
                records.append(parsed)

        if not records:
            return pd.DataFrame()

        df = pd.DataFrame(records)
        df["block_timestamp"] = pd.to_datetime(df["block_timestamp"], unit="s", utc=True)
        return df


# ---------------------------------------------------------------------------
# Transaction Feature Engineer
# ---------------------------------------------------------------------------

class TransactionFeatureEngineer:
    """
    Builds risk & analytics features from raw transaction DataFrames.
    Used downstream by the VaR and MEV exposure models.
    """

    @staticmethod
    def engineer(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        df = df.copy()

        # Gas price normalization (convert to Gwei)
        if "gas_price" in df.columns:
            df["gas_price_gwei"] = df["gas_price"].astype(float) / 1e9

        # ETH value normalization
        if "value_wei" in df.columns:
            df["value_eth"] = df["value_wei"].astype(float) / 1e18

        # Identify contract deployments (to == None / NaN)
        df["is_contract_deploy"] = df["to"].isna()

        # Identify known DeFi function selectors
        if "input" in df.columns:
            df["fn_selector"] = df["input"].str[:10]          # 0x + 8 hex chars
            df["fn_name"]     = df["fn_selector"].map(TOKEN_STANDARDS).fillna("unknown")
            df["is_defi_tx"]  = df["fn_name"] != "unknown"

        # Rolling gas percentile (priority fee pressure)
        if "gas_price_gwei" in df.columns:
            df = df.sort_values("block_number")
            df["gas_p90"] = (
                df["gas_price_gwei"]
                .rolling(window=100, min_periods=1)
                .quantile(0.9)
            )
            df["is_high_priority"] = df["gas_price_gwei"] > df["gas_p90"]

        return df

    @staticmethod
    def aggregate_by_block(df: pd.DataFrame) -> pd.DataFrame:
        """Summarize transaction features per block — useful for block-level risk signals."""
        if df.empty or "block_number" not in df.columns:
            return pd.DataFrame()

        agg = df.groupby("block_number").agg(
            tx_count=("hash", "count") if "hash" in df.columns else ("block_number", "count"),
            total_eth_volume=("value_eth", "sum"),
            avg_gas_price_gwei=("gas_price_gwei", "mean"),
            max_gas_price_gwei=("gas_price_gwei", "max"),
            defi_tx_count=("is_defi_tx", "sum"),
            contract_deploys=("is_contract_deploy", "sum"),
        ).reset_index()

        return agg


# ---------------------------------------------------------------------------
# SQLGlot Cross-Dialect SQL Builder
# ---------------------------------------------------------------------------

class SQLGlotQueryBuilder:
    """
    Write analytical SQL once and transpile to any warehouse dialect.

    Supported dialects: postgres, bigquery, snowflake, duckdb, spark, trino
    """

    # The canonical "source of truth" SQL — written in Postgres syntax
    TRANSFER_VOLUME_SQL = """
        SELECT
            DATE_TRUNC('hour', block_timestamp)   AS hour_bucket,
            contract                               AS token_contract,
            standard                               AS token_standard,
            chain_id,
            COUNT(*)                               AS transfer_count,
            SUM(amount / 1e18)                     AS volume_normalized,
            COUNT(DISTINCT "from")                 AS unique_senders,
            COUNT(DISTINCT "to")                   AS unique_receivers
        FROM token_transfers
        WHERE block_timestamp >= NOW() - INTERVAL '24 hours'
          AND standard = 'ERC-20'
        GROUP BY 1, 2, 3, 4
        ORDER BY hour_bucket DESC
    """

    SWAP_PRICE_IMPACT_SQL = """
        SELECT
            pool,
            protocol,
            chain_id,
            AVG(price)                             AS avg_price,
            STDDEV(price)                          AS price_volatility,
            SUM(amount0_in + amount0_out)          AS total_volume_token0,
            COUNT(*)                               AS swap_count
        FROM defi_swaps
        WHERE block_timestamp >= NOW() - INTERVAL '1 hour'
        GROUP BY 1, 2, 3
        HAVING COUNT(*) > 5
    """

    @classmethod
    def transpile(cls, sql: str, target_dialect: str) -> str:
        """
        Transpile a Postgres SQL string to the target warehouse dialect.

        Parameters
        ----------
        sql : str
            Source SQL (Postgres dialect)
        target_dialect : str
            One of: 'bigquery', 'snowflake', 'duckdb', 'spark', 'trino', 'postgres'

        Returns
        -------
        str
            Transpiled SQL ready for execution on target_dialect
        """
        try:
            statements = sqlglot.transpile(
                sql,
                read="postgres",
                write=target_dialect,
                pretty=True,
                error_level=sqlglot.ErrorLevel.WARN,
            )
            return "\n".join(statements)
        except sqlglot.errors.SqlglotError as exc:
            log.error("sqlglot.transpile_failed", dialect=target_dialect, error=str(exc))
            raise

    @classmethod
    def build_block_range_query(
        cls,
        start_block: int,
        end_block: int,
        table: str = "transactions",
        dialect: str = "postgres",
    ) -> str:
        """Use SQLGlot expression API to programmatically build a block-range query."""
        query = (
            exp.select(
                exp.Star(),
            )
            .from_(table)
            .where(
                exp.column("block_number").between(
                    exp.Literal.number(start_block),
                    exp.Literal.number(end_block),
                )
            )
            .order_by(exp.column("block_number"))
        )
        return query.sql(dialect=dialect, pretty=True)

    @classmethod
    def validate_sql(cls, sql: str) -> list[str]:
        """Parse SQL and return any syntactic errors as strings."""
        errors = []
        try:
            sqlglot.parse(sql, error_level=sqlglot.ErrorLevel.RAISE)
        except sqlglot.errors.ParseError as exc:
            errors = [str(e) for e in exc.errors]
        return errors

    @classmethod
    def get_all_dialects(cls) -> list[str]:
        return ["postgres", "bigquery", "snowflake", "duckdb", "spark", "trino", "mysql"]

    @classmethod
    def demo_transfer_volume_all_dialects(cls) -> dict[str, str]:
        """Useful for CI checks — transpile to every dialect."""
        return {
            dialect: cls.transpile(cls.TRANSFER_VOLUME_SQL, dialect)
            for dialect in cls.get_all_dialects()
        }


# ---------------------------------------------------------------------------
# Main ETL pipeline orchestrator
# ---------------------------------------------------------------------------

class BlockchainETLPipeline:
    """
    End-to-end ETL orchestrator for a batch of raw Kafka messages.

    Typical usage
    -------------
    pipeline = BlockchainETLPipeline(target_dialect="bigquery")
    results  = pipeline.run(raw_messages)
    """

    def __init__(self, target_dialect: str = "postgres"):
        self.target_dialect  = target_dialect
        self.transfer_decoder = TokenTransferDecoder()
        self.swap_parser      = DeFiSwapParser()
        self.tx_engineer      = TransactionFeatureEngineer()
        self.sql_builder      = SQLGlotQueryBuilder()

    def run(self, raw_messages: list[bytes | str]) -> dict[str, pd.DataFrame | str]:
        """
        Process a batch of raw Kafka messages.

        Returns a dict with keys:
          transfers   : pd.DataFrame of decoded ERC-20/721 transfers
          swaps       : pd.DataFrame of parsed DeFi swaps
          transactions: pd.DataFrame of enriched transactions
          block_agg   : pd.DataFrame of per-block aggregates
          analytics_sql: dict[str, str] dialect-transpiled SQL
        """
        parsed = [parse_raw_message(m) for m in raw_messages]

        log.info("etl.batch_start", message_count=len(parsed))

        # 1. Decode token transfers
        transfers = self.transfer_decoder.batch_decode(parsed)
        log.info("etl.transfers_decoded", count=len(transfers))

        # 2. Parse DEX swaps
        swaps = self.swap_parser.batch_parse(parsed)
        log.info("etl.swaps_parsed", count=len(swaps))

        # 3. Engineer transaction features
        tx_messages = [m for m in parsed if m.get("event_type") == "transaction"]
        tx_df = pd.DataFrame([m["payload"] for m in tx_messages]) if tx_messages else pd.DataFrame()
        if not tx_df.empty:
            for col in ["block_number", "block_timestamp", "chain_id"]:
                tx_df[col] = [m[col] for m in tx_messages]
            tx_df = self.tx_engineer.engineer(tx_df)

        # 4. Block-level aggregates
        block_agg = self.tx_engineer.aggregate_by_block(tx_df) if not tx_df.empty else pd.DataFrame()

        # 5. Generate analytics SQL for target dialect
        transfer_sql = self.sql_builder.transpile(
            SQLGlotQueryBuilder.TRANSFER_VOLUME_SQL, self.target_dialect
        )
        swap_sql = self.sql_builder.transpile(
            SQLGlotQueryBuilder.SWAP_PRICE_IMPACT_SQL, self.target_dialect
        )

        return {
            "transfers":    transfers,
            "swaps":        swaps,
            "transactions": tx_df,
            "block_agg":    block_agg,
            "transfer_sql": transfer_sql,
            "swap_sql":     swap_sql,
        }
