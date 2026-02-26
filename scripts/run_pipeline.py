"""
scripts/run_pipeline.py
Entry point for the blockchain ETL pipeline.

Usage:
  python scripts/run_pipeline.py --network mainnet --dialect bigquery
  python scripts/run_pipeline.py --network polygon --start-block 50000000
"""

import argparse
import asyncio
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import structlog

log = structlog.get_logger(__name__)


def parse_args():
    p = argparse.ArgumentParser(description="Blockchain ETL Pipeline Runner")
    p.add_argument("--network",      default="ethereum-mainnet")
    p.add_argument("--rpc-url",      default=os.getenv("RPC_URL", "https://eth.llamarpc.com"))
    p.add_argument("--kafka",        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    p.add_argument("--dialect",      default="postgres", help="SQLGlot output dialect")
    p.add_argument("--start-block",  type=int, default=None)
    p.add_argument("--chain-id",     type=int, default=1)
    p.add_argument("--dry-run",      action="store_true", help="Validate config only")
    return p.parse_args()


async def main():
    args = parse_args()

    log.info(
        "pipeline.init",
        network=args.network,
        chain_id=args.chain_id,
        dialect=args.dialect,
        dry_run=args.dry_run,
    )

    if args.dry_run:
        # Validate imports and SQLGlot transpilation
        from src.transform.etl_pipeline import SQLGlotQueryBuilder
        builder = SQLGlotQueryBuilder()
        sql = builder.transpile(builder.TRANSFER_VOLUME_SQL, args.dialect)
        log.info("dry_run.sqlglot_ok", dialect=args.dialect, sql_preview=sql[:120])

        from src.models.risk_models import ImpermanentLossModel
        il = ImpermanentLossModel.compute(2000, 4000, 100_000)
        log.info("dry_run.il_model_ok", il_pct=round(il.il_pct, 2))

        print("\n✅ Dry-run passed. All components imported and validated.")
        return

    # Production: start Kafka producer stream
    from src.ingestion.kafka_producer import BlockchainKafkaProducer
    producer = BlockchainKafkaProducer(
        rpc_url=args.rpc_url,
        kafka_servers=args.kafka,
        chain_id=args.chain_id,
        network=args.network,
    )
    await producer.stream(start_block=args.start_block)


if __name__ == "__main__":
    asyncio.run(main())
