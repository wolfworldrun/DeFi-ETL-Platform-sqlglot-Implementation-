"""
scripts/backfill.py
Backfill historical blocks from a given range into the ETL pipeline.

Usage:
  python scripts/backfill.py --start-block 18000000 --end-block 18001000
  python scripts/backfill.py --start-block 18000000 --end-block 18001000 --dialect bigquery
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import structlog

log = structlog.get_logger(__name__)


def parse_args():
    p = argparse.ArgumentParser(description="Blockchain ETL Historical Backfill")
    p.add_argument("--start-block", type=int, required=True)
    p.add_argument("--end-block",   type=int, required=True)
    p.add_argument("--rpc-url",     default="https://eth.llamarpc.com")
    p.add_argument("--dialect",     default="postgres")
    p.add_argument("--batch-size",  type=int, default=100)
    return p.parse_args()


def main():
    args = parse_args()
    assert args.end_block >= args.start_block, "end-block must be >= start-block"

    total = args.end_block - args.start_block + 1
    log.info("backfill.start", start=args.start_block, end=args.end_block, total_blocks=total)

    from web3 import Web3
    from src.transform.etl_pipeline import BlockchainETLPipeline

    w3       = Web3(Web3.HTTPProvider(args.rpc_url))
    pipeline = BlockchainETLPipeline(target_dialect=args.dialect)

    for batch_start in range(args.start_block, args.end_block + 1, args.batch_size):
        batch_end = min(batch_start + args.batch_size - 1, args.end_block)
        log.info("backfill.batch", start=batch_start, end=batch_end)

        raw_messages = []
        for block_num in range(batch_start, batch_end + 1):
            try:
                block = w3.eth.get_block(block_num, full_transactions=True)
                # Minimal serialization for pipeline input
                import json, time
                for tx in block["transactions"]:
                    raw_messages.append(json.dumps({
                        "chain_id": 1, "network": "ethereum-mainnet",
                        "block_number": block["number"],
                        "block_timestamp": block["timestamp"],
                        "event_type": "transaction",
                        "payload": {
                            "hash": tx["hash"].hex(),
                            "from": tx["from"], "to": tx.get("to"),
                            "value_wei": str(tx["value"]),
                            "gas": tx["gas"],
                            "gas_price": str(tx.get("gasPrice", 0)),
                            "nonce": tx["nonce"],
                            "input": tx["input"].hex()[:10],
                        },
                        "ingested_at": time.time(),
                    }).encode())
            except Exception as exc:
                log.warning("backfill.block_failed", block=block_num, error=str(exc))

        if raw_messages:
            results = pipeline.run(raw_messages)
            log.info("backfill.batch_done",
                     transfers=len(results["transfers"]),
                     transactions=len(results["transactions"]))

    log.info("backfill.complete", total_blocks=total)


if __name__ == "__main__":
    main()
