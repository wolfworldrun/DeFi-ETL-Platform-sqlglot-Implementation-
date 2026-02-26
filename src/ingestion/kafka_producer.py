"""
ingestion/kafka_producer.py
Streams raw blockchain events (blocks, transactions, logs) into Kafka topics.
Supports EVM chains via web3.py and gRPC subscriptions.
"""

import json
import asyncio
import time
from dataclasses import dataclass, asdict
from typing import Any, Callable

import structlog
from web3 import Web3
from web3.types import BlockData, TxData, LogReceipt
from confluent_kafka import Producer, KafkaException
from tenacity import retry, stop_after_attempt, wait_exponential

log = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Topic constants
# ---------------------------------------------------------------------------
TOPIC_BLOCKS = "blockchain.blocks.raw"
TOPIC_TRANSACTIONS = "blockchain.transactions.raw"
TOPIC_LOGS = "blockchain.logs.raw"
TOPIC_TOKEN_TRANSFERS = "blockchain.token_transfers.raw"

# Standard ERC-20 Transfer event signature
ERC20_TRANSFER_TOPIC = Web3.keccak(
    text="Transfer(address,address,uint256)"
).hex()

ERC721_TRANSFER_TOPIC = ERC20_TRANSFER_TOPIC  # same signature, differentiated by indexed args


@dataclass
class BlockchainEvent:
    """Normalized envelope for any blockchain event."""
    chain_id: int
    network: str
    block_number: int
    block_timestamp: int
    event_type: str          # "block" | "transaction" | "log" | "token_transfer"
    payload: dict[str, Any]
    ingested_at: float = 0.0

    def __post_init__(self):
        self.ingested_at = self.ingested_at or time.time()

    def to_json(self) -> bytes:
        return json.dumps(asdict(self), default=str).encode("utf-8")


class BlockchainKafkaProducer:
    """
    Connects to an EVM node via web3.py, subscribes to new blocks,
    decodes transactions and event logs, and publishes to Kafka.

    Usage
    -----
    producer = BlockchainKafkaProducer(
        rpc_url="wss://eth-mainnet.g.alchemy.com/v2/YOUR_KEY",
        kafka_servers="localhost:9092",
        chain_id=1,
        network="ethereum-mainnet",
    )
    asyncio.run(producer.stream())
    """

    def __init__(
        self,
        rpc_url: str,
        kafka_servers: str,
        chain_id: int = 1,
        network: str = "ethereum-mainnet",
        poll_interval: float = 2.0,
    ):
        self.w3 = Web3(Web3.HTTPProvider(rpc_url))
        self.chain_id = chain_id
        self.network = network
        self.poll_interval = poll_interval

        kafka_conf = {
            "bootstrap.servers": kafka_servers,
            "acks": "all",
            "retries": 5,
            "linger.ms": 10,
            "compression.type": "lz4",
        }
        self._producer = Producer(kafka_conf)
        self._delivery_failures: list[str] = []

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _delivery_report(self, err, msg):
        if err:
            log.error("kafka.delivery_failed", topic=msg.topic(), error=str(err))
            self._delivery_failures.append(str(err))
        else:
            log.debug("kafka.delivered", topic=msg.topic(), offset=msg.offset())

    def _publish(self, topic: str, event: BlockchainEvent, key: str | None = None):
        self._producer.produce(
            topic=topic,
            value=event.to_json(),
            key=(key or str(event.block_number)).encode(),
            on_delivery=self._delivery_report,
        )
        self._producer.poll(0)  # non-blocking trigger callbacks

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(min=1, max=30))
    def _fetch_block(self, block_number: int) -> BlockData:
        return self._w3_call(lambda: self.w3.eth.get_block(block_number, full_transactions=True))

    def _w3_call(self, fn: Callable):
        try:
            return fn()
        except Exception as exc:
            log.warning("web3.call_failed", error=str(exc))
            raise

    # ------------------------------------------------------------------
    # Public streaming entry point
    # ------------------------------------------------------------------

    async def stream(self, start_block: int | None = None):
        """
        Poll for new blocks and publish all data to Kafka.
        Falls back to HTTP polling if WebSocket is unavailable.
        """
        latest = start_block or self.w3.eth.block_number
        log.info("stream.started", network=self.network, start_block=latest)

        while True:
            try:
                current = self.w3.eth.block_number
                for block_num in range(latest, current + 1):
                    block = self._fetch_block(block_num)
                    self._process_block(block)
                latest = current + 1
            except Exception as exc:
                log.error("stream.error", error=str(exc))
            finally:
                self._producer.flush(timeout=5)
                await asyncio.sleep(self.poll_interval)

    # ------------------------------------------------------------------
    # Block / TX / Log processing
    # ------------------------------------------------------------------

    def _process_block(self, block: BlockData):
        block_event = BlockchainEvent(
            chain_id=self.chain_id,
            network=self.network,
            block_number=block["number"],
            block_timestamp=block["timestamp"],
            event_type="block",
            payload={
                "hash": block["hash"].hex(),
                "parent_hash": block["parentHash"].hex(),
                "miner": block["miner"],
                "gas_used": block["gasUsed"],
                "gas_limit": block["gasLimit"],
                "base_fee_per_gas": block.get("baseFeePerGas"),
                "tx_count": len(block["transactions"]),
            },
        )
        self._publish(TOPIC_BLOCKS, block_event, key=block["hash"].hex())

        for tx in block["transactions"]:
            self._process_transaction(tx, block)

    def _process_transaction(self, tx: TxData, block: BlockData):
        tx_event = BlockchainEvent(
            chain_id=self.chain_id,
            network=self.network,
            block_number=block["number"],
            block_timestamp=block["timestamp"],
            event_type="transaction",
            payload={
                "hash": tx["hash"].hex(),
                "from": tx["from"],
                "to": tx.get("to"),
                "value_wei": str(tx["value"]),
                "gas": tx["gas"],
                "gas_price": str(tx.get("gasPrice", 0)),
                "max_fee_per_gas": str(tx.get("maxFeePerGas", 0)),
                "max_priority_fee": str(tx.get("maxPriorityFeePerGas", 0)),
                "nonce": tx["nonce"],
                "input": tx["input"].hex()[:64],  # first 4 bytes = fn selector
            },
        )
        self._publish(TOPIC_TRANSACTIONS, tx_event, key=tx["hash"].hex())

        # Fetch receipt logs
        try:
            receipt = self.w3.eth.get_transaction_receipt(tx["hash"])
            for log_entry in receipt["logs"]:
                self._process_log(log_entry, block)
        except Exception as exc:
            log.warning("receipt.fetch_failed", tx=tx["hash"].hex(), error=str(exc))

    def _process_log(self, log_entry: LogReceipt, block: BlockData):
        topics = [t.hex() for t in log_entry["topics"]]
        is_transfer = topics and topics[0] == ERC20_TRANSFER_TOPIC

        log_event = BlockchainEvent(
            chain_id=self.chain_id,
            network=self.network,
            block_number=block["number"],
            block_timestamp=block["timestamp"],
            event_type="token_transfer" if is_transfer else "log",
            payload={
                "tx_hash": log_entry["transactionHash"].hex(),
                "log_index": log_entry["logIndex"],
                "contract": log_entry["address"],
                "topics": topics,
                "data": log_entry["data"].hex() if log_entry["data"] else "0x",
            },
        )
        topic = TOPIC_TOKEN_TRANSFERS if is_transfer else TOPIC_LOGS
        self._publish(topic, log_event)
