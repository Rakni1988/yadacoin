import asyncio
import base64
import binascii
import hashlib
import json
import socket
import time
from datetime import timedelta
from decimal import Decimal, getcontext
from logging import getLogger

import pyrx
from bitcoin.signmessage import BitcoinMessage, VerifyMessage
from bitcoin.wallet import P2PKHBitcoinAddress
from coincurve.utils import verify_signature
from tornado.iostream import StreamClosedError
from tornado.tcpclient import TCPClient
from tornado.util import TimeoutError

import yadacoin.core.config
from yadacoin.core.chain import CHAIN
from yadacoin.core.config import Config
from yadacoin.core.latestblock import LatestBlock
from yadacoin.core.nodes import Nodes
from yadacoin.core.transaction import (
    InvalidTransactionException,
    Output,
    TotalValueMismatchException,
    Transaction,
    TransactionAddressInvalidException,
)
from yadacoin.core.transactionutils import TU


def quantize_eight(value):
    getcontext().prec = len(str(value)) + 8
    if value == -0.0:
        value = 0.0
    value = Decimal(value)
    value = value.quantize(Decimal("0.00000000"))
    return value

async def test_node(
    node,
    semaphore,
    min_balance,
    config,
    retries=2,
    delay=2
):
    async with semaphore:
        try:
            resolved_ip = socket.gethostbyname(node.host)
            config.app_log.info(f"Resolved {node.host} to {resolved_ip}")
        except socket.gaierror:
            config.app_log.warning(f"DNS resolution error for {node.host}")
            return {
                "node": node,
                "status": "DNSResolutionError",
                "address": None,
                "balance": None,
                "error": "DNSResolutionError"
            }

        for attempt in range(retries + 1):
            error = None
            try:
                stream = await TCPClient().connect(resolved_ip, node.port, timeout=timedelta(seconds=2))

                address = str(
                    P2PKHBitcoinAddress.from_pubkey(
                        bytes.fromhex(node.identity.public_key)
                    )
                )

                balance = await config.BU.get_final_balance(address)

                if balance >= min_balance:
                    config.app_log.info(f"Node {node.host}:{node.port} (address: {address}) has a balance of {balance}.")
                    return {
                        "node": node,
                        "status": "success",
                        "address": address,
                        "balance": balance,
                        "error": None
                    }
                else:
                    config.app_log.warning(f"Node {node.host}:{node.port} does not meet minimum balance requirements (balance: {balance}).")
                    return {
                        "node": node,
                        "status": "Insufficient funds",
                        "address": address,
                        "balance": balance,
                        "error": None
                    }

            except StreamClosedError:
                config.app_log.warning(f"Stream closed exception for {node.host}:{node.port}")
                error = "StreamClosedError"
            except TimeoutError:
                config.app_log.warning(f"Timeout exception for {node.host}:{node.port}")
                error = "TimeoutError"
            except Exception as e:
                config.app_log.warning(f"Unhandled exception in block for {node.host}:{node.port}, error: {e}")
                error = str(e)
            finally:
                if "stream" in locals() and not stream.closed():
                    stream.close()

            if error in ["TimeoutError"] and attempt < retries:
                config.app_log.info(f"Retrying test for {node.host}:{node.port} after {delay} seconds (attempt {attempt + 1}/{retries})")
                await asyncio.sleep(delay)
            else:
                break

        return {
            "node": node,
            "status": "error" if error else "fail",
            "address": None,
            "balance": None,
            "error": error
        }


class CoinbaseRule1(Exception):
    pass


class CoinbaseRule2(Exception):
    pass


class CoinbaseRule3(Exception):
    pass


class CoinbaseRule4(Exception):
    pass


class RelationshipRule1(Exception):
    pass


class RelationshipRule2(Exception):
    pass


class FastGraphRule1(Exception):
    pass


class FastGraphRule2(Exception):
    pass


class ExternalInputSpentException(Exception):
    pass


class UnknownOutputAddressException(Exception):
    pass


class Block(object):
    successful_nodes = []
    # Memory optimization
    __slots__ = (
        "app_log",
        "config",
        "mongo",
        "version",
        "time",
        "index",
        "prev_hash",
        "nonce",
        "transactions",
        "txn_hashes",
        "merkle_root",
        "verify_merkle_root",
        "hash",
        "public_key",
        "signature",
        "special_min",
        "target",
        "special_target",
        "header",
        "is_verified"
    )

    @classmethod
    async def init_async(
        cls,
        version=1,
        block_time=0,
        block_index=-1,
        prev_hash="",
        nonce: str = "",
        transactions=None,
        block_hash="",
        merkle_root="",
        public_key="",
        signature="",
        special_min: bool = False,
        header="",
        target: int = 0,
        special_target: int = 0,
    ):
        self = cls()
        self.config = Config()
        self.app_log = getLogger("tornado.application")
        self.version = version
        self.time = int(block_time)
        self.index = block_index
        self.prev_hash = prev_hash
        self.nonce = nonce
        # txn_hashes = self.get_transaction_hashes()
        # self.set_merkle_root(txn_hashes)
        self.merkle_root = merkle_root
        self.verify_merkle_root = ""
        self.hash = block_hash
        self.public_key = public_key
        self.signature = signature
        self.special_min = special_min
        self.target = target
        self.special_target = special_target
        if target == 0:
            # Same call as in new block check - but there's a circular reference here.
            latest_block = LatestBlock.block
            if not latest_block:
                self.target = CHAIN.MAX_TARGET
            else:
                if self.index >= CHAIN.FORK_10_MIN_BLOCK:
                    self.target = await CHAIN.get_target_10min(latest_block, self)
                else:
                    self.target = await CHAIN.get_target(self.index, latest_block, self)
            self.special_target = self.target
            # TODO: do we need recalc special target here if special min?
        self.header = header
        self.is_verified = False

        self.transactions = []
        for txn in transactions or []:
            transaction = Transaction.ensure_instance(txn)
            transaction.coinbase = Block.is_coinbase(self, transaction)
            self.transactions.append(transaction)

        return self

    async def copy(self):
        return await Block.from_json(self.to_json())

    @classmethod
    async def test_all_nodes(cls, min_balance):
        config = Config()
        block_index = config.LatestBlock.block.index
        nodes = Nodes.get_all_nodes_for_block_height(block_index)
        semaphore = asyncio.Semaphore(25)
        tasks = [test_node(node, semaphore, min_balance, config) for node in nodes]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        test_results = {
            "block_index": block_index,
            "timestamp": int(time.time()),
            "successful_nodes": [],
            "failed_nodes": []
        }

        successful_nodes = []

        for result in results:
            node_info = {
                "host": result["node"].host,
                "port": result["node"].port,
                "address": result.get("address"),
                "balance": result.get("balance"),
                "test_status": result.get("status"),
                "error": result.get("error")
            }

            if result["status"] == "success":
                test_results["successful_nodes"].append(node_info)
                successful_nodes.append(result["node"])
            else:
                test_results["failed_nodes"].append(node_info)

        await config.mongo.async_db.nodes_test_result.insert_one(test_results)

        return successful_nodes

    @classmethod
    async def generate(
        cls,
        transactions=None,
        public_key=None,
        private_key=None,
        force_version=None,
        index=0,
        force_time=None,
        prev_hash=None,
        nonce=None,
        target=0,
    ):
        config = Config()
        if force_version is None:
            version = CHAIN.get_version_for_height(index)
        else:
            version = force_version
        if force_time:
            xtime = int(force_time)
        else:
            xtime = int(time.time())
        index = int(index)
        if index == 0:
            prev_hash = ""
        elif prev_hash is None and index != 0:
            prev_hash = LatestBlock.block.hash
        transactions = transactions or []

        transaction_objs = []
        fee_sum = 0.0
        used_sigs = []
        used_inputs = {}
        regular_txns = []
        generated_txns = []
        for x in transactions:
            x = Transaction.ensure_instance(x)
            if await x.contract_generated:
                generated_txns.append(x)
            else:
                regular_txns.append(x)

        await Block.validate_transactions(
            regular_txns, transaction_objs, used_sigs, used_inputs, index, xtime
        )

        await Block.validate_transactions(
            generated_txns, transaction_objs, used_sigs, used_inputs, index, xtime
        )

        fee_sum = sum(
            [float(transaction_obj.fee) for transaction_obj in transaction_objs]
        )
        block_reward = CHAIN.get_block_reward(index)
        if index >= CHAIN.PAY_MASTER_NODES_FORK:
            nodes = Nodes.get_all_nodes_for_block_height(config.LatestBlock.block.index)

            outputs = [
                Output.from_dict(
                    {
                        "value": (block_reward * 0.9) + float(fee_sum),
                        "to": str(
                            P2PKHBitcoinAddress.from_pubkey(bytes.fromhex(public_key))
                        ),
                    }
                )
            ]
            
            masternode_reward_total = block_reward * 0.1

            if not Block.successful_nodes:
                Block.successful_nodes = await Block.test_all_nodes(min_balance=1000)
            config.app_log.info(f"Successful nodes after update: {Block.successful_nodes}")
            successful_nodes = Block.successful_nodes

            if not successful_nodes:
                config.app_log.warning("No successful nodes found, using full node list as fallback.")
                successful_nodes = nodes

            if successful_nodes:
                if index >= CHAIN.CHECK_MASTERNODE_FEE_FORK:
                    masternode_fee_sum = sum(
                        [
                            float(transaction_obj.masternode_fee)
                            for transaction_obj in transaction_objs
                        ]
                    )
                    masternode_reward_divided = (
                        masternode_reward_total + masternode_fee_sum
                    ) / len(successful_nodes)
                else:
                    masternode_reward_divided = masternode_reward_total / len(successful_nodes)

                for successful_node in successful_nodes:
                    outputs.append(
                        Output.from_dict(
                            {
                                "value": float(masternode_reward_divided),
                                "to": str(
                                    P2PKHBitcoinAddress.from_pubkey(
                                        bytes.fromhex(successful_node.identity.public_key)
                                    )
                                ),
                            }
                        )
                    )
        else:
            outputs = [
                Output.from_dict(
                    {
                        "value": block_reward + float(fee_sum),
                        "to": str(
                            P2PKHBitcoinAddress.from_pubkey(bytes.fromhex(public_key))
                        ),
                    }
                )
            ]

        coinbase_txn = await Transaction.generate(
            public_key=public_key,
            private_key=private_key,
            outputs=outputs,
            coinbase=True,
        )
        transaction_objs.append(coinbase_txn)

        block = await cls.init_async(
            version=version,
            block_time=xtime,
            block_index=index,
            prev_hash=prev_hash,
            transactions=transaction_objs,
            public_key=public_key,
            target=target,
        )
        txn_hashes = block.get_transaction_hashes()
        block.set_merkle_root(txn_hashes)
        block.header = block.generate_header()
        if nonce:
            block.nonce = str(nonce)
            block.hash = block.generate_hash_from_header(
                block.index, block.header, str(block.nonce)
            )
            block.signature = TU.generate_signature(block.hash, private_key)
        return block

    @staticmethod
    async def validate_transactions(
        txns, transaction_objs, used_sigs, used_inputs, index, xtime
    ):
        config = Config()
        for transaction_obj in txns:
            try:
                if transaction_obj.transaction_signature in used_sigs:
                    raise InvalidTransactionException(
                        "duplicate transaction found and removed"
                    )
                check_max_inputs = False
                if index > CHAIN.CHECK_MAX_INPUTS_FORK:
                    check_max_inputs = True

                check_masternode_fee = False
                if index >= CHAIN.CHECK_MASTERNODE_FEE_FORK:
                    check_masternode_fee = True

                await transaction_obj.verify(
                    check_max_inputs=check_max_inputs,
                    check_masternode_fee=check_masternode_fee,
                )
                for output in transaction_obj.outputs:
                    if not config.address_is_valid(output.to):
                        raise TransactionAddressInvalidException(
                            "Output address is invalid"
                        )
                used_sigs.append(transaction_obj.transaction_signature)
            except Exception as e:
                await Transaction.handle_exception(e, transaction_obj)
                continue
            try:
                if int(index) > CHAIN.CHECK_TIME_FROM and (
                    int(transaction_obj.time) > int(xtime) + CHAIN.TIME_TOLERANCE
                ):
                    await config.mongo.async_db.miner_transactions.delete_many(
                        {"id": transaction_obj.transaction_signature}
                    )
                    raise InvalidTransactionException(
                        "Block embeds txn too far in the future {} {}".format(
                            xtime, transaction_obj.time
                        )
                    )

                if transaction_obj.inputs:
                    failed = False
                    input_ids = []
                    for x in transaction_obj.inputs:
                        if (x.id, transaction_obj.public_key) in used_inputs:
                            failed = True
                        used_inputs[
                            (x.id, transaction_obj.public_key)
                        ] = transaction_obj
                        input_ids.append(x.id)
                    is_input_spent = await config.BU.is_input_spent(
                        input_ids, transaction_obj.public_key
                    )
                    if is_input_spent:
                        failed = True
                    if len(input_ids) != len(list(set(input_ids))):
                        failed = True
                    if failed:
                        raise InvalidTransactionException(
                            f"Transaction has inputs already spent: {transaction_obj.transaction_signature}"
                        )

            except Exception as e:
                await Transaction.handle_exception(e, transaction_obj)
                continue

            transaction_objs.append(transaction_obj)

    def generate_header(self):
        if int(self.version) < 3:
            return (
                str(self.version)
                + str(self.time)
                + self.public_key
                + str(self.index)
                + self.prev_hash
                + "{nonce}"
                + str(self.special_min)
                + str(self.target)
                + self.merkle_root
            )
        else:
            # version 3 block do not contain special_min anymore and have target as 64 hex string
            # print("target", block.target)
            # TODO: somewhere, target is calc with a / and result is float instead of int.
            return (
                str(self.version)
                + str(self.time)
                + self.public_key
                + str(self.index)
                + self.prev_hash
                + "{nonce}"
                + hex(int(self.target))[2:].rjust(64, "0")
                + self.merkle_root
            )

    def set_merkle_root(self, txn_hashes):
        self.merkle_root = self.get_merkle_root(txn_hashes)

    def get_merkle_root(self, txn_hashes):
        hashes = []
        for i in range(0, len(txn_hashes), 2):
            txn1 = txn_hashes[i]
            try:
                txn2 = txn_hashes[i + 1]
            except:
                txn2 = ""
            hashes.append(hashlib.sha256((txn1 + txn2).encode("utf-8")).digest().hex())
        if len(hashes) > 1:
            return self.get_merkle_root(hashes)
        else:
            return hashes[0]

    @classmethod
    async def from_dict(cls, block):
        if isinstance(block, Block):
            return block
        if block.get("special_target", 0) == 0:
            block["special_target"] = block.get("target")

        return await cls.init_async(
            version=block.get("version"),
            block_time=block.get("time"),
            block_index=block.get("index"),
            public_key=block.get("public_key"),
            prev_hash=block.get("prevHash"),
            nonce=block.get("nonce"),
            block_hash=block.get("hash"),
            transactions=block.get("transactions"),
            merkle_root=block.get("merkleRoot"),
            signature=block.get("id"),
            special_min=block.get("special_min"),
            header=block.get("header", ""),
            target=int(block.get("target"), 16),
            special_target=int(block.get("special_target", 0), 16),
        )

    @classmethod
    async def from_json(cls, block_json):
        return await cls.from_dict(json.loads(block_json))

    def get_coinbase(self):
        for txn in self.transactions:
            if Block.is_coinbase(self, txn):
                return txn

    @staticmethod
    def is_coinbase(block, txn):
        return (
            block.public_key == txn.public_key
            and str(P2PKHBitcoinAddress.from_pubkey(bytes.fromhex(block.public_key)))
            in [x.to for x in txn.outputs]
            and len(txn.inputs) == 0
        )

    def generate_hash_from_header(self, height, header, nonce):
        if not hasattr(Block, "pyrx"):
            Block.pyrx = pyrx.PyRX()
        seed_hash = binascii.unhexlify(
            "4181a493b397a733b083639334bc32b407915b9a82b7917ac361816f0a1f5d4d"
        )  # sha256(yadacoin65000)
        if height >= CHAIN.BLOCK_V5_FORK:
            bh = Block.pyrx.get_rx_hash(
                header.encode().replace(b"{nonce}", binascii.unhexlify(nonce)),
                seed_hash,
                height,
            )
            hh = binascii.hexlify(bh).decode()
            return hh
        elif height >= CHAIN.RANDOMX_FORK:
            header = header.format(nonce=nonce)
            bh = Block.pyrx.get_rx_hash(header, seed_hash, height)
            hh = binascii.hexlify(bh).decode()
            return hh
        else:
            header = header.format(nonce=nonce)
            return (
                hashlib.sha256(hashlib.sha256(header.encode("utf-8")).digest())
                .digest()[::-1]
                .hex()
            )

    async def verify(self):
        getcontext().prec = 8
        if int(self.version) != int(CHAIN.get_version_for_height(self.index)):
            raise Exception(
                "Wrong version for block height",
                self.version,
                CHAIN.get_version_for_height(self.index),
            )

        txns = self.get_transaction_hashes()
        verify_merkle_root = self.get_merkle_root(txns)
        if verify_merkle_root != self.merkle_root:
            raise Exception("Invalid block merkle root")

        header = self.generate_header()
        hashtest = self.generate_hash_from_header(self.index, header, str(self.nonce))
        if self.hash != hashtest:
            getLogger("tornado.application").warning(
                "Verify error hashtest {} header {} nonce {}".format(
                    hashtest, header, self.nonce
                )
            )
            raise Exception("Invalid block hash")

        address = str(P2PKHBitcoinAddress.from_pubkey(bytes.fromhex(self.public_key)))
        try:
            result = verify_signature(
                base64.b64decode(self.signature),
                self.hash.encode("utf-8"),
                bytes.fromhex(self.public_key),
            )
            if not result:
                raise Exception("block signature1 is invalid")
        except:
            try:
                result = VerifyMessage(
                    address,
                    BitcoinMessage(self.hash.encode("utf-8"), magic=""),
                    self.signature,
                )
                if not result:
                    raise
            except:
                raise Exception("block signature2 is invalid")

        if self.index >= CHAIN.PAY_MASTER_NODES_FORK:
            masernodes_by_address = (
                Nodes.get_all_nodes_indexed_by_address_for_block_height(self.index)
            )

        # verify reward
        coinbase_sum = 0
        fee_sum = 0.0
        masternode_fee_sum = 0.0
        masternode_sums = {}
        for txn in self.transactions:
            if int(self.index) >= CHAIN.TXN_V3_FORK and int(txn.version) < 3:
                raise Exception(
                    "block contains transaction with version too old for this height"
                )

            if int(self.index) > CHAIN.CHECK_TIME_FROM and (
                int(txn.time) > int(self.time) + CHAIN.TIME_TOLERANCE
            ):
                # await self.config.mongo.async_db.miner_transactions.delete_many({'id': txn.transaction_signature})
                # raise Exception("Block embeds txn too far in the future")
                pass

            if txn.coinbase:
                if self.index >= CHAIN.PAY_MASTER_NODES_FORK:
                    block_creator_address = str(
                        P2PKHBitcoinAddress.from_pubkey(bytes.fromhex(self.public_key))
                    )
                    for output in txn.outputs:
                        if output.to == block_creator_address:
                            coinbase_sum += float(output.value)
                        elif output.to in masernodes_by_address:
                            if output.to not in masternode_sums:
                                masternode_sums[output.to] = 0
                            masternode_sums[output.to] += output.value
                        else:
                            raise UnknownOutputAddressException()
                else:
                    for output in txn.outputs:
                        coinbase_sum += float(output.value)
            elif await txn.contract_generated:
                if self.index >= CHAIN.TXN_V3_FORK_CHECK_MINER_SIGNATURE:
                    result = verify_signature(
                        base64.b64decode(txn.miner_signature),
                        hashlib.sha256(txn.transaction_signature.encode())
                        .hexdigest()
                        .encode(),
                        bytes.fromhex(self.public_key),
                    )
                    if not result:
                        raise Exception("block signature1 is invalid")
                    contract_txn = await txn.get_generating_contract()
                    await contract_txn.relationship.verify_generation(
                        self,
                        txn,
                        [
                            x
                            for x in self.transactions
                            if x.transaction_signature != txn.transaction_signature
                        ],
                    )
                fee_sum += float(txn.fee)
                if self.index >= CHAIN.CHECK_MASTERNODE_FEE_FORK:
                    masternode_fee_sum += float(txn.masternode_fee)
            else:
                fee_sum += float(txn.fee)
                if self.index >= CHAIN.CHECK_MASTERNODE_FEE_FORK:
                    masternode_fee_sum += float(txn.masternode_fee)

        reward = CHAIN.get_block_reward(self.index)

        # if Decimal(str(fee_sum)[:10]) != Decimal(str(coinbase_sum)[:10]) - Decimal(str(reward)[:10]):
        """
        KO for block 13949
        0.02099999 50.021 50.0
        Integrate block error 1 ('Coinbase output total does not equal block reward + transaction fees', 0.020999999999999998, 0.021000000000000796)
        """

        if self.index >= CHAIN.CHECK_MASTERNODE_FEE_FORK:
            masternode_sum = sum(x for x in masternode_sums.values())
            if quantize_eight(fee_sum + masternode_fee_sum) != quantize_eight(
                (coinbase_sum + masternode_sum) - reward
            ):
                raise TotalValueMismatchException(
                    "Masternode output totals do not equal block reward + masternode transaction fees",
                    float(quantize_eight(fee_sum + masternode_fee_sum)),
                    float(quantize_eight((coinbase_sum + masternode_sum) - reward)),
                )

        elif self.index >= CHAIN.PAY_MASTER_NODES_FORK:
            masternode_sum = sum(x for x in masternode_sums.values())
            if quantize_eight(fee_sum) != quantize_eight(
                (coinbase_sum + masternode_sum) - reward
            ):
                raise TotalValueMismatchException(
                    "Coinbase output total does not equal block reward + transaction fees",
                    fee_sum,
                    (coinbase_sum - reward),
                )

        else:
            if quantize_eight(fee_sum) != quantize_eight(coinbase_sum - reward):
                raise TotalValueMismatchException(
                    "Coinbase output total does not equal block reward + transaction fees",
                    fee_sum,
                    (coinbase_sum - reward),
                )

        self.is_verified = True

    def get_transaction_hashes(self):
        """Returns a sorted list of tx hash, so the merkle root is constant across nodes"""
        return sorted([str(x.hash) for x in self.transactions], key=str.lower)

    async def save(self):
        await self.verify()
        for txn in self.transactions:
            if txn.inputs:
                failed = False
                used_ids_in_this_txn = []
                for x in txn.inputs:
                    is_input_spent = (
                        await yadacoin.core.config.CONFIG.BU.is_input_spent(
                            x.id, txn.public_key
                        )
                    )
                    if is_input_spent:
                        failed = True
                    if x.id in used_ids_in_this_txn:
                        failed = True
                    used_ids_in_this_txn.append(x.id)
                if failed:
                    raise Exception("double spend", [x.id for x in txn.inputs])
        res = await self.config.mongo.async_db.blocks.find_one(
            {"index": (int(self.index) - 1)}
        )
        if (res and res["hash"] == self.prev_hash) or self.index == 0:
            await self.config.mongo.async_db.blocks.replace_one(
                {"index": self.index}, self.to_dict(), upsert=True
            )
        else:
            print("CRITICAL: block rejected...")

    def to_dict(self):
        try:
            return {
                "version": self.version,
                "time": int(self.time),
                "index": self.index,
                "public_key": self.public_key,
                "prevHash": self.prev_hash,
                "nonce": self.nonce,
                "transactions": [x.to_dict() for x in self.transactions],
                "hash": self.hash,
                "merkleRoot": self.merkle_root,
                "special_min": self.special_min,
                "target": hex(self.target)[2:].rjust(64, "0"),
                "special_target": hex(self.special_target)[2:].rjust(64, "0"),
                "header": self.header,
                "id": self.signature,
            }
        except Exception as e:
            print(e)
            print("target", self.target, "spec", self.special_target)

    def to_json(self):
        return json.dumps(self.to_dict(), indent=4)

    def in_the_future(self):
        """Tells wether the block is too far away in the future"""
        return int(self.time) > time.time() + CHAIN.TIME_TOLERANCE
