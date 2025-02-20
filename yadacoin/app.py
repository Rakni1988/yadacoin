﻿"""
YadaCoin Open Source License (YOSL) v1.1

Copyright (c) 2017-2025 Matthew Vogel, Reynold Vogel, Inc.

This software is licensed under YOSL v1.1 – for personal and research use only.
NO commercial use, NO blockchain forks, and NO branding use without permission.

For commercial license inquiries, contact: info@yadacoin.io

Full license terms: see LICENSE.txt in this repository.
"""

import asyncio
import importlib
import json
import logging
import ntpath
import os
import pkgutil
import ssl
import sys

currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
try:
    import resource

    resource.setrlimit(resource.RLIMIT_NOFILE, (131072, 131072))
except:
    pass
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from hashlib import sha256
from logging.handlers import RotatingFileHandler
from os import path
from time import time
from traceback import format_exc

import tornado.ioloop
import tornado.locks
import tornado.log
from Crypto.PublicKey.ECC import EccKey
from tornado.httpclient import AsyncHTTPClient
from tornado.httpserver import HTTPServer
from tornado.ioloop import PeriodicCallback
from tornado.options import define, options
from tornado.web import Application, StaticFileHandler

import yadacoin.core.blockchainutils
import yadacoin.core.config
import yadacoin.core.transactionutils
from plugins.yadacoinpool import handlers
from yadacoin import version
from yadacoin.core.block import Block
from yadacoin.core.blockchain import Blockchain
from yadacoin.core.chain import CHAIN
from yadacoin.core.consensus import Consensus
from yadacoin.core.crypt import Crypt
from yadacoin.core.graphutils import GraphUtils
from yadacoin.core.health import Health
from yadacoin.core.latestblock import LatestBlock
from yadacoin.core.miningpool import MiningPool
from yadacoin.core.miningpoolpayout import PoolPayer
from yadacoin.core.mongo import Mongo
from yadacoin.core.peer import (
    Group,
    Miner,
    Peer,
    Peers,
    Pool,
    Seed,
    SeedGateway,
    ServiceProvider,
    User,
)
from yadacoin.core.processingqueue import BlockProcessingQueueItem, ProcessingQueues
from yadacoin.core.smtp import Email
from yadacoin.core.transaction import Transaction
from yadacoin.enums.modes import MODES
from yadacoin.enums.peertypes import PEER_TYPES
from yadacoin.http.explorer import EXPLORER_HANDLERS
from yadacoin.http.graph import GRAPH_HANDLERS
from yadacoin.http.keyeventlog import KEY_EVENT_LOG_HANDLERS
from yadacoin.http.node import NODE_HANDLERS
from yadacoin.http.pool import POOL_HANDLERS
from yadacoin.http.product import PRODUCT_HANDLERS
from yadacoin.http.wallet import WALLET_HANDLERS
from yadacoin.http.web import WEB_HANDLERS
from yadacoin.http.dashboard import DASHBOARD_HANDLERS
from yadacoin.managers.docker import Docker
from yadacoin.tcpsocket.node import NodeRPC, NodeSocketClient, NodeSocketServer
from yadacoin.tcpsocket.pool import StratumServer
from yadacoin.websocket.base import WEBSOCKET_HANDLERS, RCPWebSocketServer
from yadacoin.websocket.peerjs import PEERJS_HANDLERS

define("debug", default=False, help="debug mode", type=bool)
define("verbose", default=False, help="verbose mode", type=bool)
define("network", default="", help="Force mainnet, testnet or regnet", type=str)
define(
    "reset",
    default=False,
    help="If blockchain is invalid, truncate at error block",
    type=bool,
)
define(
    "config",
    default="config/config.json",
    help="Config file location, default is 'config/config.json'",
    type=str,
)
define("verify", default=False, help="Verify chain, default False", type=bool)
define("server", default=False, help="Is server for testing", type=bool)
define("client", default=False, help="Is client for testing", type=bool)
define(
    "mongohost",
    default="",
    help="Value to override mongodb_host config value",
    type=str,
)
define(
    "modes",
    default=[],
    type=str,
    multiple=True,
    help="Operation modes. node, web, pool",
)
define("ignore", default=False, help="Added for testing", type=str)


class WorkerVars:
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


from tornado.ioloop import IOLoop
from tornado.platform.asyncio import AsyncIOMainLoop


class NodeApplication(Application):
    def __init__(self, test=False):
        options.parse_command_line(final=False)
        self.init_config(options)
        self.configure_logging()
        self.init_config_properties(test=test)
        self.config.lock = asyncio.Lock()
        if test:
            return
        if MODES.NODE.value in self.config.modes:
            self.init_seeds()
            self.init_seed_gateways()
            self.init_service_providers()
            self.init_groups()
            try:
                self.config.app_log.info(
                    "Node: {}:{}".format(self.config.peer_host, self.config.peer_port)
                )
            except Exception as e:
                self.config.app_log.info("{}, starting without node enabled.".format(e))
            self.config.node_server_instance = self.config.nodeServer()
            self.config.node_server_instance.bind(self.config.peer_port)
            self.config.node_server_instance.start(1)

        self.init_peer()
        if MODES.POOL.value in self.config.modes:
            self.init_pool()

        if MODES.WEB.value in self.config.modes:
            if os.path.exists(path.join(path.dirname(__file__), "..", "static")):
                static_path = path.join(path.dirname(__file__), "..", "static")
            else:
                static_path = path.join(
                    path.dirname(__file__), "static"
                )  # probably running from binary

            if os.path.exists(
                path.join(path.join(path.dirname(__file__), "..", "static"), "app")
            ):
                static_app_path = path.join(
                    path.join(path.dirname(__file__), "..", "static"), "app"
                )
            else:
                static_app_path = path.join(
                    path.join(path.dirname(__file__), "static"), "app"
                )  # probably running from binary

            self.default_handlers = [
                (r"/app/(.*)", StaticFileHandler, {"path": static_app_path}),
                (r"/yadacoinstatic/(.*)", StaticFileHandler, {"path": static_path}),
            ]
            self.default_handlers.extend(handlers.HANDLERS)
            self.init_websocket()
            self.init_webui()
            self.init_plugins()
            self.init_http()
            self.init_whitelist()
            self.init_jwt()
        self.init_ioloop()

    async def remove_peer(self, peer, reason=None, send_disconnect=True):
        try:
            if isinstance(peer, str):
                self.config.app_log.info(f"🔍 Searching for peer {peer} in nodeServer and nodeClient.")

                peer_stream = None

                for peer_cls in self.config.nodeServer.inbound_streams.keys():
                    if peer in self.config.nodeServer.inbound_streams[peer_cls]:
                        peer_stream = self.config.nodeServer.inbound_streams[peer_cls][peer]
                        break

                if not peer_stream:
                    for peer_cls in self.config.nodeClient.outbound_streams.keys():
                        if peer in self.config.nodeClient.outbound_streams[peer_cls]:
                            peer_stream = self.config.nodeClient.outbound_streams[peer_cls][peer]
                            break

                if not peer_stream:
                    self.config.app_log.warning(f"⚠️ Peer {peer} not found in streams. Skipping removal.")
                    return

                peer = peer_stream

            if reason and send_disconnect:
                try:
                    await asyncio.wait_for(
                        self.config.nodeShared.write_params(peer, "disconnect", {"reason": reason}),
                        timeout=3,
                    )
                except (asyncio.TimeoutError, Exception) as e:
                    self.config.app_log.warning(f"Failed to send disconnect to peer {getattr(peer.peer, 'rid', 'Unknown')}: {e}")

            peer.close()

            if not hasattr(peer, "peer"):
                return

            id_attr = getattr(peer.peer, peer.peer.id_attribute)

            if id_attr in self.config.nodeServer.inbound_streams[peer.peer.__class__.__name__]:
                del self.config.nodeServer.inbound_streams[peer.peer.__class__.__name__][id_attr]

            if id_attr in self.config.nodeServer.inbound_pending[peer.peer.__class__.__name__]:
                del self.config.nodeServer.inbound_pending[peer.peer.__class__.__name__][id_attr]

            if id_attr in self.config.nodeClient.outbound_streams[peer.peer.__class__.__name__]:
                del self.config.nodeClient.outbound_streams[peer.peer.__class__.__name__][id_attr]
                self.config.nodeClient.outbound_ignore[peer.peer.__class__.__name__][peer.peer.identity.username_signature] = int(time())

            if id_attr in self.config.nodeClient.outbound_pending[peer.peer.__class__.__name__]:
                del self.config.nodeClient.outbound_pending[peer.peer.__class__.__name__][id_attr]
                self.config.nodeClient.outbound_ignore[peer.peer.__class__.__name__][peer.peer.identity.username_signature] = int(time())

            self.config.app_log.info(f"✅ Peer {id_attr} removed successfully.")

        except Exception as e:
            self.config.app_log.error(f"❌ Error while removing peer: {e}")


    def delete_retry_messages(self, rid):
        try:
            removed_from_node_server = len([key for key in self.config.nodeServer.retry_messages if key[0] == rid])
            removed_from_node_client = len([key for key in self.config.nodeClient.retry_messages if key[0] == rid])
            removed_from_counters = len([key for key in self.retry_counters if key[0] == rid])

            self.config.nodeServer.retry_messages = {k: v for k, v in self.config.nodeServer.retry_messages.items() if k[0] != rid}
            self.config.nodeClient.retry_messages = {k: v for k, v in self.config.nodeClient.retry_messages.items() if k[0] != rid}
            self.retry_counters = {k: v for k, v in self.retry_counters.items() if k[0] != rid}

            self.config.app_log.info(
                f"✅ Deleted {removed_from_node_server} messages from nodeServer.retry_messages, "
                f"{removed_from_node_client} from nodeClient.retry_messages, and "
                f"{removed_from_counters} from retry_counters for peer {rid}."
            )
        except Exception as e:
            self.config.app_log.error(f"❌ Error deleting retry_messages for peer {rid}: {e}")

    async def background_peers(self):
        """Peers management coroutine. responsible for peers testing and outgoing connections"""

        self.config.app_log.debug("background_peers")
        if not hasattr(self.config, "background_peers"):
            self.config.background_peers = WorkerVars(busy=False)
        if self.config.background_peers.busy:
            self.config.app_log.debug("background_peers - busy")
            return
        self.config.background_peers.busy = True
        try:
            await self.config.peer.ensure_peers_connected()
            self.config.health.peer.last_activity = int(time())
        except:
            self.config.app_log.error(format_exc())
        self.config.background_peers.busy = False

    async def background_status(self):
        """This background co-routine is responsible for status collection and display"""
        self.config.app_log.debug("background_status")
        if not hasattr(self.config, "background_status"):
            self.config.background_status = WorkerVars(busy=False, last_summary=None)
            await self.config.mongo.async_db.node_status.delete_many({"archived": True})
            await self.config.mongo.async_db.node_status.update_many(
                {}, {"$set": {"archived": True}}
            )
            if (
                Docker.is_inside_docker()
                and hasattr(self.config, "docker_debug")
                and self.config.docker_debug
            ):
                self.config.background_status.docker = Docker()
        if self.config.background_status.busy:
            self.config.app_log.debug("background_status - busy")
            return
        self.config.background_status.busy = True
        try:
            status = await self.config.get_status()
            status["synced"] = await Peer.is_synced()
            status["timestamp"] = int(time())
            status["processing_queues"] = self.config.processing_queues.to_status_dict()
            await self.config.health.check_health()
            status["health"] = self.config.health.to_dict()
            status["message_sender"] = {
                "nodeServer": {
                    "num_messages": len(list(self.config.nodeServer.retry_messages))
                },
                "nodeClient": {
                    "num_messages": len(list(self.config.nodeClient.retry_messages))
                },
            }
            status["slow_queries"] = {
                "count": len(self.config.mongo.async_db.slow_queries),
                "detail": self.config.mongo.async_db.slow_queries,
            }
            status["transaction_tracker"] = {
                "nodeServer": self.config.nodeServer.newtxn_tracker.to_dict(),
                "nodeClient": self.config.nodeClient.newtxn_tracker.to_dict(),
            }
            status["disconnect_tracker"] = {
                "nodeServer": self.config.nodeServer.disconnect_tracker.to_dict(),
                "nodeClient": self.config.nodeClient.disconnect_tracker.to_dict(),
            }
            if (
                Docker.is_inside_docker()
                and hasattr(self.config, "docker_debug")
                and self.config.docker_debug
            ):
                self.config.background_status.docker.set_container_stats()
                status["docker"] = self.config.background_status.docker.to_dict()

            if hasattr(self.config, "debug_memory") and self.config.debug_memory:
                from pympler import muppy, summary

                all_objects = muppy.get_objects()
                summarized = summary.summarize(all_objects)
                summary.print_(summarized)
                dict_list = []
                for item in summarized:
                    type_str, num_objects, total_size = item
                    dict_list.append(
                        {
                            "type": str(type_str),  # Convert type to string
                            "num_objects": num_objects,
                            "total_size": total_size,
                        }
                    )
                if self.config.background_status.last_summary:
                    diff = summary.get_diff(
                        self.config.background_status.last_summary, summarized
                    )
                    summary.print_(diff)
                    diff_dict_list = []
                    for item in diff:
                        type_str, num_diff, size_diff = item
                        if num_diff or size_diff:
                            diff_dict_list.append(
                                {
                                    "type": str(type_str),  # Convert type to string
                                    "num_diff": num_diff,
                                    "size_diff": size_diff,
                                }
                            )
                    status["memory"] = {"diff": diff_dict_list}
                self.config.background_status.last_summary = summarized

            if (
                hasattr(self.config, "log_health_status")
                and self.config.log_health_status
            ):
                if status["health"]["status"]:
                    self.config.app_log.info(json.dumps(status, indent=4))
                else:
                    self.config.app_log.warning(json.dumps(status, indent=4))
            await self.config.mongo.async_db.node_status.delete_many(
                {"timestamp": {"$lt": status["timestamp"] - (60 * 60 * 24)}}
            )
            await self.config.mongo.async_db.node_status.insert_one(status)
        except Exception:
            self.config.app_log.error(format_exc())
        self.config.background_status.busy = False

    async def background_block_checker(self):
        """
        Responsible for propagating new blocks in the network.

        This coroutine ensures that newly discovered blocks are efficiently distributed to peers.
        It continuously monitors the latest block and triggers propagation only when a change is detected.

        If a new block is found (i.e., its height or hash has changed), it is immediately broadcasted to peers.
        Additionally, if no new block has been found for a defined period,
        it will resend the latest known block to maintain synchronization across nodes (default: 300s).

        This mechanism prevents redundant broadcasts while ensuring all peers
        stay up-to-date with the latest blockchain state.
        """
        self.config.app_log.debug("background_block_checker")
        if not hasattr(self.config, "background_block_checker"):
            self.config.background_block_checker = WorkerVars(
                busy=False, last_send=0, last_block_height=0, last_block_hash=None
            )
        if self.config.background_block_checker.busy:
            self.config.app_log.debug("background_block_checker - busy")
            return
        self.config.background_block_checker.busy = True

        try:
            current_block = LatestBlock.block
            last_block_height = self.config.background_block_checker.last_block_height
            last_block_hash = self.config.background_block_checker.last_block_hash

            if current_block:
                self.config.app_log.debug(
                    f"Current block: Height={current_block.index}, Hash={current_block.hash}"
                )

                # Check if the block height or hash has changed
                if (
                    current_block.index != last_block_height
                    or current_block.hash != last_block_hash
                ):
                    self.config.background_block_checker.last_block_height = (
                        current_block.index
                    )
                    self.config.background_block_checker.last_block_hash = (
                        current_block.hash
                    )

                    self.config.app_log.info(
                        f"New block detected: Height={current_block.index}, "
                        f"Hash={current_block.hash}, Time={datetime.fromtimestamp(current_block.time).strftime('%Y-%m-%d %H:%M:%S')}"
                    )

                    # Propagate the new block to peers
                    await self.config.nodeShared.send_block_to_peers(current_block)
                    self.config.background_block_checker.last_send = int(time())
                    self.config.app_log.info(
                        f"Block {current_block.index} successfully propagated to peers."
                    )
                elif int(time()) - self.config.background_block_checker.last_send > 300:
                    # Regular propagation if no new block but timeout exceeded
                    self.config.app_log.info(
                        "Regular propagation triggered after timeout."
                    )
                    self.config.background_block_checker.last_send = int(time())
                    await self.config.nodeShared.send_block_to_peers(current_block)
                else:
                    self.config.app_log.debug(
                        f"No propagation needed. Last send: {self.config.background_block_checker.last_send}, "
                        f"Time since last send: {int(time()) - self.config.background_block_checker.last_send}s"
                    )
            else:
                self.config.app_log.debug("No current block available to propagate.")

        except Exception:
            self.config.app_log.error(format_exc())
        finally:
            self.config.health.block_checker.last_activity = int(time())
            self.config.background_block_checker.busy = False

    async def background_newblock_stress_test(self):
        # only tests outbound currently
        for peer_cls in list(self.config.nodeClient.outbound_streams.keys()).copy():
            for rid in self.config.nodeClient.outbound_streams[peer_cls]:
                for x in range(50):
                    txn = await Transaction.generate(
                        private_key=self.config.private_key,
                        public_key=self.config.public_key,
                    )
                    block = await Block.generate(
                        private_key=self.config.private_key,
                        public_key=self.config.public_key,
                        transactions=[txn],
                        index=600000,
                    )
                    # self.config.nodeClient.retry_messages[
                    #     (rid, "newtxn", txn.transaction_signature)
                    # ] = {
                    #     "transaction": txn.to_dict(),
                    #     "test": True,
                    # }
                    self.config.processing_queues.block_queue.add(
                        BlockProcessingQueueItem(
                            Blockchain(block),
                            self.config.nodeClient.outbound_streams[peer_cls][rid],
                        )
                    )

    async def background_message_sender(self):
        self.config.app_log.debug("background_message_sender")

        if not hasattr(self, "retry_counters"):
            self.retry_counters = {}

        if not hasattr(self.config, "background_message_sender"):
            self.config.background_message_sender = WorkerVars(busy=False)

        if self.config.background_message_sender.busy:
            return

        self.config.background_message_sender.busy = True

        try:
            async def process_retry_messages(retry_messages, stream_dict, stream_type):
                for x in list(retry_messages.keys()):
                    if x not in retry_messages:
                        self.config.app_log.warning(f"🚨 Skipping retry for {x}, it was already removed.")
                        continue

                    message = retry_messages.get(x)
                    if not message:
                        retry_messages.pop(x, None)
                        self.config.app_log.debug(f"{stream_type} - Removed empty retry message {x}.")
                        continue

                    peer_id = x[0]  # Peer ID (RID)
                    request_type = x[1]

                    self.config.app_log.info(f"🔎 Checking if peer {peer_id} exists in {stream_type} stream_dict.")
                    peer_found = any(peer_id in stream_dict.get(cls, {}) for cls in stream_dict.keys())

                    if not peer_found:
                        self.config.app_log.warning(f"🚨 Peer {peer_id} not found in {stream_type}, removing related messages.")
                        self.delete_retry_messages(peer_id)
                        continue

                    message_id = None

                    if request_type == ["blockresponse", "blocksresponse"]:
                        message_id = x[3]
                    else:
                        message_id = x[2]


                    if not message_id:
                        self.config.app_log.warning(f"❌ Skipping retry, missing identifier for {x}")
                        continue
                    else:
                        self.config.app_log.info(f"✅ Identified message_id for retry: {message_id} (Type: {request_type})")

                    key = (peer_id, request_type, message_id)
                    self.retry_counters[key] = self.retry_counters.get(key, 0) + 1

                    self.config.app_log.info(f"🔄 Retrying message {key} for {stream_type}, attempt {self.retry_counters[key]}.")

                    retry_message_id = x[3] if len(x) > 3 else None

                    if retry_message_id and retry_message_id not in stream_dict.get(request_type, {}):
                        if peer_id in stream_dict.get(request_type, {}):
                            self.config.app_log.warning(f"⚠️ Message {retry_message_id} not in queue, but peer {peer_id} exists. Retrying instead of deleting.")
                            continue
                        else:
                            if request_type in ["blockresponse", "blocksresponse"] and message.get("result", {}).get("block") is None:
                                self.config.app_log.info(f"✅ Received valid empty {request_type} from {peer_id}, skipping deletion.")
                                continue

                            self.config.app_log.warning(f"❌ Peer {peer_id} is gone. Removing retry_messages.")
                            self.delete_retry_messages(peer_id)
                            continue

                    if self.retry_counters[key] > 3:
                        await self.remove_peer(x[0], reason="Exceeded retry limit", send_disconnect=False)
                        self.delete_retry_messages(x[0])
                        self.config.app_log.warning(f"⚠️ Message {key} exceeded retry limit. Removing peer {x[0]}.")
                        continue

                    for peer_cls in list(stream_dict.keys()).copy():
                        if peer_id in stream_dict[peer_cls]:
                            peer_stream = stream_dict[peer_cls][peer_id]

                            if peer_stream.closed():
                                self.config.app_log.warning(f"🚨 Peer {peer_id} stream is closed. Skipping message {x[3]}.")
                                self.delete_retry_messages(peer_id)
                                continue

                            try:
                                if len(x) > 3:
                                    await self.config.nodeShared.write_result(peer_stream, x[1], message, x[3])
                                else:
                                    await self.config.nodeShared.write_params(peer_stream, x[1], message)
                            except asyncio.CancelledError:
                                self.config.app_log.warning(f"⏳ Write operation cancelled for message {x[3]} to {peer_id}. Peer might be disconnecting.")
                            except Exception as e:
                                self.config.app_log.error(f"💥 Error writing to peer {peer_id}: {e}")

            await process_retry_messages(self.config.nodeServer.retry_messages, self.config.nodeServer.inbound_streams, "nodeServer")
            await process_retry_messages(self.config.nodeClient.retry_messages, self.config.nodeClient.outbound_streams, "nodeClient")

            self.config.health.message_sender.last_activity = int(time())

        except Exception as e:
            self.config.app_log.error(f"❌ Error in background_message_sender: {e}")

        finally:
            self.config.background_message_sender.busy = False

    async def background_txn_queue_processor(self):
        self.config.app_log.debug("background_txn_queue_processor")
        if not hasattr(self.config, "background_txn_queue_processor"):
            self.config.background_txn_queue_processor = WorkerVars(busy=False)
        if self.config.background_txn_queue_processor.busy:
            self.config.app_log.debug("background_txn_queue_processor - busy")
            return
        self.config.background_txn_queue_processor.busy = True
        try:
            if self.config.processing_queues.transaction_queue.queue:
                self.config.processing_queues.transaction_queue.time_sum_start()
                await self.config.node_server_instance.process_transaction_queue()
                self.config.processing_queues.transaction_queue.time_sum_end()
            self.config.health.transaction_processor.last_activity = int(time())
        except:
            self.config.app_log.error(format_exc())
            self.config.processing_queues.transaction_queue.time_sum_end()
        self.config.background_txn_queue_processor.busy = False

    async def background_block_queue_processor(self):
        self.config.app_log.debug("background_block_queue_processor")
        if not hasattr(self.config, "background_block_queue_processor"):
            self.config.background_block_queue_processor = WorkerVars(busy=False)
        if self.config.background_block_queue_processor.busy:
            self.config.app_log.debug("background_block_queue_processor - busy")
            return
        self.config.background_block_queue_processor.busy = True
        while True:
            try:
                synced = await Peer.is_synced()
                skip = False
                if self.config.processing_queues.block_queue.queue:
                    if (
                        time() - self.config.health.consensus.last_activity
                        < CHAIN.FORCE_CONSENSUS_TIME_THRESHOLD
                    ):
                        skip = True
                if not skip or not synced:
                    await self.config.consensus.sync_bottom_up(synced)
                    self.config.health.consensus.last_activity = time()
            except Exception:
                self.config.app_log.error(format_exc())

            try:
                if self.config.processing_queues.block_queue.queue:
                    self.config.processing_queues.block_queue.time_sum_start()
                    await self.config.consensus.process_block_queue()
                    self.config.processing_queues.block_queue.time_sum_end()
                self.config.health.block_inserter.last_activity = int(time())
            except:
                self.config.app_log.error(format_exc())
                self.config.processing_queues.block_queue.time_sum_end()

            synced = await Peer.is_synced()
            if not synced and self.config.processing_queues.block_queue.queue:
                continue
            break
        self.config.background_block_queue_processor.busy = False

    async def background_pool_payer(self):
        """Responsible for paying miners"""
        """
        New blocks will directly trigger the correct event.
        This co-routine checks if new transactions have been received, or if special_min is triggered,
        So we can update the miners.
        """
        self.config.app_log.debug("background_pool_payer")
        if not hasattr(self.config, "background_pool_payer"):
            self.config.background_pool_payer = WorkerVars(busy=False)
        if self.config.background_pool_payer.busy:
            self.config.app_log.debug("background_pool_payer - busy")
            return
        self.config.background_pool_payer.busy = True
        try:
            if self.config.pp:
                await self.config.pp.do_payout()

            self.config.health.pool_payer.last_activity = int(time())
        except Exception:
            self.config.app_log.error(format_exc())
        self.config.background_pool_payer.busy = False

    async def background_cache_validator(self):
        """Responsible for validating the cache and clearing it when necessary"""

        self.config.app_log.debug("background_cache_validator")
        if not hasattr(self.config, "background_cache_validator"):
            self.config.background_cache_validator = WorkerVars(busy=False)
        if self.config.background_cache_validator.busy:
            self.config.app_log.debug("background_cache_validator - busy")
            return
        self.config.background_cache_validator.busy = True
        if not hasattr(self.config, "cache_inited"):
            self.cache_collections = [
                x
                for x in await self.config.mongo.async_db.list_collection_names()
                if x.endswith("_cache")
            ]
            self.cache_last_times = {}
            try:
                async for x in self.config.mongo.async_db.blocks.find(
                    {"updated_at": {"$exists": False}}
                ):
                    await self.config.mongo.async_db.blocks.update_one(
                        {"index": x["index"]}, {"$set": {"updated_at": time()}}
                    )
                for cache_collection in self.cache_collections:
                    self.cache_last_times[cache_collection] = 0
                    await self.config.mongo.async_db[cache_collection].delete_many(
                        {"cache_time": {"$exists": False}}
                    )
                self.config.cache_inited = True
            except Exception:
                self.config.app_log.error(format_exc())
        self.config.background_cache_validator.busy = False

        """
        We check for cache items that are not currently in the blockchain
        If not, we delete the cached item.
        """
        try:
            for cache_collection in self.cache_collections:
                if not self.cache_last_times.get(cache_collection):
                    self.cache_last_times[cache_collection] = 0
                latest = await self.config.mongo.async_db[cache_collection].find_one(
                    {"cache_time": {"$gt": self.cache_last_times[cache_collection]}},
                    sort=[("height", -1)],
                )
                if latest:
                    self.cache_last_times[cache_collection] = latest["cache_time"]
                else:
                    self.cache_last_times[cache_collection] = 0

                pipeline = [
                    {
                        "$match": {
                            "cache_time": {
                                "$gt": self.cache_last_times[cache_collection]
                            }
                        }
                    },
                    {
                        "$group": {
                            "_id": {"block_hash": "$block_hash", "height": "$height"},
                            "cache_time": {"$first": "$cache_time"},
                        }
                    },
                    {"$sort": {"height": -1}},
                ]
                async for txn in self.config.mongo.async_db[cache_collection].aggregate(
                    pipeline
                ):
                    if not await self.config.mongo.async_db.blocks.find_one(
                        {
                            "index": txn["_id"].get("height"),
                            "hash": txn["_id"].get("block_hash"),
                        }
                    ) and not await self.config.mongo.async_db.miner_transactions.find_one(
                        {
                            "id": txn.get("id"),
                        }
                    ):
                        await self.config.mongo.async_db[cache_collection].delete_many(
                            {"height": txn.get("height")}
                        )
                        break
                    else:
                        if txn["cache_time"] > self.cache_last_times[cache_collection]:
                            self.cache_last_times[cache_collection] = txn["cache_time"]

            self.config.health.cache_validator.last_activity = int(time())
        except Exception:
            self.config.app_log.error("error in background_cache_validator")
            self.config.app_log.error(format_exc())

    async def background_mempool_cleaner(self):
        """Responsible for removing failed transactions from the mempool"""

        self.config.app_log.debug("background_mempool_cleaner")
        if not hasattr(self.config, "background_mempool_cleaner"):
            self.config.background_mempool_cleaner = WorkerVars(busy=False)
        if self.config.background_mempool_cleaner.busy:
            self.config.app_log.debug("background_mempool_cleaner - busy")
            return
        self.config.background_mempool_cleaner.busy = True
        try:
            await self.config.TU.clean_mempool(self.config)
            self.config.health.mempool_cleaner.last_activity = int(time())
        except Exception:
            self.config.app_log.error(format_exc())
        self.config.background_mempool_cleaner.busy = False

    async def background_mempool_sender(self):
        """Responsible for rebroadcasting mempool transactions"""
        self.config.app_log.debug("background_mempool_sender")

        if not hasattr(self.config, "background_mempool_sender"):
            self.config.background_mempool_sender = WorkerVars(busy=False)

        if self.config.background_mempool_sender.busy:
            self.config.app_log.debug("background_mempool_sender - busy")
            return
        self.config.background_mempool_sender.busy = True
        try:
            await self.config.TU.rebroadcast_mempool(
                self.config, NodeRPC.confirmed_peers, include_zero=True
            )
        except Exception:
            self.config.app_log.error(format_exc())
        self.config.background_mempool_sender.busy = False

    async def background_transactions_combining(self):
        """Responsible for combining small UTXOs into larger ones in the background"""

        if not hasattr(self.config, "background_transactions_combining"):
            self.config.background_transactions_combining = WorkerVars(busy=False)
        if self.config.background_transactions_combining.busy:
            self.config.app_log.debug("background_transactions_combining - busy")
            return
        self.config.background_transactions_combining.busy = True
        try:
            await self.config.TU.combine_oldest_transactions(self.config)
        except Exception:
            self.config.app_log.error(format_exc())
        self.config.background_transactions_combining.busy = False

    async def background_nonce_processor(self):
        """Responsible for processing all share submissions from miners"""

        self.config.app_log.debug("background_nonce_processor")
        if not hasattr(self.config, "background_nonce_processor"):
            self.config.background_nonce_processor = WorkerVars(busy=False)
        if self.config.background_nonce_processor.busy:
            self.config.app_log.debug("background_nonce_processor - busy")
            return
        self.config.background_nonce_processor.busy = True
        try:
            if self.config.processing_queues.nonce_queue.queue:
                self.config.processing_queues.nonce_queue.time_sum_start()
                await self.config.mp.process_nonce_queue()
                self.config.processing_queues.nonce_queue.time_sum_end()
            self.config.health.nonce_processor.last_activity = int(time())
        except:
            self.config.app_log.error(format_exc())
            self.config.processing_queues.nonce_queue.time_sum_end()
        self.config.background_nonce_processor.busy = False

    def configure_logging(self):
        # tornado.log.enable_pretty_logging()
        log_dir = path.abspath("static/dashboard/log")
        os.makedirs(log_dir, exist_ok=True)
        self.config.app_log = logging.getLogger("tornado.application")
        tornado.log.enable_pretty_logging(logger=self.config.app_log)
        logfile = os.path.join(log_dir, "yada_app.log")
        # Rotate log after reaching 512K, keep 5 old copies.
        rotateHandler = RotatingFileHandler(logfile, "a", 512 * 1024, 5)
        formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
        rotateHandler.setFormatter(formatter)
        self.config.app_log.addHandler(rotateHandler)
        self.config.app_log.setLevel(logging.INFO)
        if self.config.debug:
            self.config.app_log.setLevel(logging.DEBUG)
        if hasattr(self.config, "asyncio_debug") and self.config.asyncio_debug:
            logging.basicConfig(level=logging.DEBUG)
            AsyncIOMainLoop().install()
            ioloop = IOLoop.current()
            if hasattr(self.config, "asyncio_debug_duration"):
                asyncio_debug_duration = self.config.asyncio_debug_duration
            else:
                asyncio_debug_duration = 0.05
            ioloop.slow_callback_duration = asyncio_debug_duration

        self.access_log = logging.getLogger("tornado.access")
        tornado.log.enable_pretty_logging()
        logfile2 = os.path.join(log_dir, "yada_access.log")
        rotateHandler2 = RotatingFileHandler(logfile2, "a", 512 * 1024, 5)
        formatter2 = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
        rotateHandler2.setFormatter(formatter2)
        self.access_log.addHandler(rotateHandler2)

        self.config.app_log.propagate = False
        self.access_log.propagate = False
        # This logguer config is quite a mess, but works well enough for the time being.
        logging.getLogger("engineio").propagate = False

    def init_config(self, options):
        if not path.isfile(options.config):
            self.config = yadacoin.core.config.Config.generate()
            try:
                os.makedirs(os.path.dirname(options.config))
            except:
                pass
            with open(options.config, "w") as f:
                f.write(self.config.to_json())

        with open(options.config) as f:
            self.config = yadacoin.core.config.Config(json.loads(f.read()))
            # Sets the global var for all objects
            yadacoin.core.config.CONFIG = self.config
            self.config.debug = options.debug
            # force network, command line one takes precedence
            if options.network != "":
                self.config.network = options.network

        self.config.reset = options.reset
        if options.mongohost:
            self.config.mongodb_host = options.mongohost
        if options.modes:
            self.config.modes = options.modes

    def init_consensus(self):
        self.config.consensus = tornado.ioloop.IOLoop.current().run_sync(
            Consensus.init_async
        )
        if options.verify:
            self.config.app_log.info("Verifying existing blockchain")
            tornado.ioloop.IOLoop.current().run_sync(
                self.config.consensus.verify_existing_blockchain
            )
        else:
            self.config.app_log.info(
                "Verification of existing blockchain skipped by config"
            )

    def init_whitelist(self):
        api_whitelist = "api_whitelist.json"
        api_whitelist_filename = options.config.replace(
            ntpath.basename(options.config), api_whitelist
        )
        if path.isfile(api_whitelist_filename):
            with open(api_whitelist_filename) as f:
                self.config.api_whitelist = [x["host"] for x in json.loads(f.read())]

    def init_ioloop(self):
        tornado.ioloop.IOLoop.current().set_default_executor(
            ThreadPoolExecutor(max_workers=1)
        )

        if MODES.NODE.value in self.config.modes:
            PeriodicCallback(
                self.background_status, self.config.status_wait * 1000
            ).start()

            PeriodicCallback(
                self.background_block_checker, self.config.block_checker_wait * 1000
            ).start()

            PeriodicCallback(
                self.background_cache_validator, self.config.cache_validator_wait * 1000
            ).start()

            PeriodicCallback(
                self.background_mempool_cleaner, self.config.mempool_cleaner_wait * 1000
            ).start()

            PeriodicCallback(
                self.background_mempool_sender, self.config.mempool_sender_wait * 1000
            ).start()

            PeriodicCallback(
                self.background_txn_queue_processor,
                self.config.txn_queue_processor_wait * 1000,
            ).start()

            PeriodicCallback(
                self.background_block_queue_processor,
                self.config.block_queue_processor_wait * 1000,
            ).start()

            PeriodicCallback(
                self.background_peers, self.config.peers_wait * 1000
            ).start()

            PeriodicCallback(
                self.background_message_sender, self.config.message_sender_wait * 1000
            ).start()

            if self.config.peer_type in [
                PEER_TYPES.SERVICE_PROVIDER.value,
                PEER_TYPES.SEED_GATEWAY.value,
                PEER_TYPES.SEED.value,
                PEER_TYPES.USER.value,
            ]:
                PeriodicCallback(
                    self.background_transactions_combining,
                    self.config.transactions_combining_wait * 1000,
                ).start()

            if MODES.POOL.value in self.config.modes:
                PeriodicCallback(
                    self.background_nonce_processor,
                    self.config.nonce_processor_wait * 1000,
                ).start()

        if self.config.pool_payout:
            self.config.app_log.info("PoolPayout activated")
            self.config.pp = PoolPayer()

            PeriodicCallback(
                self.background_pool_payer, self.config.pool_payer_wait * 1000
            ).start()

        if (
            hasattr(self.config, "stress_test_newtxn")
            and self.config.stress_test_newtxn
        ):
            PeriodicCallback(self.background_newtxn_stress_test, 3000).start()

        if (
            hasattr(self.config, "stress_test_newblock")
            and self.config.stress_test_newblock
        ):
            PeriodicCallback(self.background_newblock_stress_test, 3000).start()

        while True:
            tornado.ioloop.IOLoop.current().start()

    def init_jwt(self):
        jwt_key = EccKey(curve="p256", d=int(self.config.private_key, 16))
        self.config.jwt_secret_key = jwt_key.export_key(format="PEM")
        self.config.jwt_public_key = (
            self.config.jwt_public_key or jwt_key.public_key().export_key(format="PEM")
        )
        self.config.jwt_options = {
            "verify_signature": True,
            "verify_exp": True,
            "verify_nbf": False,
            "verify_iat": True,
            "verify_aud": False,
        }

    def init_seeds(self):
        if self.config.network == "mainnet":
            self.config.seeds = Peers.get_config_seeds() or Peers.get_seeds()
        elif self.config.network == "regnet":
            self.config.seeds = Peers.get_config_seeds()

    def init_seed_gateways(self):
        if self.config.network == "mainnet":
            self.config.seed_gateways = (
                Peers.get_config_seed_gateways() or Peers.get_seed_gateways()
            )
        elif self.config.network == "regnet":
            self.config.seed_gateways = Peers.get_config_seed_gateways()

    def init_service_providers(self):
        if self.config.network == "mainnet":
            self.config.service_providers = (
                Peers.get_config_service_providers() or Peers.get_service_providers()
            )
        elif self.config.network == "regnet":
            self.config.service_providers = Peers.get_config_service_providers()

    def init_groups(self):
        if self.config.network == "mainnet":
            self.config.groups = Peers.get_config_groups() or Peers.get_groups()
        elif self.config.network == "regnet":
            self.config.groups = Peers.get_config_groups()

    def init_websocket(self):
        self.default_handlers.extend(WEBSOCKET_HANDLERS)

    def init_webui(self):
        self.default_handlers.extend(NODE_HANDLERS)
        self.default_handlers.extend(GRAPH_HANDLERS)
        self.default_handlers.extend(EXPLORER_HANDLERS)
        self.default_handlers.extend(WALLET_HANDLERS)
        self.default_handlers.extend(PRODUCT_HANDLERS)
        self.default_handlers.extend(WEB_HANDLERS)
        self.default_handlers.extend(POOL_HANDLERS)
        self.default_handlers.extend(KEY_EVENT_LOG_HANDLERS)
        self.default_handlers.extend(DASHBOARD_HANDLERS)
        if self.config.peer_type == PEER_TYPES.SERVICE_PROVIDER.value or (
            hasattr(self.config, "activate_peerjs")
            and self.config.activate_peerjs == True
        ):
            self.default_handlers.extend(PEERJS_HANDLERS)

    def init_plugins(self):
        for finder, name, ispkg in pkgutil.iter_modules(
            [path.join(path.dirname(__file__), "..", "plugins")]
        ):
            if os.path.exists(
                path.join(path.dirname(__file__), "..", "plugins", name, "postboot.py")
            ):
                post_boot = importlib.import_module("plugins." + name + ".postboot")
                tornado.ioloop.IOLoop.current().run_sync(lambda: post_boot.go(self))
            handlers = importlib.import_module("plugins." + name + ".handlers")
            if name == self.config.root_app:
                [
                    self.default_handlers.insert(0, handler)
                    for handler in handlers.HANDLERS
                ]
            else:
                self.default_handlers.extend(handlers.HANDLERS)

    def init_http(self):
        self.config.app_log.info(
            "API: http://{}:{}".format(self.config.serve_host, self.config.serve_port)
        )
        if MODES.WEB.value in self.config.modes:
            self.config.app_log.info(
                "Wallet: http://{}:{}/app".format(
                    self.config.serve_host, self.config.serve_port
                )
            )
        if MODES.PROXY.value in self.config.modes:
            self.config.app_log.info(
                "Proxy: {}:{}".format(self.config.serve_host, self.config.proxy_port)
            )
        if os.path.exists(path.join(path.dirname(__file__), "..", "templates")):
            template_path = path.join(path.dirname(__file__), "..", "templates")
        else:
            template_path = path.join(path.dirname(__file__), "templates")
        settings = dict(
            app_title="Yadacoin Node",
            template_path=template_path,
            xsrf_cookies=False,  # TODO: sort out, depending on python client version (< 3.6) does not work with xsrf activated
            cookie_secret=sha256(self.config.private_key.encode("utf-8")).hexdigest(),
            compress_response=True,
            debug=options.debug,  # Also activates auto reload
            autoreload=False,
            serve_traceback=options.debug,
            yadacoin_vars={"node_version": version},
            yadacoin_config=self.config,
            mp=None,
            BU=yadacoin.core.blockchainutils.GLOBAL_BU,
            TU=yadacoin.core.transactionutils.TU,
            compiled_template_cache=False,
        )
        handlers = self.default_handlers.copy()
        super().__init__(handlers, **settings)
        self.config.application = self
        self.config.http_server = HTTPServer(self)
        self.config.http_server.listen(self.config.serve_port, self.config.serve_host)
        if (
            MODES.SSL.value in self.config.modes
            and hasattr(self.config, "ssl")
            and self.config.ssl.is_valid()
        ):
            ssl_ctx = ssl.create_default_context(
                ssl.Purpose.CLIENT_AUTH, cafile=self.config.ssl.ca_file
            )
            ssl_ctx.load_cert_chain(
                self.config.ssl.cert_file, keyfile=self.config.ssl.key_file
            )
            self.config.https_server = HTTPServer(self, ssl_options=ssl_ctx)
            self.config.https_server.listen(self.config.ssl.port)
        if hasattr(self.config, "email") and self.config.email.is_valid():
            self.config.emailer = Email()

    def init_pool(self):
        self.config.app_log.info(
            "Pool: {}:{}".format(self.config.peer_host, self.config.stratum_pool_port)
        )
        StratumServer.inbound_streams[Miner.__name__] = {}
        self.config.pool_server = StratumServer()
        StratumServer.config = self.config
        StratumServer.config.mp = tornado.ioloop.IOLoop.current().run_sync(
            MiningPool.init_async
        )
        self.config.pool_server.listen(self.config.stratum_pool_port)

    def init_peer(self):
        self.config.peer = Peer.my_peer()

    def init_config_properties(self, test=False):
        self.config.health = Health()
        self.config.processing_queues = ProcessingQueues()
        self.config.mongo = Mongo()
        self.config.http_client = AsyncHTTPClient()
        self.config.BU = yadacoin.core.blockchainutils.BlockChainUtils()
        self.config.TU = yadacoin.core.transactionutils.TU
        yadacoin.core.blockchainutils.set_BU(self.config.BU)  # To be removed
        self.config.GU = GraphUtils()
        self.config.LatestBlock = LatestBlock
        if test:
            return
        tornado.ioloop.IOLoop.current().run_sync(self.config.LatestBlock.block_checker)
        self.init_consensus()
        self.config.cipher = Crypt(self.config.wif)
        if MODES.NODE.value in self.config.modes:
            # self.config.pyrx = pyrx.PyRX()
            # self.config.pyrx.get_rx_hash(
            #     "header",
            #     binascii.unhexlify(
            #         "4181a493b397a733b083639334bc32b407915b9a82b7917ac361816f0a1f5d4d"
            #     ),
            #     4,
            # )
            self.config.nodeServer = NodeSocketServer
            self.config.nodeShared = NodeRPC()
            self.config.nodeClient = NodeSocketClient()

            for x in [Seed, SeedGateway, ServiceProvider, User, Miner, Pool]:
                if x.__name__ not in self.config.nodeClient.outbound_streams:
                    self.config.nodeClient.outbound_ignore[x.__name__] = {}
                if x.__name__ not in self.config.nodeClient.outbound_streams:
                    self.config.nodeClient.outbound_pending[x.__name__] = {}
                if x.__name__ not in self.config.nodeClient.outbound_streams:
                    self.config.nodeClient.outbound_streams[x.__name__] = {}
            for x in [Seed, SeedGateway, ServiceProvider, User, Miner, Pool]:
                if x.__name__ not in self.config.nodeServer.inbound_pending:
                    self.config.nodeServer.inbound_pending[x.__name__] = {}
                if x.__name__ not in self.config.nodeServer.inbound_streams:
                    self.config.nodeServer.inbound_streams[x.__name__] = {}

        self.config.websocketServer = RCPWebSocketServer
        self.config.app_log = logging.getLogger("tornado.application")
        self.config.challenges = {}
        if MODES.WEB.value in self.config.modes:
            for x in [User, Group]:
                if x.__name__ not in self.config.websocketServer.inbound_streams:
                    self.config.websocketServer.inbound_streams[x.__name__] = {}

        if MODES.PROXY.value in self.config.modes:
            from yadacoin.http.proxy import ProxyConfig, ProxyHandler

            proxy_app = Application(
                [
                    (r".*", ProxyHandler),
                ],
                compiled_template_cache=False,
                debug=True,
            )
            self.config.proxy_server = HTTPServer(proxy_app)
            self.config.proxy_server.listen(self.config.proxy_port)
            self.config.proxy = ProxyConfig()
            for x in self.config.mongo.site_db.proxy_whitelist.find({}, {"_id": 0}):
                self.config.proxy.white_list[x["domain"]] = x
            for x in self.config.mongo.site_db.proxy_blacklist.find({}, {"_id": 0}):
                self.config.proxy.black_list[x["domain"]] = x
            self.config.proxy_server.inbound_peers = {User.__name__: {}}

        if MODES.DNS.value in self.config.modes:
            from yadacoin.udp.base import UDPServer

            self.config.udpServer = UDPServer
            self.config.udpServer.inbound_streams = {User.__name__: {}}
            # server = tornado.ioloop.IOLoop.current().run_sync(lambda s=UDPServer, ip='127.0.0.1', port=53: tornado.ioloop.IOLoop.current().asyncio_loop.create_server(s, ip, port))
            # tornado.ioloop.IOLoop.current().run_sync(server.serve_forever)
            tornado.ioloop.IOLoop.current().run_sync(
                lambda s=UDPServer, local_addr=(
                    "0.0.0.0",
                    53,
                ): tornado.ioloop.IOLoop.current().asyncio_loop.create_datagram_endpoint(
                    s, local_addr=local_addr
                )
            )


if __name__ == "__main__":
    NodeApplication()
