import asyncio
import socket
import time

from datetime import timedelta

from tornado.iostream import StreamClosedError
from tornado.tcpclient import TCPClient
from tornado.util import TimeoutError

from yadacoin.core.nodes import Nodes
from yadacoin.core.config import Config


class NodesTester:
    successful_nodes = []
    all_nodes = []

    @classmethod
    async def test_all_nodes(cls, block_index):
        """
        Test all masternodes and return the list of successful ones.
        Save results to MongoDB and update successful_nodes and all_nodes.
        """
        config = Config()  # Inicjalizacja Config w metodzie
        semaphore = asyncio.Semaphore(20)  # Limit concurrent tests
        nodes = Nodes.get_all_nodes_for_block_height(block_index)

        tasks = [cls.test_node(config, node, semaphore) for node in nodes]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        successful_nodes = [node for node in results if node is not None]
        failed_nodes = len(nodes) - len(successful_nodes)

        # Save results to MongoDB
        await config.mongo.async_db.node_test_result.insert_one({
            "test_time": time.time(),
            "successful_count": len(successful_nodes),
            "failed_count": failed_nodes,
            "total_nodes": len(nodes),
        })

        # Update class-level lists
        cls.successful_nodes = successful_nodes
        cls.all_nodes = nodes

        config.app_log.info(
            f"Node testing completed: {len(successful_nodes)} successful, "
            f"{failed_nodes} failed, Total Nodes: {len(nodes)}"
        )

        # Fallback to all nodes if no nodes passed the test
        if not successful_nodes:
            config.app_log.warning(
                "No nodes passed the test. Falling back to all nodes that match the criteria."
            )
            cls.successful_nodes = nodes

        return successful_nodes

    @staticmethod
    async def test_node(config, node, semaphore, retries=1):
        """
        Test a single masternode for connectivity with retry logic.
        """
        async with semaphore:
            for attempt in range(retries + 1):
                try:
                    # DNS resolution block
                    try:
                        socket.gethostbyname(node.host)
                    except socket.gaierror as dns_error:
                        config.app_log.warning(
                            f"DNS resolution failed for {node.host}:{node.port}, error: {dns_error}"
                        )
                        return None

                    # TCP connectivity
                    stream = await TCPClient().connect(
                        node.host, node.port, timeout=timedelta(seconds=5)
                    )
                    return node
                except (StreamClosedError, TimeoutError) as e:
                    config.app_log.warning(
                        f"Attempt {attempt + 1} failed for {node.host}:{node.port} - {e}"
                    )
                    if attempt == retries:
                        config.app_log.warning(
                            f"Max retries reached for {node.host}:{node.port}. Skipping..."
                        )
                        return None
                except Exception as e:
                    config.app_log.warning(
                        f"Unhandled exception in block generate: testing masternode {node.host}:{node.port}, error: {e}"
                    )
                    return None
                finally:
                    if "stream" in locals() and not stream.closed():
                        stream.close()