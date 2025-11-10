"""
YadaCoin Open Source License (YOSL) v1.1

Copyright (c) 2017-2025 Matthew Vogel, Reynold Vogel, Inc.

This software is licensed under YOSL v1.1 – for personal and research use only.
NO commercial use, NO blockchain forks, and NO branding use without permission.

For commercial license inquiries, contact: info@yadacoin.io

Full license terms: see LICENSE.txt in this repository.
"""

import asyncio
import time

from yadacoin.core.config import Config


class LatestBlock:
    config = None
    block = None

    # Retarget / rebuild control
    _retarget_tip_hash = None
    _retarget_tip_index = None
    _retarget_tip_time = None
    _retarget_done = False
    _build_lock = asyncio.Lock()

    # Template cache
    blocktemplate_target = None
    blocktemplate_time = None
    blocktemplate_index = None
    blocktemplate_hash = None
    cached_transactions = []


    @classmethod
    async def set_config(cls):
        cls.config = Config()

    @classmethod
    async def block_checker(cls):
        if not cls.config:
            await cls.set_config()
        await cls.update_latest_block()

    @classmethod
    async def update_latest_block(cls):
        from yadacoin.core.block import Block

        block = await cls.config.mongo.async_db.blocks.find_one(
            {}, {"_id": 0}, sort=[("index", -1)]
        )
        if not block:
            await cls.config.BU.insert_genesis()
            return
        cls.block = await Block.from_dict(block)

    @classmethod
    async def get_latest_block(cls):
        from yadacoin.core.block import Block

        block = await cls.config.mongo.async_db.blocks.find_one(
            {}, {"_id": 0}, sort=[("index", -1)]
        )
        if block:
            return await Block.from_dict(block)
        else:
            cls.block = None

    @classmethod
    async def get_block_template(cls):
        from yadacoin.core.block import Block
        from yadacoin.core.transaction import Transaction
        import time as _time

        # Upewnij się, że mamy aktualny tip
        if not cls.block:
            await cls.update_latest_block()
        if not cls.block:
            return None

        now       = int(_time.time())
        tip_hash  = cls.block.hash
        tip_index = cls.block.index
        tip_time  = int(cls.block.time)   # czas „prawdziwy” z łańcucha, nie lokalny

        # --- DETEKCJA ZMIANY TIP'A / REORG'U ---
        tip_changed = (
            tip_hash  != getattr(cls, "_retarget_tip_hash",  None) or
            tip_index != getattr(cls, "_retarget_tip_index", None) or
            tip_time  != getattr(cls, "_retarget_tip_time",  None)
        )
        if tip_changed:
            cls._retarget_tip_hash  = tip_hash
            cls._retarget_tip_index = tip_index
            cls._retarget_tip_time  = tip_time
            cls._retarget_done      = False  # pozwól na jednorazowy rebuild dla nowego tipa

            # Inwalidacja cache'u, żeby kolejny przebieg mógł zbudować template
            cls.blocktemplate_hash  = None
            cls.blocktemplate_index = None
            # cls.blocktemplate_time i target zostaną nadpisane przy rebuildzie

        # --- ILE MINĘŁO OD TIP-TIME ---
        delta_from_tip = max(0, now - tip_time)

        # --- WARUNKI REBUILD'U ---
        # 1) tip zmienił się albo nie mamy jeszcze zbudowanego template'u
        needs_rebuild = (
            (cls.block.hash  != getattr(cls, "blocktemplate_hash",  None)) or
            (cls.block.index != getattr(cls, "blocktemplate_index", None)) or
            (getattr(cls, "blocktemplate_time", None) is None) or
            (getattr(cls, "blocktemplate_target", None) is None)
        )

        # 2) jednorazowy refresh po ~20 min od czasu tipa (2×target_time + margines)
        EDGE_SEC = 1205
        edge_rebuild = (delta_from_tip >= EDGE_SEC) and (getattr(cls, "_retarget_done", False) is False)

        if needs_rebuild or edge_rebuild:
            async with cls._build_lock:
                # double-check po locku (równoległy /get mógł już odbudować)
                now = int(_time.time())
                delta_from_tip = max(0, now - tip_time)

                needs_rebuild = (
                    (cls.block.hash  != getattr(cls, "blocktemplate_hash",  None)) or
                    (cls.block.index != getattr(cls, "blocktemplate_index", None)) or
                    (getattr(cls, "blocktemplate_time", None) is None) or
                    (getattr(cls, "blocktemplate_target", None) is None)
                )
                edge_rebuild = (delta_from_tip >= EDGE_SEC) and (getattr(cls, "_retarget_done", False) is False)

                if needs_rebuild or edge_rebuild:
                    try:
                        tx_raw = await cls.config.mongo.async_db.miner_transactions.find().to_list(length=None)
                        txs = [Transaction.ensure_instance(txn) for txn in tx_raw]

                        prev_hash = cls.block.hash if cls.block.hash else "0" * 64
                        new_block = await Block.generate(
                            transactions=txs,
                            index=cls.block.index + 1,
                            prev_hash=prev_hash
                        )

                        cls.blocktemplate_time   = now
                        cls.blocktemplate_index  = cls.block.index
                        cls.blocktemplate_hash   = cls.block.hash
                        cls.blocktemplate_target = new_block.target
                        cls.cached_transactions  = txs

                        if edge_rebuild:
                            cls._retarget_done = True
                            cls.config.app_log.info(
                                f"[GBT] one-shot refresh at Δ={delta_from_tip}s for tip={cls.block.index} "
                                f"target={hex(cls.blocktemplate_target)[2:18]}..."
                            )
                    except Exception as e:
                        cls.config.app_log.error(f"Error generating full block template: {e}")
                        raise

        # Bezpieczne wartości wyjściowe
        target_hex = (
            hex(cls.blocktemplate_target)[2:].rjust(64, '0')
            if getattr(cls, "blocktemplate_target", None) is not None else "0" * 64
        )
        time_out = getattr(cls, "blocktemplate_time", now)
        tx_out   = [txn.to_dict() for txn in getattr(cls, "cached_transactions", [])]

        return {
            "version": cls.block.version,
            "prev_hash": cls.block.hash if cls.block.hash else "0" * 64,
            "index": cls.block.index + 1,
            "target": target_hex,
            "time": time_out,
            "transactions": tx_out,
        }
