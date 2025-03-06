"""
YadaCoin Open Source License (YOSL) v1.1

Copyright (c) 2017-2025 Matthew Vogel, Reynold Vogel, Inc.

This software is licensed under YOSL v1.1 – for personal and research use only.
NO commercial use, NO blockchain forks, and NO branding use without permission.

For commercial license inquiries, contact: info@yadacoin.io

Full license terms: see LICENSE.txt in this repository.
"""

"""
Handlers required by the pool operations
"""

from yadacoin.http.base import BaseHandler


class PoolSharesHandler(BaseHandler):
    async def get(self):
        address = self.get_query_argument("address")
        query = {"address": address}
        if "." not in address:
            query = {
                "$or": [
                    {"address": address},
                    {"address_only": address},
                ]
            }
        total_share = await self.config.mongo.async_db.shares.count_documents(query)
        total_hash = total_share * self.config.pool_diff
        self.render_as_json({"total_hash": int(total_hash)})


class PoolPayoutsHandler(BaseHandler):
    async def get(self):
        address = self.get_query_argument("address")
        query = {"address": address}
        if "." in address:
            query = {"address": address.split(".")[0]}
        out = []
        results = self.config.mongo.async_db.share_payout.find(
            {"txn.outputs.to": address}, {"_id": 0}
        ).sort([("index", -1)])
        async for result in results:
            if (
                await self.config.mongo.async_db.blocks.count_documents(
                    {"transactions.id": result["txn"]["id"]}
                )
                > 0
            ):
                out.append(result)
        self.render_as_json({"results": out})


class PoolHashRateHandler(BaseHandler):
    async def get(self):
        address = self.get_query_argument("address")
        query = {"address": address}
        if "." not in address:
            query = {
                "$or": [
                    {"address": address},
                    {"address_only": address},
                ]
            }
        last_share = await self.config.mongo.async_db.shares.find_one(
            query, {"_id": 0}, sort=[("time", -1)]
        )
        if not last_share:
            return self.render_as_json({"result": 0})
        miner_hashrate_seconds = (
            self.config.miner_hashrate_seconds
            if hasattr(self.config, "miner_hashrate_seconds")
            else 1200
        )

        query = {"time": {"$gt": last_share["time"] - miner_hashrate_seconds}}
        if "." in address:
            query["address"] = address
        else:
            query["$or"] = [
                {"address": address},
                {"address_only": address},
            ]
        number_of_shares = await self.config.mongo.async_db.shares.count_documents(
            query
        )
        miner_hashrate = (
            number_of_shares * self.config.pool_diff
        ) / miner_hashrate_seconds
        self.render_as_json({"miner_hashrate": int(miner_hashrate)})


class PoolScanMissedPayoutsHandler(BaseHandler):
    async def get(self):
        start_index = self.get_query_argument("start_index")
        await self.config.pp.do_payout({"index": int(start_index)})
        self.render_as_json({"status": True})

class NodeTestResultsHandler(BaseHandler):
    async def get(self):
        """
        Returns the last 120 node test results, grouped into charts of 40 results each.
        """
        MAX_RESULTS = 120
        CHART_SIZE = 40

        # Fetch the last 120 records, sorted by test_time in descending order
        cursor = self.config.mongo.async_db.node_test_result.find(
            {}, {"_id": 0}
        ).sort([("test_time", -1)]).limit(MAX_RESULTS)

        # Convert cursor to list of results
        results = []
        async for record in cursor:
            results.append(record)

        if not results:
            self.render_as_json({"charts": []})
            return

        # Sort results ascending by test_time for better chart readability
        results.sort(key=lambda x: x["test_time"])

        # Split results into charts of 40 results each
        charts_data = []
        for i in range(0, len(results), CHART_SIZE):
            chunk = results[i:i + CHART_SIZE]
            charts_data.append({
                "start_time": chunk[0]["test_time"],
                "end_time": chunk[-1]["test_time"],
                "data": chunk
            })

        # Return the grouped data as JSON
        self.render_as_json({"charts": charts_data})

# class PoolForceRefresh(BaseHandler):
#     async def get(self):
#         await self.config.mp.refresh()


POOL_HANDLERS = [
    (r"/shares-for-address", PoolSharesHandler),
    (r"/payouts-for-address", PoolPayoutsHandler),
    (r"/hashrate-for-address", PoolHashRateHandler),
    (r"/scan-missed-payouts", PoolScanMissedPayoutsHandler),
    (r"/node-test-results", NodeTestResultsHandler),
    # (r"/force-refresh", PoolForceRefresh),
]
