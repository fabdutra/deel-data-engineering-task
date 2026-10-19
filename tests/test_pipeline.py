import datetime as dt
import json
import tempfile
import unittest
from pathlib import Path
from pyspark.sql import SparkSession
from app.config import Settings
from app.materialize import initialize, materialize_batch
from app.metrics import load_publication, register_views, result
from app.schemas import BRONZE_SCHEMA, TABLES
from app.storage import read_delta


class StreamingCorrectnessTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.getActiveSession()

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="acme-tests-")
        self.settings = Settings(lake_root=self.directory.name)
        self.offset = 0
        initialize(self.spark, self.settings)

    def tearDown(self):
        self.spark.catalog.clearCache()
        self.directory.cleanup()

    def event(self, table, row, op="c", lsn=1, before=None, key=None):
        self.offset += 1
        pk = TABLES[table][0]
        key = key if key is not None else row[pk]
        payload = {"op": op, "before": before, "after": row if op != "d" else None,
                   "source": {"lsn": lsn, "ts_ms": 1700000000000 + lsn}, "ts_ms": 1700000001000 + lsn}
        if op == "d":
            payload["before"] = before
        now = dt.datetime.now(dt.timezone.utc)
        return (json.dumps({pk: key}).encode(), json.dumps(payload).encode(),
                f"finance_db.operations.{table}", 0, self.offset, now, now)

    def batch(self, events, batch_id, fault=None):
        # Structured Streaming invokes foreachBatch with a separate session.
        raw = self.spark.newSession().createDataFrame(events, BRONZE_SCHEMA)
        materialize_batch(self.spark, self.settings, raw, batch_id, fault)

    def rows(self, metric):
        register_views(self.spark, self.settings, load_publication(self.spark, self.settings))
        return [r.asDict() for r in result(self.spark, metric).collect()]

    def order(self, order_id, customer=1, status="PENDING", date=20010):
        return dict(order_id=order_id, customer_id=customer, status=status, order_date=20000, delivery_date=date)

    def item(self, item_id, order_id, product=10, quantity=3):
        return dict(order_item_id=item_id, order_id=order_id, product_id=product, quanity=quantity)

    def test_cdc_metrics_recovery_deletes_and_late_parents(self):
        events = [self.event("customers", dict(customer_id=i, customer_name=name, is_active=True), "r")
                  for i, name in ((1, "Alice"), (2, "Bob"), (3, "Carol"))]
        events += [self.event("products", dict(product_id=i, product_name=f"Product {i}", unity_price="12.34"), "r") for i in (10, 20)]
        orders = [self.order(100), self.order(101, status="COMPLETED", date=20011),
                  self.order(102, 2, "PROCESSING"), self.order(103, 3, "REPROCESSING", 20012), self.order(104, date=20011)]
        events += [self.event("orders", o, "r") for o in orders]
        events += [self.event("order_items", i, "r") for i in
                   (self.item(1000, 100), self.item(1001, 100, quantity=2),
                    self.item(1002, 102, product=20, quantity=4), self.item(1003, 103, quantity=7))]
        self.batch(events, 0)
        self.assertEqual(sum(r["open_orders"] for r in self.rows("open-orders")), 4)
        self.assertEqual([r["open_orders"] for r in self.rows("top-delivery-dates")], [2, 1, 1])
        self.assertEqual(self.rows("pending-items"), [dict(product_id=10, pending_item_lines=3, pending_units=12),
                                                       dict(product_id=20, pending_item_lines=1, pending_units=4)])
        self.assertEqual([(r["customer_id"], r["pending_orders"]) for r in self.rows("top-customers")], [(1, 2), (2, 1), (3, 1)])
        self.assertEqual(str(read_delta(self.spark, self.settings.path("silver/products")).first()["unity_price"]), "12.3400")
        baseline = load_publication(self.spark, self.settings)["versions"]
        updates = [self.event("orders", self.order(100, status="COMPLETED"), "u", 2, orders[0]),
                   self.event("orders", self.order(104, 2, date=20012), "u", 3, orders[-1]),
                   self.event("order_items", self.item(1002, 102, quantity=5), "u", 2)]

        def fail(stage):
            if stage == "before_publication":
                raise RuntimeError("Injected crash after Gold writes")

        with self.assertRaisesRegex(RuntimeError, "Injected crash"):
            self.batch(updates, 1, fail)
        self.assertEqual(load_publication(self.spark, self.settings)["versions"], baseline)
        self.assertEqual(sum(r["open_orders"] for r in self.rows("open-orders")), 4)
        # Replay the exact batch, including duplicate delivery; published data must converge.
        self.batch(updates + updates, 1)
        self.assertEqual(self.rows("pending-items"), [dict(product_id=10, pending_item_lines=2, pending_units=12)])
        self.assertEqual([(r["customer_id"], r["pending_orders"]) for r in self.rows("top-customers")], [(2, 2), (3, 1)])
        self.batch([self.event("orders", None, "d", 4, key=102),
                    self.event("order_items", None, "d", 4, key=1003)], 2)
        self.assertEqual(self.rows("pending-items"), [])
        # Older events cannot resurrect deleted records, including across batches.
        self.batch([self.event("orders", self.order(102, 2, "PROCESSING"), "u", 1),
                    self.event("order_items", self.item(1003, 103, quantity=7), "u", 1)], 3)
        self.assertEqual(self.rows("pending-items"), [])
        self.batch([self.event("order_items", self.item(2000, 999, quantity=11), lsn=5)], 4)
        self.assertEqual(self.rows("pending-items"), [])
        self.batch([self.event("orders", self.order(999, customer=999, date=None), lsn=6)], 5)
        self.assertEqual(self.rows("pending-items"), [dict(product_id=10, pending_item_lines=1, pending_units=11)])
        self.assertTrue(any(r["customer_id"] == 999 and r["customer_name"] is None for r in self.rows("top-customers")))
        # A Kafka null tombstone is retained in Bronze, but does not mutate Silver.
        now = dt.datetime.now(dt.timezone.utc)
        self.batch([(b'{"order_id":102}', None, "finance_db.operations.orders", 0, 999, now, now)], 6)
        self.assertEqual(sum(r["open_orders"] for r in self.rows("open-orders")), 3)
        # A new item must join an unchanged order from the initial snapshot,
        # even though that order has no event in this microbatch. Include a new
        # order as well: metrics must combine preexisting and newly created data.
        history_order = read_delta(self.spark, self.settings.path("silver/orders")).filter("order_id = 103").first()
        self.assertEqual(history_order["_lsn"], 1)
        self.batch([
            self.event("order_items", self.item(3000, 103, product=20, quantity=13), lsn=7),
            self.event("orders", self.order(10000, customer=1, date=20013), lsn=7),
            self.event("order_items", self.item(3001, 10000, quantity=2), lsn=7),
        ], 7)
        self.assertEqual(self.rows("pending-items"), [
            dict(product_id=10, pending_item_lines=2, pending_units=13),
            dict(product_id=20, pending_item_lines=1, pending_units=13),
        ])
        historical_join = read_delta(self.spark, self.settings.path("gold/fact_order_item")).filter("order_item_id = 3000").first()
        self.assertEqual((historical_join["order_id"], historical_join["status"], historical_join["customer_key"]),
                         (103, "REPROCESSING", 3))
        self.assertEqual(sum(r["open_orders"] for r in self.rows("open-orders")), 4)
        self.assertEqual(read_delta(self.spark, self.settings.path("silver/orders")).filter("order_id = 103").first()["_lsn"], 1)
        # Invalid data is durable and blocks a new publication instead of silently losing CDC.
        previous = load_publication(self.spark, self.settings)["versions"]
        with self.assertRaisesRegex(ValueError, "quarantined"):
            self.batch([self.event("orders", self.order(100, status="UNKNOWN"), "u", 99)], 8)
        self.assertEqual(load_publication(self.spark, self.settings)["versions"], previous)
        self.assertEqual(read_delta(self.spark, self.settings.path("quarantine/events")).count(), 1)


if __name__ == "__main__":
    unittest.main()
