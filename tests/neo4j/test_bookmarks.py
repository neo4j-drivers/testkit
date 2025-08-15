from uuid import uuid4

import nutkit.protocol as types
from tests.neo4j.shared import (
    get_driver,
    with_retries,
)
from tests.shared import (
    get_driver_name,
    TestkitTestCase,
)


class TestBookmarks(TestkitTestCase):
    def setUp(self):
        super().setUp()
        self._driver = get_driver(self._backend)
        self._session = None

    def tearDown(self):
        if self._session:
            self._session.close()
        self._driver.close()
        super().tearDown()

    def test_can_obtain_bookmark_after_commit(self):
        def work(session):
            tx = session.begin_transaction()
            tx.run("RETURN 1")
            tx.commit()
            return self._session.last_bookmarks()

        self._session = self._driver.session("w")
        bookmarks = with_retries(work, self._session)
        self.assertTrue(bookmarks)

    def test_can_pass_bookmark_into_next_session(self):
        # TODO: remove this block once all languages work
        if get_driver_name() in ["dotnet"]:
            self.skipTest("Backend seems to misinterpret query parameters")
        unique_id = uuid4().hex

        def create(session):
            tx = session.begin_transaction()
            tx.run("MERGE (a:Thing {uuid:$uuid})",
                   params={"uuid": types.CypherString(unique_id)})
            tx.commit()
            return session.last_bookmarks()

        self._session = self._driver.session("w")
        bookmarks = with_retries(create, self._session)
        self._session.close()
        self.assertEqual(len(bookmarks), 1)

        def read(session):
            tx = session.begin_transaction()
            result = tx.run("MATCH (a:Thing {uuid:$uuid}) RETURN a",
                            params={"uuid": types.CypherString(unique_id)})
            self.assertEqual(result.keys(), ["a"])
            records = [rec.values[0] for rec in result]
            tx.commit()
            return records

        self._session = self._driver.session("r", bookmarks)
        records = with_retries(read, self._session)
        self.assertEqual(len(records), 1)
        thing = records[0]
        self.assertIsInstance(thing, types.CypherNode)
        self.assertIn("uuid", thing.props.value)
        self.assertEqual(thing.props.value["uuid"],
                         types.CypherString(unique_id))

    def test_no_bookmark_after_rollback(self):
        def work(session):
            tx = session.begin_transaction()
            tx.run("CREATE (a)")
            tx.rollback()
            return session.last_bookmarks()

        self._session = self._driver.session("w")
        bookmarks = with_retries(work, self._session)
        self.assertEqual(len(bookmarks), 0)

    def test_fails_on_invalid_bookmark(self):
        # TODO: remove this block once all languages work
        if get_driver_name() in ["javascript"]:
            self.skipTest("Fails the exception code assertion")

        def work(session):
            tx = session.begin_transaction()
            result = tx.run("RETURN 1")
            result.next()

        self._session = self._driver.session(
            "w", ["hi, this is an invalid bookmark"]
        )
        with self.assertRaises(types.DriverError) as exc:
            with_retries(work, self._session)
        if get_driver_name() in ["java"]:
            self.assertEqual(
                "org.neo4j.driver.exceptions.ClientException",
                exc.exception.errorType
            )
        elif get_driver_name() in ["python"]:
            self.assertEqual(
                "<class 'neo4j.exceptions.ClientError'>",
                exc.exception.errorType
            )
        elif get_driver_name() in ["ruby"]:
            self.assertEqual(
                "Neo4j::Driver::Exceptions::ClientException",
                exc.exception.errorType
            )
        self.assertEqual("Neo.ClientError.Transaction.InvalidBookmark",
                         exc.exception.code)

    def test_fails_on_invalid_bookmark_using_tx_func(self):
        # TODO: remove this block once all languages work
        if get_driver_name() in ["go"]:
            self.skipTest("Fails the exception code assertion")
        # TODO: remove this block once all languages work
        if get_driver_name() in ["javascript"]:
            self.skipTest("Times out when invoking the transaction function")
        self._session = self._driver.session(
            "w", ["hi, this is an invalid bookmark"]
        )

        def work(tx):
            result = tx.run("RETURN 1")
            result.next()

        with self.assertRaises(types.DriverError) as exc:
            self._session.execute_read(work)
        if get_driver_name() in ["java"]:
            self.assertEqual(
                "org.neo4j.driver.exceptions.ClientException",
                exc.exception.errorType
            )
        elif get_driver_name() in ["python"]:
            self.assertEqual(
                "<class 'neo4j.exceptions.ClientError'>",
                exc.exception.errorType
            )
        elif get_driver_name() in ["ruby"]:
            self.assertEqual(
                "Neo4j::Driver::Exceptions::ClientException",
                exc.exception.errorType
            )
        self.assertEqual("Neo.ClientError.Transaction.InvalidBookmark",
                         exc.exception.code)

    def test_can_handle_multiple_bookmarks(self):
        bookmarks = []
        expected_node_count = 5
        test_execution_id = uuid4().hex

        def create_node(tx):
            result = tx.run(
                "CREATE (t:MultipleBookmarksTest {testId:$uuid})",
                params={"uuid": types.CypherString(test_execution_id)}
            )
            result.consume()

        for _ in range(expected_node_count):
            self._session = self._driver.session("w")
            self._session.execute_write(create_node)
            bookmarks.append(self._session.last_bookmarks())
            self._session.close()

        bookmarks = [bookmark for sublist in bookmarks for bookmark in sublist]
        self._session = self._driver.session("r", bookmarks)

        def get_node_count(tx):
            result = tx.run(
                "MATCH (t:MultipleBookmarksTest {testId:$uuid})"
                " RETURN count(t)",
                params={"uuid": types.CypherString(test_execution_id)}
            )
            record = result.next()
            return record.values[0]

        count = self._session.execute_read(get_node_count)
        self.assertEqual(types.CypherInt(expected_node_count), count)

    def test_can_pass_write_bookmark_into_write_session(self):
        test_execution_id = uuid4().hex

        def create(session):
            tx = session.begin_transaction()
            result = tx.run(
                "MERGE (t:AccessModeTest {testId:$uuid})",
                params={"uuid": types.CypherString(test_execution_id)}
            )
            result.consume()
            tx.commit()
            return self._session.last_bookmarks()

        self._session = self._driver.session("w")
        bookmarks = with_retries(create, self._session)
        self._session.close()

        def read(session):
            tx = session.begin_transaction()
            result = tx.run(
                "MATCH (t:AccessModeTest {testId:$uuid})"
                " RETURN count(t)",
                params={"uuid": types.CypherString(test_execution_id)}
            )
            record = result.next()
            node_count = record.values[0]
            tx.commit()
            return node_count

        self._session = self._driver.session("w", bookmarks)
        node_count = with_retries(read, self._session)
        self.assertEqual(types.CypherInt(1), node_count)

    def test_can_pass_read_bookmark_into_write_session(self):
        test_execution_id = uuid4().hex

        def create1(session):
            tx = session.begin_transaction()
            result = tx.run(
                "MERGE (t:AccessModeTest {testId:$uuid, num: 1})",
                params={"uuid": types.CypherString(test_execution_id)}
            )
            result.consume()
            tx.commit()
            return session.last_bookmarks()

        self._session = self._driver.session("w")
        bookmarks = with_retries(create1, self._session)
        self._session.close()

        def read(session):
            tx = session.begin_transaction()
            result = tx.run(
                "MATCH (t:AccessModeTest {testId:$uuid}) "
                "RETURN count(t)",
                params={"uuid": types.CypherString(test_execution_id)}
            )
            record = result.next()
            node_count = record.values[0]
            tx.commit()
            return session.last_bookmarks(), node_count

        self._session = self._driver.session("r", bookmarks)
        bookmarks, node_count1 = with_retries(read, self._session)
        self._session.close()

        def create2(session):
            tx = session.begin_transaction()
            result = tx.run(
                "MERGE (t:AccessModeTest {testId:$uuid, num: 2})",
                params={"uuid": types.CypherString(test_execution_id)}
            )
            result.consume()
            tx.commit()
            return session.last_bookmarks()

        self._session = self._driver.session("w", bookmarks)
        bookmarks = with_retries(create2, self._session)
        self._session.close()

        self._session = self._driver.session("r", bookmarks)
        _, node_count2 = with_retries(read, self._session)

        self.assertEqual(types.CypherInt(1), node_count1)
        self.assertEqual(types.CypherInt(2), node_count2)
