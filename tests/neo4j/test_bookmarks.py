from uuid import uuid4

import nutkit.protocol as types
from tests.neo4j.shared import (
    cluster_unsafe_test,
    get_driver,
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

    @cluster_unsafe_test
    def test_can_obtain_bookmark_after_commit(self):
        self._session = self._driver.session("w")
        tx = self._session.begin_transaction()
        tx.run("RETURN 1")
        tx.commit()
        bookmarks = self._session.last_bookmarks()
        self.assertTrue(bookmarks)

    @cluster_unsafe_test
    def test_can_pass_bookmark_into_next_session(self):
        # TODO: remove this block once all languages work
        if get_driver_name() in ["dotnet"]:
            self.skipTest("Backend seems to misinterpret query parameters")
        unique_id = uuid4().hex

        self._session = self._driver.session("w")
        tx = self._session.begin_transaction()
        tx.run("CREATE (a:Thing {uuid:$uuid})",
               params={"uuid": types.CypherString(unique_id)})
        tx.commit()
        bookmarks = self._session.last_bookmarks()
        self.assertEqual(len(bookmarks), 1)

        self._session.close()
        self._session = self._driver.session("r", bookmarks)
        tx = self._session.begin_transaction()
        result = tx.run("MATCH (a:Thing {uuid:$uuid}) RETURN a",
                        params={"uuid": types.CypherString(unique_id)})
        if self.driver_supports_features(types.Feature.TMP_RESULT_KEYS):
            self.assertEqual(result.keys(), ["a"])
        records = [rec.values[0] for rec in result]
        tx.commit()
        self.assertEqual(len(records), 1)
        thing = records[0]
        self.assertIsInstance(thing, types.CypherNode)
        self.assertIn("uuid", thing.props.value)
        self.assertEqual(thing.props.value["uuid"],
                         types.CypherString(unique_id))

    @cluster_unsafe_test
    def test_no_bookmark_after_rollback(self):
        self._session = self._driver.session("w")
        tx = self._session.begin_transaction()
        tx.run("CREATE (a)")
        tx.rollback()

        bookmarks = self._session.last_bookmarks()
        self.assertEqual(len(bookmarks), 0)

    @cluster_unsafe_test
    def test_fails_on_invalid_bookmark(self):
        # TODO: remove this block once all languages work
        if get_driver_name() in ["javascript"]:
            self.skipTest("Fails the exception code assertion")
        self._session = self._driver.session(
            "w", ["hi, this is an invalid bookmark"])
        with self.assertRaises(types.DriverError) as exc:
            tx = self._session.begin_transaction()
            result = tx.run("RETURN 1")
            result.next()
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
            "w", ["hi, this is an invalid bookmark"])

        def work(tx):
            result = tx.run("RETURN 1")
            result.next()

        with self.assertRaises(types.DriverError) as exc:
            self._session.read_transaction(work)
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
                params={"uuid": types.CypherString(test_execution_id)})
            result.consume()

        for _ in range(expected_node_count):
            self._session = self._driver.session("w")
            self._session.write_transaction(create_node)
            bookmarks.append(self._session.last_bookmarks())
            self._session.close()

        bookmarks = [bookmark for sublist in bookmarks for bookmark in sublist]
        self._session = self._driver.session("r", bookmarks)

        def get_node_count(tx):
            result = tx.run(
                "MATCH (t:MultipleBookmarksTest {testId:$uuid})"
                " RETURN count(t)",
                params={"uuid": types.CypherString(test_execution_id)})
            record = result.next()
            return record.values[0]

        count = self._session.read_transaction(get_node_count)
        self.assertEqual(types.CypherInt(expected_node_count), count)

    @cluster_unsafe_test
    def test_can_pass_write_bookmark_into_write_session(self):
        test_execution_id = uuid4().hex
        self._session = self._driver.session("w")
        tx = self._session.begin_transaction()
        result = tx.run(
            "CREATE (t:AccessModeTest {testId:$uuid})",
            params={"uuid": types.CypherString(test_execution_id)})
        result.consume()
        tx.commit()
        bookmarks = self._session.last_bookmarks()
        self._session.close()

        self._session = self._driver.session("w", bookmarks)
        tx = self._session.begin_transaction()
        result = tx.run(
            "MATCH (t:AccessModeTest {testId:$uuid})"
            " RETURN count(t)",
            params={"uuid": types.CypherString(test_execution_id)})
        record = result.next()
        node_count = record.values[0]
        tx.commit()

        self.assertEqual(types.CypherInt(1), node_count)

    @cluster_unsafe_test
    def test_can_pass_read_bookmark_into_write_session(self):
        test_execution_id = uuid4().hex
        self._session = self._driver.session("w")
        tx = self._session.begin_transaction()
        result = tx.run(
            "CREATE (t:AccessModeTest {testId:$uuid})",
            params={"uuid": types.CypherString(test_execution_id)})
        result.consume()
        tx.commit()
        bookmarks = self._session.last_bookmarks()
        self._session.close()

        self._session = self._driver.session("r", bookmarks)
        tx = self._session.begin_transaction()
        result = tx.run(
            "MATCH (t:AccessModeTest {testId:$uuid}) "
            "RETURN count(t)",
            params={"uuid": types.CypherString(test_execution_id)})
        record = result.next()
        node_count1 = record.values[0]
        tx.commit()
        bookmarks = self._session.last_bookmarks()
        self._session.close()

        self._session = self._driver.session("w", bookmarks)
        tx = self._session.begin_transaction()
        result = tx.run(
            "CREATE (t:AccessModeTest {testId:$uuid})",
            params={"uuid": types.CypherString(test_execution_id)})
        result.consume()
        tx.commit()
        bookmarks = self._session.last_bookmarks()
        self._session.close()

        self._session = self._driver.session("r", bookmarks)
        tx = self._session.begin_transaction()
        result = tx.run(
            "MATCH (t:AccessModeTest {testId:$uuid}) "
            "RETURN count(t)",
            params={"uuid": types.CypherString(test_execution_id)})
        record = result.next()
        node_count2 = record.values[0]
        tx.commit()

        self.assertEqual(types.CypherInt(1), node_count1)
        self.assertEqual(types.CypherInt(2), node_count2)
