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
        self._session = self._driver.session("r")
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
