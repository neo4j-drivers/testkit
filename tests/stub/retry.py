import unittest, subprocess, os

from tests.shared import *

stubserver_repo = "/home/peter/code/neo4j-drivers/boltstub"

os.environ['PYTHONPATH'] = stubserver_repo


class TestRetry(unittest.TestCase):
    def setUp(self):
        self._backend = new_backend()
        self._server = None

    def tearDown(self):
        self._backend.close()

    def test_read(self):
        # Start a stub server with the correct script
        # PYTHONPATH=/home/peter/code/neo4j-drivers/boltstub python3 -m boltstub.__main__ -l :9001 script
        subprocess.run([
            "python3", "-m", "boltstub.__main__", "-l", ":9001", "script"
        ])
        pass
