"""
Defines stub suites
"""

import unittest
import tests.stub.retry as retry
from tests.shared import get_driver_name

loader = unittest.TestLoader()

protocol4x0 = unittest.TestSuite()
protocol4x0.addTests(loader.loadTestsFromModule(retry))
