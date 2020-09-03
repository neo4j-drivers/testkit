"""
Defines stub suites
"""

import unittest
import tests.stub.retry as retry

loader = unittest.TestLoader()

stub_suite = unittest.TestSuite()
stub_suite.addTests(loader.loadTestsFromModule(retry))
