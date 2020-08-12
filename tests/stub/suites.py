"""
Defines stub suites
"""

import unittest
import tests.stub.retry as retry
from tests.shared import get_driver_name

loader = unittest.TestLoader()

protocol4x0 = unittest.TestSuite()

# Support for transactional functions not implemented in dotnet
if not get_driver_name() in ['dotnet']:
    protocol4x0.addTests(loader.loadTestsFromModule(retry))
