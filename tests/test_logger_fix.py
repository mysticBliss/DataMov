import unittest
from datamov.core.logger.Logger import Logger

class TestLoggerFix(unittest.TestCase):
    def setUp(self):
        # Save original instance
        self.original_instance = Logger._instance
        Logger._instance = None

    def tearDown(self):
        # Restore original instance
        Logger._instance = self.original_instance

    def test_add_config_handles_missing_initialization(self):
        # Create instance without calling __init__
        logger = Logger.__new__(Logger)

        # Verify config is missing (it should be, as __init__ wasn't called)
        self.assertFalse(hasattr(logger, 'config'))

        # This should NOT raise AttributeError, but initialize config
        # Currently, it will raise AttributeError, so we expect this to fail initially
        logger.add_config("key", "value")

        # Verify config was created and set
        self.assertTrue(hasattr(logger, 'config'))
        self.assertEqual(logger.config["key"], "value")

    def test_init_preserves_existing_config(self):
        # Create instance without calling __init__
        logger = Logger.__new__(Logger)

        # Manually initialize config
        logger.config = {"pre_existing": "true"}

        # Now call __init__ (simulating normal initialization)
        logger.__init__()

        # Verify existing config is preserved
        self.assertIn("pre_existing", logger.config)
        self.assertEqual(logger.config["pre_existing"], "true")

if __name__ == "__main__":
    unittest.main()
