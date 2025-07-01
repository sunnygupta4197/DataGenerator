import os
import unittest
import logging
from config_manager.config_manager import ConfigurationManager
from optimized_main import OptimizedDataGenerationOrchestrator


class TestOptimizedMain(unittest.TestCase):
    def setUp(self):
        self.logger = logging.getLogger("TestLogger")
        self.logger.setLevel(logging.INFO)
        self.example_files = [
            "example1.json",
            "example2.json",
            "example3.json",
            "example4.json",
            "example5.json"
        ]

    def _load_and_run(self, config_path):
        config = ConfigurationManager().load_configuration(config_path, enable_all_features=True, rows=10)
        orchestrator = OptimizedDataGenerationOrchestrator(config, self.logger)
        data = orchestrator.run_data_generation(config.rows)
        return data

    def test_all_examples(self):
        for example in self.example_files:
            with self.subTest(example=example):
                path = os.path.join("./examples", example)
                path = os.path.join("../examples", example)
                data = self._load_and_run(path)
                self.assertIsInstance(data, dict)
                self.assertGreater(len(data), 0)


if __name__ == "__main__":
    unittest.main()
