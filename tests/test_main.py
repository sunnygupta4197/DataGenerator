import os
import unittest
import logging
from datetime import datetime

from config_manager.config_manager import ConfigurationManager
from main import OptimizedDataGenerationOrchestrator


class TestOptimizedMain(unittest.TestCase):
    def setUp(self):
        self.logger = logging.getLogger("TestLogger")
        self.logger.setLevel(logging.INFO)
        self.rows = 10
        self.enable_all_features = True
        self.start_date_time = datetime.now().strftime("%Y%m%d%H%M%S%f")
        self.example_files = [
            "example1.json",
            "example2.json",
            "example3.json",
            "example4.json",
            "example5.json"
        ]

    def _load_and_run(self, config_path):
        config_file_name, configfile_ext = os.path.splitext(os.path.basename(config_path))
        config = ConfigurationManager().load_configuration(
            config_path,
            enable_all_features=self.enable_all_features,
            rows=self.rows,
            output_format='csv',
            output_dir=f'output/{self.start_date_time}/{config_file_name}'
        )
        orchestrator = OptimizedDataGenerationOrchestrator(config, self.logger)
        data = orchestrator.run_data_generation(config.rows)
        return config, data

    def test_all_examples(self):
        for example in self.example_files:
            print("Running example {}".format(example))
            with self.subTest(example=example):
                path = os.path.join(os.path.dirname(__file__), "../examples", example)
                config, data = self._load_and_run(path)
                self.assertIsInstance(data, dict)
                self.assertGreater(len(data), 0)
                table = list(data.keys())
                self.assertEqual(len(table), len(config.tables))
                for table, table_data in data.items():
                    self.assertEqual(len(table_data), self.rows)
            print("Example {} completed".format(example))
        print("All examples passed")


if __name__ == "__main__":
    unittest.main()
