from typing import Any, Dict
from .base_generator import BaseDataGenerator
from .streaming_generator import StreamingDataGenerator
from .parallel_generator import ParallelDataGenerator
from .adaptive_generator import AdaptiveDataGenerator


class GeneratorFactory:
    """Factory for creating and selecting appropriate generators"""

    def __init__(self, config, logger, parallel_generator):
        self.config = config
        self.logger = logger
        self.generators = [
            StreamingDataGenerator(config, logger, parallel_generator),
            ParallelDataGenerator(config, logger, parallel_generator),
            AdaptiveDataGenerator(config, logger, parallel_generator)
        ]

    def select_generator(self, table_metadata: Dict[str, Any],
                         total_records: int) -> BaseDataGenerator:
        """Select the best generator for the given parameters"""

        for generator in self.generators:
            if generator.should_use_strategy(table_metadata, total_records):
                self.logger.info(f"Selected {generator.get_strategy_name()} generator")
                return generator

        # Fallback to adaptive
        return self.generators[-1]  # AdaptiveDataGenerator
