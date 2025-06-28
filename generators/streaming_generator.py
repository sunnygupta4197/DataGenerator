from typing import List, Dict, Any, Iterator
from .base_generator import BaseDataGenerator
import time


class StreamingDataGenerator(BaseDataGenerator):
    """Specialized generator for large datasets using streaming"""

    def __init__(self, config, logger, parallel_generator):
        super().__init__(config, logger)
        self.parallel_generator = parallel_generator
        self.batch_size = config.performance.batch_size or 10000

    def should_use_strategy(self, table_metadata: Dict[str, Any],
                            total_records: int) -> bool:
        """Use streaming for large datasets"""
        return (self.config.performance.enable_streaming and
                total_records > 50000)

    def get_strategy_name(self) -> str:
        return "Streaming"

    def generate(self, table_metadata: Dict[str, Any],
                 total_records: int,
                 foreign_key_data: Dict[str, List] = None) -> Iterator[List[Dict]]:
        """Generate data using streaming approach"""
        start_time = time.time()
        total_generated = 0

        self.logger.info(f"📊 Using STREAMING generation for {total_records:,} records")

        for batch in self.parallel_generator.generate_streaming(
                table_metadata=table_metadata,
                total_records=total_records,
                foreign_key_data=foreign_key_data or {}
        ):
            yield batch
            total_generated += len(batch)

            # Update metrics
            self.metrics.records_generated = total_generated
            self.metrics.processing_time = time.time() - start_time

