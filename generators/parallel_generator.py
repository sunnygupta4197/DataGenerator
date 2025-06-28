from typing import Any, Dict, Iterator, List
import time
from .base_generator import BaseDataGenerator
import os


class ParallelDataGenerator(BaseDataGenerator):
    """Specialized generator for parallel processing"""

    def __init__(self, config, logger, parallel_generator):
        super().__init__(config, logger)
        self.parallel_generator = parallel_generator

    def should_use_strategy(self, table_metadata: Dict[str, Any],
                            total_records: int) -> bool:
        """Use parallel for medium-sized datasets"""
        return (self.config.performance.enable_parallel and
                5000 < total_records <= 50000)

    def get_strategy_name(self) -> str:
        return "Parallel"

    def generate(self, table_metadata: Dict[str, Any],
                 total_records: int,
                 foreign_key_data: Dict[str, List] = None) -> Iterator[List[Dict]]:
        """Generate data using parallel approach"""
        start_time = time.time()

        self.logger.info(f"⚡ Using PARALLEL generation for {total_records:,} records")

        cpu_count = os.cpu_count()
        use_processes = (total_records > 50000 and cpu_count > 2 and
                         self.config.performance.max_workers > 2)

        data = self.parallel_generator.generate_parallel(
            table_metadata=table_metadata,
            total_records=total_records,
            foreign_key_data=foreign_key_data or {},
            use_processes=use_processes
        )

        # Update metrics
        self.metrics.records_generated = len(data)
        self.metrics.processing_time = time.time() - start_time

        yield data
