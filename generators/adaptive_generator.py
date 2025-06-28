from typing import Any, Dict, List, Iterator
import time
from .base_generator import BaseDataGenerator


class AdaptiveDataGenerator(BaseDataGenerator):
    """Adaptive generator for small to medium datasets"""

    def __init__(self, config, logger, parallel_generator):
        super().__init__(config, logger)
        self.parallel_generator = parallel_generator

    def should_use_strategy(self, table_metadata: Dict[str, Any],
                            total_records: int) -> bool:
        """Use adaptive for smaller datasets"""
        return total_records <= 5000

    def get_strategy_name(self) -> str:
        return "Adaptive"

    def generate(self, table_metadata: Dict[str, Any],
                 total_records: int,
                 foreign_key_data: Dict[str, List] = None) -> Iterator[List[Dict]]:
        """Generate data using adaptive approach"""
        start_time = time.time()

        self.logger.info(f"🤖 Using ADAPTIVE generation for {total_records:,} records")

        for batch in self.parallel_generator.generate_adaptive(
                table_metadata=table_metadata,
                total_records=total_records,
                foreign_key_data=foreign_key_data or {}
        ):
            if isinstance(batch, list) and len(batch) > 0:
                yield batch

        # Update metrics
        self.metrics.processing_time = time.time() - start_time
