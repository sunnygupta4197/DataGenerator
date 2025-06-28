from abc import ABC, abstractmethod
from typing import Dict, List, Any, Iterator, Tuple
import logging
from dataclasses import dataclass


@dataclass
class GenerationMetrics:
    """Metrics collected during generation"""
    records_generated: int = 0
    processing_time: float = 0.0
    memory_usage_mb: float = 0.0
    quality_score: float = 0.0
    validation_errors: int = 0
    security_operations: int = 0


class BaseDataGenerator(ABC):
    """Abstract base class for all data generators"""

    def __init__(self, config, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.metrics = GenerationMetrics()

    @abstractmethod
    def generate(self, table_metadata: Dict[str, Any],
                 total_records: int,
                 foreign_key_data: Dict[str, List] = None) -> Iterator[List[Dict]]:
        """Generate data in batches"""
        pass

    @abstractmethod
    def get_strategy_name(self) -> str:
        """Return the strategy name for logging"""
        pass

    def should_use_strategy(self, table_metadata: Dict[str, Any],
                            total_records: int) -> bool:
        """Determine if this strategy should be used"""
        return True

    def get_metrics(self) -> GenerationMetrics:
        """Get generation metrics"""
        return self.metrics

