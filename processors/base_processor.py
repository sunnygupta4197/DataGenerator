from abc import ABC, abstractmethod
from typing import Dict, List, Any, Tuple


class BaseDataProcessor(ABC):
    """Base class for data processors"""

    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        self.enabled = self._is_enabled()
        self.batch_interval = self.set_batch_interval()

    @abstractmethod
    def _is_enabled(self) -> bool:
        """Check if this processor is enabled"""
        pass

    @abstractmethod
    def set_batch_interval(self) -> int:
        pass

    @abstractmethod
    def process(self, batch: List[Dict], table_metadata: Dict[str, Any],
                batch_count: int) -> Tuple[List[Dict], Dict[str, Any]]:
        """Process a batch of data"""
        pass

    @abstractmethod
    def get_processor_name(self) -> str:
        """Get processor name for logging"""
        pass

