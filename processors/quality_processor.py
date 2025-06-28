from typing import List, Dict, Any, Tuple
from .base_processor import BaseDataProcessor


class QualityAnalysisProcessor(BaseDataProcessor):
    """Processor for data quality analysis"""

    def __init__(self, config, logger, quality_analyzer):
        super().__init__(config, logger)
        self.quality_analyzer = quality_analyzer

    def set_batch_interval(self):
        return 25

    def _is_enabled(self) -> bool:
        return self.config.validation.enable_data_quality_analysis

    def get_processor_name(self) -> str:
        return "Quality Analysis"

    def process(self, batch: List[Dict], table_metadata: Dict[str, Any],
                batch_count: int) -> Tuple[List[Dict], Dict[str, Any]]:
        """Analyze data quality"""
        metrics = {}

        try:
            quality_analysis = self.quality_analyzer.analyze_distribution(
                batch[:min(50, len(batch))], table_metadata
            )
            if quality_analysis:
                metrics['quality_score'] = quality_analysis.get('data_quality_score', 0)
                self.logger.debug(f"Quality score for batch {batch_count}: {metrics['quality_score']:.3f}")
        except Exception as e:
            self.logger.warning(f"Quality analysis failed for batch {batch_count}: {e}")

        return batch, metrics

