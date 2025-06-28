from typing import List, Dict, Tuple, Any
from .base_processor import BaseDataProcessor


class BusinessRulesProcessor(BaseDataProcessor):
    """Processor for business rules validation"""

    def __init__(self, config, logger, business_rules_engine):
        super().__init__(config, logger)
        self.business_rules_engine = business_rules_engine
        self._rules_cache = {}

    def set_batch_interval(self):
        return 30

    def _is_enabled(self) -> bool:
        return getattr(self.config.validation, 'enable_business_rules', False)

    def get_processor_name(self) -> str:
        return "Business Rules"

    def process(self, batch: List[Dict], table_metadata: Dict[str, Any],
                batch_count: int) -> Tuple[List[Dict], Dict[str, Any]]:
        """Validate business rules"""
        metrics = {'business_rules_violations': 0}

        try:
            # Get cached rules
            table_name = table_metadata.get('table_name', 'unknown')
            rules = self._rules_cache.get(table_name, [])

            if not rules:
                return batch, metrics

            # Validate sample
            sample = batch[:min(25, len(batch))]
            violations = self.business_rules_engine.validate_business_rules(sample, rules)
            metrics['business_rules_violations'] = violations.get('total_violations', 0)

            if metrics['business_rules_violations'] > 0:
                compliance_rate = violations.get('compliance_rate', 1.0)
                self.logger.warning(
                    f"Batch {batch_count} rule violations: {metrics['business_rules_violations']} "
                    f"(Compliance: {compliance_rate:.1%})"
                )

        except Exception as e:
            self.logger.warning(f"Business rules validation failed for batch {batch_count}: {e}")

        return batch, metrics
