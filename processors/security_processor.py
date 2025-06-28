from typing import List, Dict, Any, Tuple
from .base_processor import BaseDataProcessor


class SecurityProcessor(BaseDataProcessor):
    """Enhanced processor for comprehensive security operations (masking, encryption, auditing)"""

    def __init__(self, config, logger, security_manager):
        super().__init__(config, logger)
        self.security_manager = security_manager
        self._sensitivity_cache = {}
        self._initialize_security_manager()

    def set_batch_interval(self):
        return -1

    def _initialize_security_manager(self):
        """Initialize security manager with configuration"""
        # Enable masking
        self.security_manager.enable_masking = self.config.security.enable_data_masking

        # Set encryption key if provided
        if hasattr(self.config.security, 'encryption_key') and self.config.security.encryption_key:
            self.security_manager.set_encryption_key(self.config.security.encryption_key.encode())

        # Add custom masking rules from config
        if hasattr(self.config.security, 'masking_rules'):
            for pattern, masking_type in self.config.security.masking_rules.items():
                self.security_manager.add_masking_rule(pattern, masking_type)

    def _is_enabled(self) -> bool:
        return (self.config.security.enable_data_masking or
                getattr(self.config.security, 'enable_encryption', False) or
                getattr(self.config.security, 'enable_auditing', False))

    def get_processor_name(self) -> str:
        return "Security/Masking/Encryption"

    def process(self, batch: List[Dict], table_metadata: Dict[str, Any],
                batch_count: int) -> Tuple[List[Dict], Dict[str, Any]]:
        """Apply comprehensive security processing"""
        metrics = {
            'security_operations': 0,
            'masked_fields': 0,
            'encrypted_fields': 0,
            'audit_records': 0,
            'compliance_violations': 0
        }

        if not self.enabled or not batch:
            return batch, metrics

        try:
            processed_batch = batch.copy()

            # Build sensitivity map (cached)
            sensitivity_map = self._build_sensitivity_map_cached(table_metadata)

            # 1. Apply data masking
            if self.config.security.enable_data_masking:
                processed_batch, masking_metrics = self._apply_masking(
                    processed_batch, sensitivity_map, batch_count
                )
                metrics.update(masking_metrics)

            # 2. Apply encryption to highly sensitive fields
            if getattr(self.config.security, 'enable_encryption', False):
                processed_batch, encryption_metrics = self._apply_encryption(
                    processed_batch, sensitivity_map, batch_count
                )
                metrics.update(encryption_metrics)

            # 3. Validate compliance if enabled
            if getattr(self.config.security, 'enable_compliance_check', False):
                compliance_metrics = self._validate_compliance(
                    processed_batch, table_metadata, batch_count
                )
                metrics.update(compliance_metrics)

            # 4. Create audit trail
            if getattr(self.config.security, 'audit_enabled', False):
                audit_metrics = self._create_audit_record(
                    table_metadata, len(processed_batch), sensitivity_map
                )
                metrics.update(audit_metrics)

            self.logger.debug(f"Security processing completed for batch {batch_count}: {metrics}")
            return processed_batch, metrics

        except Exception as e:
            self.logger.warning(f"Security processing failed for batch {batch_count}: {e}")
            return batch, metrics

    def _apply_masking(self, batch: List[Dict], sensitivity_map: Dict[str, str],
                       batch_count: int) -> Tuple[List[Dict], Dict[str, Any]]:
        """Apply data masking using SecurityManager"""
        sensitive_levels = ['PII', 'SENSITIVE']
        if not any(level in sensitive_levels for level in sensitivity_map.values()):
            return batch, {'security_operations': 0, 'masked_fields': 0}

        masked_batch = self.security_manager.mask_sensitive_data(batch, sensitivity_map)

        # Count masked fields
        sensitive_columns = [col for col, level in sensitivity_map.items()
                             if level in sensitive_levels]
        masked_operations = len(batch) * len(sensitive_columns)

        return masked_batch, {
            'security_operations': masked_operations,
            'masked_fields': masked_operations
        }

    def _apply_encryption(self, batch: List[Dict], sensitivity_map: Dict[str, str],
                          batch_count: int) -> Tuple[List[Dict], Dict[str, Any]]:
        """Apply encryption to PII fields"""
        pii_fields = [col for col, level in sensitivity_map.items() if level == 'PII']

        if not pii_fields:
            return batch, {'encrypted_fields': 0}

        encrypted_batch = self.security_manager.encrypt_sensitive_fields(batch, pii_fields)
        encrypted_operations = len(batch) * len(pii_fields)

        self.logger.debug(f"Encrypted {len(pii_fields)} PII fields in batch {batch_count}")

        return encrypted_batch, {'encrypted_fields': encrypted_operations}

    def _validate_compliance(self, batch: List[Dict], table_metadata: Dict[str, Any],
                             batch_count: int) -> Dict[str, Any]:
        """Validate data compliance using SecurityManager"""
        compliance_rules = getattr(self.config.security, 'compliance_rules', {})

        if not compliance_rules:
            return {'compliance_violations': 0}

        compliance_report = self.security_manager.validate_compliance(batch, compliance_rules)
        violation_count = len(compliance_report.get('violations', []))

        if violation_count > 0:
            self.logger.warning(f"Compliance violations found in batch {batch_count}: {violation_count}")

        return {'compliance_violations': violation_count}

    def _create_audit_record(self, table_metadata: Dict[str, Any], record_count: int,
                             sensitivity_map: Dict[str, str]) -> Dict[str, Any]:
        """Create audit trail record"""
        generation_params = {
            'table_name': table_metadata.get('table_name', 'unknown'),
            'processor': self.get_processor_name(),
            'security_enabled': True
        }

        sensitive_columns = [col for col, level in sensitivity_map.items()
                             if level in ['PII', 'SENSITIVE']]

        self.security_manager.audit_data_generation(
            generation_params, record_count, sensitive_columns
        )

        return {'audit_records': 1}

    def _build_sensitivity_map_cached(self, table_metadata: Dict[str, Any]) -> Dict[str, str]:
        """Build sensitivity map with caching"""
        table_name = table_metadata.get("table_name", "unknown")

        if table_name in self._sensitivity_cache:
            return self._sensitivity_cache[table_name]

        sensitivity_map = {}
        for column in table_metadata.get('columns', []):
            col_name = column.get('name', '')
            sensitivity_level = column.get('sensitivity', 'PUBLIC')

            # Auto-detect sensitivity if enabled
            if (sensitivity_level == 'PUBLIC' and
                    self.config.security.auto_detect_pii):
                col_name_lower = col_name.lower()
                if any(keyword in col_name_lower for keyword in
                       ['email', 'phone', 'ssn', 'credit']):
                    sensitivity_level = 'PII'
                elif any(keyword in col_name_lower for keyword in
                         ['income', 'salary', 'address']):
                    sensitivity_level = 'SENSITIVE'

            sensitivity_map[col_name] = sensitivity_level

        self._sensitivity_cache[table_name] = sensitivity_map
        return sensitivity_map

    def export_audit_trail(self, output_path: str = None) -> str:
        """Export security audit trail"""
        return self.security_manager.export_audit_trail(output_path)

    def get_compliance_report(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Get compliance validation report"""
        compliance_rules = getattr(self.config.security, 'compliance_rules', {})
        return self.security_manager.validate_compliance(data, compliance_rules)