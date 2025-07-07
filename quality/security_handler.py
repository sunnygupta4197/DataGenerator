import os
import logging
import time
from typing import Dict, List, Any, Optional
import pandas as pd
import json
import socket


class SecurityManager:
    """
    Comprehensive security management for sensitive data
    """

    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.encryption_key = None
        self.masking_rules = {}
        self.enable_masking = False
        self.audit_trail = []

    def set_encryption_key(self, key: bytes):
        """Set encryption key for sensitive data"""
        self.encryption_key = key

    def add_masking_rule(self, column_pattern: str, masking_type: str):
        """Add data masking rule"""
        self.masking_rules[column_pattern] = masking_type

    def mask_sensitive_data(self, data: List[Dict[str, Any]],
                            sensitivity_map: Dict[str, str]) -> List[Dict[str, Any]]:
        """Apply data masking rules to sensitive data"""
        if not self.enable_masking:
            return data

        masked_data = []
        masked_fields_count = {}

        for row in data:
            masked_row = {}
            for column, value in row.items():
                sensitivity_level = sensitivity_map.get(column, 'PUBLIC')

                if sensitivity_level in ['PII', 'SENSITIVE']:
                    masked_row[column] = self._mask_value(value, column)
                    masked_fields_count[column] = masked_fields_count.get(column, 0) + 1
                else:
                    masked_row[column] = value

            masked_data.append(masked_row)

        # Log masking activity
        if masked_fields_count:
            self.logger.info(f"Data masking applied: {masked_fields_count}")

        return masked_data

    def _mask_value(self, value: Any, column: str) -> Any:
        """Apply masking to individual value"""
        if value is None:
            return value

        str_value = str(value)

        # Apply custom masking rules first
        for pattern, masking_type in self.masking_rules.items():
            if pattern.lower() in column.lower():
                return self._apply_custom_masking(str_value, masking_type)

        # Default masking rules
        if '@' in str_value:
            return self._mask_email(str_value)
        elif str_value.replace('-', '').replace(' ', '').replace('(', '').replace(')', '').isdigit() and len(
                str_value) >= 10:
            return self._mask_phone(str_value)
        elif 'name' in column.lower():
            return self._mask_name(str_value)
        elif 'ssn' in column.lower() or 'social' in column.lower():
            return self._mask_ssn(str_value)
        elif 'card' in column.lower() or 'credit' in column.lower():
            return self._mask_credit_card(str_value)
        else:
            return self._mask_generic(str_value)

    def _apply_custom_masking(self, value: str, masking_type: str) -> str:
        """Apply custom masking based on type"""
        if masking_type == 'full':
            return '*' * len(value)
        elif masking_type == 'partial':
            return self._mask_generic(value)
        elif masking_type == 'hash':
            import hashlib
            return hashlib.sha256(value.encode()).hexdigest()[:8]
        else:
            return self._mask_generic(value)

    def _mask_email(self, email: str) -> str:
        """Mask email address"""
        if '@' not in email:
            return email

        local, domain = email.split('@', 1)
        if len(local) > 2:
            masked_local = local[0] + '*' * (len(local) - 2) + local[-1]
        else:
            masked_local = '*' * len(local)

        return f"{masked_local}@{domain}"

    def _mask_phone(self, phone: str) -> str:
        """Mask phone number"""
        digits = ''.join(c for c in phone if c.isdigit())
        if len(digits) >= 10:
            masked = digits[:3] + '*' * (len(digits) - 6) + digits[-3:]
            return phone.replace(digits, masked)
        return phone

    def _mask_name(self, name: str) -> str:
        """Mask name"""
        if len(name) <= 1:
            return name
        return name[0] + '*' * (len(name) - 1)

    def _mask_ssn(self, ssn: str) -> str:
        """Mask SSN"""
        digits = ''.join(c for c in ssn if c.isdigit())
        if len(digits) == 9:
            return f"***-**-{digits[-4:]}"
        return '*' * len(ssn)

    def _mask_credit_card(self, card: str) -> str:
        """Mask credit card number"""
        digits = ''.join(c for c in card if c.isdigit())
        if len(digits) >= 13:
            return f"****-****-****-{digits[-4:]}"
        return '*' * len(card)

    def _mask_generic(self, value: str) -> str:
        """Generic masking"""
        if len(value) <= 2:
            return '*' * len(value)
        return value[0] + '*' * (len(value) - 2) + value[-1]

    def encrypt_sensitive_fields(self, data: List[Dict[str, Any]],
                                 sensitive_fields: List[str]) -> List[Dict[str, Any]]:
        """Encrypt sensitive data fields"""
        if not self.encryption_key:
            self.logger.warning("No encryption key set, skipping encryption")
            return data

        encrypted_data = []
        encryption_count = 0

        for row in data:
            encrypted_row = {}
            for column, value in row.items():
                if column in sensitive_fields:
                    encrypted_row[column] = self._encrypt_value(value)
                    encryption_count += 1
                else:
                    encrypted_row[column] = value

            encrypted_data.append(encrypted_row)

        self.logger.info(f"Encrypted {encryption_count} field values")
        return encrypted_data

    def _encrypt_value(self, value: Any) -> Optional[str]:
        """Encrypt individual value"""
        if value is None:
            return value

        try:
            # In production, use proper encryption libraries like cryptography
            # This is a simplified example using base64 (NOT secure!)
            import base64
            import hashlib

            str_value = str(value)

            # Create a simple "encryption" (this is NOT secure!)
            # In production, use proper AES encryption
            key_hash = hashlib.sha256(self.encryption_key).digest()[:16]

            # Simple XOR encryption (demonstration only)
            encrypted_bytes = bytes([ord(c) ^ key_hash[i % len(key_hash)] for i, c in enumerate(str_value)])
            encoded = base64.b64encode(encrypted_bytes).decode()

            return f"ENC:{encoded}"

        except Exception as e:
            self.logger.error(f"Encryption failed: {e}")
            return f"ENC_ERROR:{str(value)[:8]}***"

    def audit_data_generation(self, generation_params: Dict[str, Any],
                              records_count: int,
                              sensitive_columns: List[str]) -> Dict[str, Any]:
        """Create audit trail for data generation"""
        from datetime import datetime
        import uuid
        import getpass
        username = 'system'
        try:
            username = os.getenv('USER', os.getlogin())
        except Exception:
            username = getpass.getuser()

        audit_record = {
            'audit_id': str(uuid.uuid4()),
            'timestamp': datetime.now().isoformat(),
            'event_type': 'data_generation',
            'generation_params': generation_params,
            'records_generated': records_count,
            'sensitive_columns': sensitive_columns,
            'user': username,
            'hostname': os.getenv('HOSTNAME', socket.gethostname()),
            'pid': os.getpid(),
            'compliance_level': self._assess_compliance_level(generation_params),
            'security_measures_applied': {
                'masking_enabled': self.enable_masking,
                'encryption_enabled': bool(self.encryption_key),
                'custom_rules_count': len(self.masking_rules)
            }
        }

        # Store in audit trail
        self.audit_trail.append(audit_record)

        # Log audit record
        self.logger.info(f"Audit record created: {audit_record['audit_id']}")

        return audit_record

    def _assess_compliance_level(self, params: Dict[str, Any]) -> str:
        """Assess compliance level of generation parameters"""
        risk_indicators = []

        # Check for PII-related terms
        pii_terms = ['pii', 'personal', 'ssn', 'social', 'credit', 'email', 'phone', 'address']
        sensitive_terms = ['sensitive', 'confidential', 'private', 'restricted']

        for key, value in params.items():
            str_value = str(value).lower()
            if any(term in str_value for term in pii_terms):
                risk_indicators.append('PII_DETECTED')
            elif any(term in str_value for term in sensitive_terms):
                risk_indicators.append('SENSITIVE_DETECTED')

        # Assess overall risk
        if 'PII_DETECTED' in risk_indicators:
            return 'HIGH_RISK'
        elif 'SENSITIVE_DETECTED' in risk_indicators:
            return 'MEDIUM_RISK'
        else:
            return 'LOW_RISK'

    def export_audit_trail(self, output_path: str = None) -> str:
        """Export audit trail to file"""
        if not output_path:
            from datetime import datetime
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = f"./audit_trail_{timestamp}.json"

        try:
            with open(output_path, 'w') as f:
                json.dump(self.audit_trail, f, indent=2, default=str)

            self.logger.info(f"Audit trail exported to: {output_path}")
            return output_path

        except Exception as e:
            self.logger.error(f"Failed to export audit trail: {e}")
            raise

    def validate_compliance(self, data: List[Dict[str, Any]],
                            compliance_rules: Dict[str, Any]) -> Dict[str, Any]:
        """Validate data against compliance rules"""
        compliance_report = {
            'timestamp': time.time(),
            'total_records': len(data),
            'violations': [],
            'compliance_score': 1.0,
            'rules_checked': len(compliance_rules)
        }

        if not data:
            return compliance_report

        df = pd.DataFrame(data)

        for rule_name, rule_config in compliance_rules.items():
            try:
                violations = self._check_compliance_rule(df, rule_name, rule_config)
                compliance_report['violations'].extend(violations)
            except Exception as e:
                self.logger.error(f"Error checking compliance rule {rule_name}: {e}")

        # Calculate compliance score
        if compliance_report['violations']:
            violation_rate = len(compliance_report['violations']) / len(data)
            compliance_report['compliance_score'] = max(0.0, 1.0 - violation_rate)

        return compliance_report

    def _check_compliance_rule(self, df: pd.DataFrame, rule_name: str,
                               rule_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Check individual compliance rule"""
        violations = []
        rule_type = rule_config.get('type')

        if rule_type == 'no_real_pii':
            # Check that no real PII patterns exist
            pii_patterns = rule_config.get('patterns', [])
            for pattern in pii_patterns:
                for column in df.columns:
                    if df[column].dtype == 'object':  # String columns
                        matches = df[column].str.contains(pattern, regex=True, na=False)
                        if matches.any():
                            for idx in df[matches].index:
                                violations.append({
                                    'rule': rule_name,
                                    'type': 'real_pii_detected',
                                    'column': column,
                                    'row_index': idx,
                                    'pattern': pattern
                                })

        elif rule_type == 'data_minimization':
            # Check that only necessary columns are present
            allowed_columns = set(rule_config.get('allowed_columns', []))
            actual_columns = set(df.columns)
            unauthorized_columns = actual_columns - allowed_columns

            if unauthorized_columns:
                violations.append({
                    'rule': rule_name,
                    'type': 'unauthorized_columns',
                    'columns': list(unauthorized_columns)
                })

        elif rule_type == 'anonymization_check':
            # Check that sensitive data is properly anonymized
            sensitive_columns = rule_config.get('sensitive_columns', [])
            for column in sensitive_columns:
                if column in df.columns:
                    # Check for patterns that suggest real data
                    real_patterns = rule_config.get('real_data_patterns', [])
                    for pattern in real_patterns:
                        matches = df[column].astype(str).str.contains(pattern, regex=True, na=False)
                        if matches.any():
                            violations.append({
                                'rule': rule_name,
                                'type': 'insufficient_anonymization',
                                'column': column,
                                'pattern': pattern,
                                'match_count': matches.sum()
                            })

        return violations