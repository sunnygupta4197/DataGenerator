import logging
from typing import Dict, List, Any
import pandas as pd


class BusinessRulesEngine:
    """High-performance business rules engine"""

    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self._rules_cache = {}

    def generate_default_business_rules(self, table_metadata: Dict[str, Any]) -> List[Dict]:
        """Generate default business rules based on table structure"""
        table_name = table_metadata.get("table_name", "unknown")
        columns = {col['name']: col for col in table_metadata.get('columns', [])}

        # Use cache key
        cache_key = f"{table_name}_{len(columns)}"
        if cache_key in self._rules_cache:
            return self._rules_cache[cache_key]

        rules = []

        # Age-based rules
        if 'age' in columns and 'status' in columns:
            rules.append({
                'type': 'conditional',
                'condition_column': 'age',
                'condition_operator': '<',
                'condition_value': 18,
                'requirement_column': 'status',
                'requirement_value': 'MINOR',
                'description': 'Minors must have MINOR status'
            })

        # Income-credit score correlation
        if 'income' in columns and 'credit_score' in columns:
            rules.append({
                'type': 'range_dependency',
                'income_column': 'income',
                'score_column': 'credit_score',
                'income_threshold': 100000,
                'score_threshold': 700,
                'description': 'High income should correlate with good credit'
            })

        # Email validation
        if 'email' in columns:
            rules.append({
                'type': 'format_validation',
                'column': 'email',
                'pattern': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
                'description': 'Email must be valid format'
            })

        # Phone validation
        if 'phone' in columns:
            rules.append({
                'type': 'format_validation',
                'column': 'phone',
                'pattern': r'^\+?1?[-.\s]?\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}$',
                'description': 'Phone must be valid US format'
            })

        self._rules_cache[cache_key] = rules
        return rules

    def validate_business_rules(self, data: List[Dict], rules: List[Dict]) -> Dict[str, Any]:
        """Validate business rules against data"""
        if not data or not rules:
            return {'total_violations': 0, 'compliance_rate': 1.0, 'violations': []}

        violations = []
        total_checks = 0

        # Convert to DataFrame for vectorized operations
        df = pd.DataFrame(data)

        for rule in rules:
            rule_type = rule.get('type')
            total_checks += len(data)

            if rule_type == 'conditional':
                violations.extend(self._validate_conditional_rule(df, rule))
            elif rule_type == 'range_dependency':
                violations.extend(self._validate_range_dependency(df, rule))
            elif rule_type == 'format_validation':
                violations.extend(self._validate_format_rule(df, rule))
            elif rule_type == 'mutual_exclusivity':
                violations.extend(self._validate_mutual_exclusivity_rule(df, rule))

        compliance_rate = 1.0 - (len(violations) / total_checks) if total_checks > 0 else 1.0

        print(violations)

        return {
            'total_violations': len(violations),
            'compliance_rate': compliance_rate,
            'violations': violations[:10],  # Limit for performance
            'rules_checked': len(rules)
        }

    def _validate_conditional_rule(self, df: pd.DataFrame, rule: Dict) -> List[Dict]:
        """Validate conditional rules using vectorization"""
        try:
            condition_col = rule['condition_column']
            requirement_col = rule['requirement_column']

            if condition_col not in df.columns or requirement_col not in df.columns:
                return []

            condition_op = rule['condition_operator']
            condition_val = rule['condition_value']
            requirement_val = rule['requirement_value']

            # Vectorized condition check
            if condition_op == '<':
                mask = df[condition_col] < condition_val
            elif condition_op == '>':
                mask = df[condition_col] > condition_val
            elif condition_op == '==':
                mask = df[condition_col] == condition_val
            else:
                return []

            # Check violations
            violations_mask = mask & (df[requirement_col] != requirement_val)
            violation_indices = df[violations_mask].index.tolist()

            return [{'rule_description': rule.get('description', 'Conditional rule violation'),
                     'row_index': idx} for idx in violation_indices[:5]]
        except Exception:
            return []

    def _validate_range_dependency(self, df: pd.DataFrame, rule: Dict) -> List[Dict]:
        """Validate range dependency rules"""
        try:
            income_col = rule['income_column']
            score_col = rule['score_column']

            if income_col not in df.columns or score_col not in df.columns:
                return []

            income_threshold = rule['income_threshold']
            score_threshold = rule['score_threshold']

            # Vectorized check
            high_income_mask = df[income_col] >= income_threshold
            low_score_mask = df[score_col] < score_threshold
            violations_mask = high_income_mask & low_score_mask

            violation_indices = df[violations_mask].index.tolist()

            return [{'rule_description': rule.get('description', 'Range dependency violation'),
                     'row_index': idx} for idx in violation_indices[:5]]
        except Exception:
            return []

    def _validate_format_rule(self, df: pd.DataFrame, rule: Dict) -> List[Dict]:
        """Validate format rules using regex"""
        try:
            column = rule['column']
            pattern = rule['pattern']

            if column not in df.columns:
                return []

            # Vectorized regex validation
            valid_mask = df[column].astype(str).str.match(pattern, na=False)
            violations_mask = ~valid_mask

            violation_indices = df[violations_mask].index.tolist()

            return [{'rule_description': rule.get('description', 'Format validation violation'),
                     'row_index': idx} for idx in violation_indices[:5]]
        except Exception:
            return []

    def _validate_mutual_exclusivity_rule(self, df: pd.DataFrame, rule: Dict) -> List[Dict]:
        try:
            column1 = rule.get('column1')
            column2 = rule.get('column2')
            value1 = rule.get('value1')
            value2 = rule.get('value2')

            if column1 not in df.columns or column2 not in df.columns:
                return []

            violations_mask = (df[column1] == value1) & (df[column2] == value2)
            violation_indices = df[violations_mask].index.tolist()
            return [{'rule_description': rule.get('description', 'Mutual exclusivity violation'),
                     'row_index': idx} for idx in violation_indices[:5]]
        except Exception:
            return []
