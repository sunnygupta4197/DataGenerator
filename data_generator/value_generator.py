import random
import json
import io
import sys
import rstr
import csv
from faker import Faker
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Any, Optional
from enum import Enum

from validators.unified_validation_system import UnifiedValidator
from .ai_providers import AIProviderManager


class ValueGenerator:
    """Enhanced ValueGenerator with configurable AI integration using modular providers"""

    def __init__(self, faker: Faker, logger: logging.Logger, ai_config=None):
        self.faker = faker
        self.logger = logger
        self.ai_config = ai_config

        # Initialize AI Provider Manager
        self.ai_manager = None
        if ai_config:
            try:
                self.ai_manager = AIProviderManager(logger, ai_config)
                if self.ai_manager.is_available():
                    self.logger.info("AI Provider Manager initialized successfully")
                else:
                    self.logger.warning("No AI providers available")
                    self.ai_manager = None
            except Exception as e:
                self.logger.error(f"Failed to initialize AI Provider Manager: {e}")
                self.ai_manager = None

    def generate_by_rule(self, rule: Any, data_type: str, column_name: str = "generated_column") -> Any:
        """Main entry point for value generation with AI support"""
        if isinstance(rule, str):
            result = self._generate_from_string_rule(rule)
            if result is not None:
                return result
        elif isinstance(rule, dict):
            # Check if this is an AI generation rule
            if rule.get("type") == "ai_generated":
                return self._generate_with_ai_rule(rule, data_type, column_name)
            else:
                result = self._generate_from_dict_rule(rule, data_type, column_name)
                if result is not None:
                    return result

        result = self._generate_default_by_type(data_type)
        if result is not None:
            return result

        # Final fallback to AI if enabled and no other method worked
        if self.ai_manager and self._should_use_ai_fallback():
            return self._generate_with_ai_fallback(rule, data_type, column_name)

        # Ultimate fallback
        return self._ultimate_fallback(data_type)

    def _generate_with_ai_rule(self, rule: Dict[str, Any], data_type: str, column_name: str) -> Any:
        """Generate value using AI with specific rule configuration"""
        if not self.ai_manager:
            self.logger.warning("AI generation requested but no AI manager available")
            return self._ultimate_fallback(data_type)

        try:
            return self.ai_manager.generate_with_ai(rule, data_type, column_name)
        except Exception as e:
            self.logger.warning(f"AI rule generation failed: {e}")
            return self._ultimate_fallback(data_type)

    def _generate_with_ai_fallback(self, rule: Any, data_type: str, column_name: str = "generated_column") -> Any:
        """Generate value using AI as fallback"""
        if not self.ai_manager:
            return self._ultimate_fallback(data_type)

        try:
            return self.ai_manager.generate_with_ai(rule, data_type, column_name)
        except Exception as e:
            self.logger.warning(f"AI fallback generation failed: {e}")
            return self._ultimate_fallback(data_type)

    def _should_use_ai_fallback(self) -> bool:
        """Check if AI fallback should be used"""
        if not self.ai_manager:
            return False

        # Check if any provider has fallback enabled
        stats = self.ai_manager.get_statistics()
        for provider_stats in stats["providers"].values():
            if provider_stats.get("configuration", {}).get("fallback_enabled", False):
                return True

        return False

    # AI management methods
    def switch_ai_provider(self, provider_name: str) -> bool:
        """Switch primary AI provider"""
        if not self.ai_manager:
            self.logger.warning("No AI manager available")
            return False
        return self.ai_manager.switch_primary_provider(provider_name)

    def get_ai_statistics(self) -> dict:
        """Get comprehensive statistics about AI usage"""
        if not self.ai_manager:
            return {"error": "No AI manager available"}
        return self.ai_manager.get_statistics()

    def reset_ai_statistics(self):
        """Reset AI performance statistics"""
        if self.ai_manager:
            self.ai_manager.reset_statistics()
        else:
            self.logger.warning("No AI manager available")

    def clear_ai_cache(self, cache_key: str = None):
        """Clear AI cache for specific key or all keys"""
        if self.ai_manager:
            self.ai_manager.clear_cache(cache_key)
        else:
            self.logger.warning("No AI manager available")

    def is_ai_available(self) -> bool:
        """Check if AI generation is available"""
        return self.ai_manager is not None and self.ai_manager.is_available()

    # Existing methods remain unchanged
    def _generate_from_string_rule(self, rule: str) -> Any:
        """Generate value from string rule (faker method name)"""
        cleaned_rule = rule.replace(" ", "").replace("_", "").lower()

        # Direct mappings for common cases
        direct_mappings = {
            "bool": lambda: random.choice([True, False]),
            "uuid": lambda: self.faker.uuid4(),
            "cc": lambda: self.faker.credit_card_number(),
            "cc_cvv": lambda: self.faker.credit_card_security_code(),
            "cc_expiry_date": lambda: self.faker.credit_card_expire(),
            "phone": lambda: self.faker.phone_number(),
            "phonenumber": lambda: self.faker.phone_number(),
            "phoneno": lambda: self.faker.phone_number(),
            "firstname": lambda: self.faker.first_name(),
            "lastname": lambda: self.faker.last_name(),
            "timestamp": lambda: self.faker.date_time().strftime("%Y-%m-%d %H:%M:%S"),
        }

        # Try direct mapping first
        if rule in direct_mappings:
            return direct_mappings[rule]()
        if cleaned_rule in direct_mappings:
            return direct_mappings[cleaned_rule]()

        # Try faker method
        return self._try_faker_method(rule, cleaned_rule)

    def _try_faker_method(self, original_rule: str, cleaned_rule: str) -> Any:
        """Try to call faker method with fallback"""
        faker_methods = {x.replace("_", ""): x for x in dir(self.faker) if not x.startswith("_")}
        faker_methods.update({
            ''.join([y[0] for y in x.split('_')]): x
            for x in dir(self.faker)
            if '_' in x and not x.startswith('_')
        })

        method_name = faker_methods.get(original_rule) or faker_methods.get(cleaned_rule)
        if method_name:
            try:
                return getattr(self.faker, method_name)()
            except AttributeError:
                self.logger.warning(f"Faker method '{method_name}' not found")

        return None

    def _generate_from_dict_rule(self, rule: dict, data_type: str, column_name: str) -> Any:
        """Generate value from dictionary rule"""
        rule_type = rule.get("type")

        generators = {
            "choice": lambda: self._generate_choice(rule),
            "date": lambda: self._generate_date_range(rule),
            "date_range": lambda: self._generate_date_range(rule),
            "time": lambda: self._generate_time_range(rule),
            "time_range": lambda: self._generate_time_range(rule),
            "timestamp": lambda: self._generate_timestamp_range(rule),
            "timestamp_range": lambda: self._generate_timestamp_range(rule),
            "range": lambda: self._generate_numeric_range(rule, data_type),
            "fixed": lambda: rule.get("value"),
            "default": lambda: rule.get("value"),
            "email": lambda: self.faker.email(),
            "phone_number": lambda: self.faker.phone_number(),
            "uuid": lambda: self.faker.uuid4(),
            "regex": lambda: self._generate_regex_value(rule)
        }

        if rule_type in generators:
            value = generators[rule_type]()
        else:
            value = self.generate_by_rule(rule_type, data_type, column_name) if rule_type else None

        if 'prefix' in rule or 'suffix' in rule:
            value = self._apply_prefix_suffix(value, rule)

        if not rule.get('multi_line', True):
            value = self.handle_multi_line(value)

        if rule_type == 'regex' or 'regex' in rule:
            value = self._apply_regex_validation(value, rule, column_name)
        return value

    def handle_multi_line(self, value: Any) -> Any:
        """Handle multi line value"""
        if value is None:
            return None

        str_value = str(value)
        return str_value.replace("\n", " ")

    def _apply_prefix_suffix(self, value: Any, rule: dict) -> Any:
        """Apply prefix and/or suffix to generated value"""
        if value is None:
            return value

        # Convert value to string for prefix/suffix operations
        str_value = str(value)

        # Apply prefix
        prefix = rule.get("prefix", "")
        if prefix:
            str_value = f"{prefix}{str_value}"

        # Apply suffix
        suffix = rule.get("suffix", "")
        if suffix:
            str_value = f"{str_value}{suffix}"

        # Return as string if prefix/suffix was applied, otherwise return original type
        if prefix or suffix:
            return str_value

        return value

    def _generate_choice(self, rule: dict) -> Any:
        """Generate value from choices with optional probabilities"""
        choices = rule.get("value", [])
        probabilities = rule.get("probabilities", {})

        if probabilities:
            weights = [probabilities.get(choice, 1.0) for choice in choices]
            return random.choices(choices, weights=weights, k=1)[0]
        return random.choice(choices)

    def _generate_date_range(self, rule: dict) -> Any:
        """Generate date within range"""
        start_date = rule.get("start", "1950-01-01")
        end_date = rule.get("end", datetime.now().strftime("%Y-%m-%d"))

        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        delta = end - start
        return (start + timedelta(days=random.randint(0, delta.days))).date()

    def _generate_time_range(self, rule: dict) -> str:
        """Generate time within range"""
        start_time = rule.get("start", "00:00:00")
        end_time = rule.get("end", "23:59:59")

        start_dt = datetime.strptime(f"1970-01-01 {start_time}", "%Y-%m-%d %H:%M:%S")
        end_dt = datetime.strptime(f"1970-01-01 {end_time}", "%Y-%m-%d %H:%M:%S")

        return self.faker.date_time_between_dates(start_dt, end_dt).strftime("%H:%M:%S")

    def _generate_timestamp_range(self, rule: dict) -> str:
        """Generate timestamp within range"""
        start_ts = rule.get("start", "2000-01-01 00:00:00")
        end_ts = rule.get("end", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

        start_dt = datetime.strptime(start_ts, "%Y-%m-%d %H:%M:%S")
        end_dt = datetime.strptime(end_ts, "%Y-%m-%d %H:%M:%S")

        return self.faker.date_time_between_dates(start_dt, end_dt).strftime("%Y-%m-%d %H:%M:%S")

    def _generate_numeric_range(self, rule: dict, data_type: str) -> Any:
        """Generate numeric value within range"""
        min_val = rule.get("min", 0)
        max_val = rule.get("max", sys.maxsize)

        if data_type in ["int", "str"]:
            return random.randint(int(min_val), int(max_val))
        elif data_type in ["float", "str"]:
            return round(random.uniform(min_val, max_val), 2)
        return min_val

    def _generate_regex_value(self, rule: dict) -> str:
        pattern = rule.get('regex')
        if pattern is None:
            return self.faker.sentence()
        return rstr.xeger(pattern)

    def _apply_regex_validation(self, value: Any, rule: dict, column_name: str) -> Any:
        """Apply regex validation with retry logic"""
        rule_regex = rule.get("regex")
        if not rule_regex or value is None:
            return value
        validator = UnifiedValidator(logger=self.logger)

        max_attempts = 10
        attempts = 0

        while not validator.validate_value(str(value), rule) and attempts < max_attempts:
            rule_type = rule.get("type")
            if rule_type == "email":
                base_value = self.faker.email()
            elif rule_type == "phone_number":
                base_value = self._generate_phone_matching_regex(rule_regex)
            else:
                base_value = self.generate_by_rule(rule_type, "str", column_name)

            # Reapply prefix/suffix after regenerating base value
            value = self._apply_prefix_suffix(base_value, rule)
            attempts += 1

        return value

    def _generate_phone_matching_regex(self, regex_pattern: str) -> str:
        """Generate phone number matching regex pattern"""
        if "10,15" in regex_pattern:
            length = random.randint(10, 15)
            return ''.join([str(random.randint(0, 9)) for _ in range(length)])
        return self.faker.phone_number()

    def _generate_default_by_type(self, data_type: str) -> Any:
        """Generate default value based on data type"""
        defaults = {
            "int": lambda: random.randint(1, 100000),
            "float": lambda: round(random.uniform(1.0, 100.0), 2),
            "bool": lambda: random.choice([True, False]),
            "date": lambda: self.faker.date_between(start_date="-5y", end_date="today"),
            "str": lambda: self.faker.text(max_nb_chars=50),
        }

        return defaults.get(data_type, lambda: None)()

    def _ultimate_fallback(self, data_type: str) -> Any:
        """Ultimate fallback when all else fails"""
        fallbacks = {
            "int": lambda: random.randint(1, 1000),
            "integer": lambda: random.randint(1, 1000),
            "float": lambda: round(random.uniform(1.0, 100.0), 2),
            "double": lambda: round(random.uniform(1.0, 100.0), 2),
            "decimal": lambda: round(random.uniform(1.0, 100.0), 2),
            "bool": lambda: random.choice([True, False]),
            "boolean": lambda: random.choice([True, False]),
            "date": lambda: self.faker.date_between(start_date="-5y", end_date="today"),
            "str": lambda: f"generated_value_{random.randint(1, 9999)}",
            "string": lambda: f"generated_value_{random.randint(1, 9999)}",
            "text": lambda: f"generated_value_{random.randint(1, 9999)}",
        }

        generator = fallbacks.get(data_type.lower(), lambda: f"default_value_{random.randint(1, 9999)}")
        return generator()