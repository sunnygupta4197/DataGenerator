import sys
import rstr
from faker import Faker
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Any, Optional
import random

# Import your existing modules
from quality.validation_system import Validator
from .ai_providers import AIProviderManager


class ValueGenerator:
    """Enhanced ValueGenerator with configurable AI integration and date formatting"""

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

    def _handle_ai_generation(self, rule: Any, data_type: str, column_name: str, is_fallback: bool = False) -> Any:
        """Unified AI generation handling"""
        if not self.ai_manager:
            if is_fallback:
                return self._get_fallback_value(data_type, "ai_unavailable")
            else:
                self.logger.warning("AI generation requested but no AI manager available")
                return self._get_fallback_value(data_type, "ai_unavailable")

        try:
            return self.ai_manager.generate_with_ai(rule, data_type, column_name)
        except Exception as e:
            error_context = "AI fallback generation" if is_fallback else "AI rule generation"
            self.logger.warning(f"{error_context} failed: {e}")
            return self._get_fallback_value(data_type, "ai_error")

    def _generate_with_ai_rule(self, rule: Dict[str, Any], data_type: str, column_name: str) -> Any:
        """Generate value using AI with specific rule configuration"""
        return self._handle_ai_generation(rule, data_type, column_name, is_fallback=False)

    def _generate_with_ai_fallback(self, rule: Any, data_type: str, column_name: str = "generated_column") -> Any:
        """Generate value using AI as fallback"""
        return self._handle_ai_generation(rule, data_type, column_name, is_fallback=True)

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
        """Generate date within range with optional formatting"""
        start_date = rule.get("start", "1950-01-01")
        end_date = rule.get("end", datetime.now().strftime("%Y-%m-%d"))
        date_format = rule.get("format", "%Y-%m-%d")  # Custom format

        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        delta = end - start
        generated_date = (start + timedelta(days=random.randint(0, delta.days))).date()

        # Apply custom formatting if specified
        if date_format:
            return self._format_date(generated_date, date_format)

        return generated_date

    def _generate_time_range(self, rule: dict) -> str:
        """Generate time within range with optional formatting"""
        start_time = rule.get("start", "00:00:00")
        end_time = rule.get("end", "23:59:59.000")
        time_format = rule.get("format", "%H:%M:%S")  # Default format

        start_dt = datetime.strptime(f"1970-01-01 {start_time}", "%Y-%m-%d %H:%M:%S")
        end_dt = datetime.strptime(f"1970-01-01 {end_time}", "%Y-%m-%d %H:%M:%S")

        generated_time = self.faker.date_time_between_dates(start_dt, end_dt).replace(microsecond=random.randint(0, 999999))

        # Apply custom formatting
        return generated_time.strftime(time_format)

    def _generate_timestamp_range(self, rule: dict) -> str:
        """Generate timestamp within range with optional formatting"""
        start_ts = rule.get("start", "2000-01-01 00:00:00")
        end_ts = rule.get("end", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        timestamp_format = rule.get("format", "%Y-%m-%d %H:%M:%S")  # Default format

        start_dt = datetime.strptime(start_ts, "%Y-%m-%d %H:%M:%S")
        end_dt = datetime.strptime(end_ts, "%Y-%m-%d %H:%M:%S")

        generated_timestamp = self.faker.date_time_between_dates(start_dt, end_dt).replace(microsecond=random.randint(0, 999999))

        # Apply custom formatting
        if timestamp_format:
            return self._format_datetime(generated_timestamp, timestamp_format)

        return generated_timestamp.strftime("%Y-%m-%d %H:%M:%S")

    def _convert_human_format_to_strftime(self, format_string: str) -> str:
        """Convert human-readable format patterns to Python strftime patterns"""
        # Comprehensive format mappings - order matters for replacement
        format_mappings = {
            # Date patterns (longest first to avoid partial matches)
            'MMMM': '%B',  # Full month name (January, February, etc.)
            'MMM': '%b',  # Abbreviated month name (Jan, Feb, etc.)
            'YYYY': '%Y',  # 4-digit year
            'DD': '%d',  # Day with leading zero (01-31)
            'MM': '%m',  # Month with leading zero (01-12)
            'YY': '%y',  # 2-digit year

            # Time patterns
            'SSS': '%f', # MiliSeconds with leading zero (000-999)
            'HH': '%H',  # Hour with leading zero (00-23)
            'hh': '%I',  # Hour with leading zero (01-12)
            'mm': '%M',  # Minute with leading zero (00-59)
            'ss': '%S',  # Second with leading zero (00-59)
            'A': '%p',  # AM/PM uppercase
            'a': '%p',  # AM/PM (strftime always returns uppercase)

            # Additional common patterns
            'dddd': '%A',  # Full day name (Monday, Tuesday, etc.)
            'ddd': '%a',  # Abbreviated day name (Mon, Tue, etc.)
        }

        # Replace human-readable patterns with strftime patterns
        # Sort by length (descending) to avoid partial replacements
        strftime_format = format_string
        for human_pattern, strftime_pattern in sorted(format_mappings.items(), key=len, reverse=True):
            strftime_format = strftime_format.replace(human_pattern, strftime_pattern)

        return strftime_format

    def _format_date(self, date_obj, format_string: str) -> str:
        """Format date object according to human-readable format string"""
        try:
            strftime_format = self._convert_human_format_to_strftime(format_string)
            return date_obj.strftime(strftime_format)
        except ValueError as e:
            self.logger.warning(f"Invalid date format '{format_string}': {e}. Using default format.")
            return date_obj.strftime('%Y-%m-%d')  # Fallback to ISO format

    def _format_datetime(self, datetime_obj, format_string: str) -> str:
        """Format datetime object according to human-readable format string"""
        try:
            strftime_format = self._convert_human_format_to_strftime(format_string)
            return datetime_obj.strftime(strftime_format)
        except ValueError as e:
            self.logger.warning(f"Invalid datetime format '{format_string}': {e}. Using default format.")
            return datetime_obj.strftime('%Y-%m-%d %H:%M:%S')  # Fallback to ISO format

    def _validate_and_suggest_format(self, format_string: str, context: str = "") -> str:
        """Validate format string and suggest corrections if needed"""
        # Common format mistakes and their corrections
        format_corrections = {
            'dd': 'DD', 'DD': 'DD',
            'mm': 'MM', 'MM': 'MM',
            'yyyy': 'YYYY', 'YYYY': 'YYYY',
            'yy': 'YY', 'YY': 'YY',
            'hh': 'HH', 'HH': 'HH',
            'min': 'mm', 'minute': 'mm', 'minutes': 'mm',
            'sec': 'ss', 'second': 'ss', 'seconds': 'ss', 'sss': 'miliseconds',
            'am': 'A', 'pm': 'A', 'AM': 'A', 'PM': 'A',
            'month': 'MMMM', 'mon': 'MMM',
            'day': 'DD', 'year': 'YYYY'
        }

        corrected_format = format_string
        corrections_made = []

        # Apply corrections
        for mistake, correction in format_corrections.items():
            if mistake in corrected_format and mistake != correction:
                corrected_format = corrected_format.replace(mistake, correction)
                corrections_made.append(f"'{mistake}' → '{correction}'")

        # Log corrections if any were made
        if corrections_made:
            self.logger.info(f"Format auto-corrected{' for ' + context if context else ''}: "
                             f"{', '.join(corrections_made)}. "
                             f"Original: '{format_string}' → Corrected: '{corrected_format}'")

        return corrected_format

    def _generate_numeric_range(self, rule: dict, data_type: str) -> Any:
        """Generate numeric value within range"""
        min_val = rule.get("min", 0)
        max_val = rule.get("max", sys.maxsize)

        if data_type in ["int", "str"]:
            return random.randint(int(min_val), int(max_val))
        elif data_type in ["float", "str"]:
            precision = rule.get("precision", 2)
            return round(random.uniform(min_val, max_val), precision)
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
        validator = Validator(logger=self.logger)

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

    def _get_fallback_value(self, data_type: str, context: str = "default") -> Any:
        """Unified fallback value generation for all contexts"""
        # Unified fallback mappings
        fallback_mappings = {
            "int": lambda: random.randint(1, 100000 if context == "default" else 1000),
            "integer": lambda: random.randint(1, 100000 if context == "default" else 1000),
            "float": lambda: round(random.uniform(1.0, 100.0), 2),
            "double": lambda: round(random.uniform(1.0, 100.0), 2),
            "decimal": lambda: round(random.uniform(1.0, 100.0), 2),
            "bool": lambda: random.choice([True, False]),
            "boolean": lambda: random.choice([True, False]),
            "date": lambda: self.faker.date_between(start_date="-5y", end_date="today"),
            "str": lambda: (
                self.faker.text(max_nb_chars=50) if context == "default"
                else f"generated_value_{random.randint(1, 9999)}"
            ),
            "string": lambda: (
                self.faker.text(max_nb_chars=50) if context == "default"
                else f"generated_value_{random.randint(1, 9999)}"
            ),
            "text": lambda: (
                self.faker.text(max_nb_chars=50) if context == "default"
                else f"generated_value_{random.randint(1, 9999)}"
            ),
        }

        generator = fallback_mappings.get(
            data_type.lower(),
            lambda: f"{context}_value_{random.randint(1, 9999)}"
        )
        return generator()

    def _generate_default_by_type(self, data_type: str) -> Any:
        """Generate default value based on data type"""
        return self._get_fallback_value(data_type, "default")

    def _ultimate_fallback(self, data_type: str) -> Any:
        """Ultimate fallback when all else fails"""
        return self._get_fallback_value(data_type, "fallback")