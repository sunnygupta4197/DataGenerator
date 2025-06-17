"""
Base AI Provider Abstract Class

This module defines the abstract base class for all AI providers.
"""

from abc import ABC, abstractmethod
from typing import Any, List, Dict
import logging
import json
import io
import csv
from datetime import datetime


class BaseAIProvider(ABC):
    """Abstract base class for AI providers"""

    def __init__(self, logger: logging.Logger, config):
        self.logger = logger
        self.config = config
        self._available = False

    @abstractmethod
    def _test_connection(self) -> bool:
        """Test connection to the AI provider"""
        pass

    @abstractmethod
    def _generate_response(self, prompt: str) -> str:
        """Generate response from AI provider"""
        pass

    @abstractmethod
    def get_provider_name(self) -> str:
        """Get the name of the provider"""
        pass

    def is_available(self) -> bool:
        """Check if provider is available"""
        return self._available

    def generate_data(self, rule: Any, data_type: str, column_name: str, count: int) -> List[Any]:
        """Generate synthetic data using AI"""
        if not self.is_available():
            raise Exception(f"{self.get_provider_name()} provider is not available")

        # Prepare metadata
        column_metadata = self._prepare_column_metadata(rule, data_type, column_name, count)

        # Generate prompt
        prompt = self._generate_prompt(column_metadata)

        # Get AI response
        csv_data = self._generate_response(prompt)

        # Parse and return values
        return self._parse_csv_response(csv_data, column_name, data_type)

    def _prepare_column_metadata(self, rule: Any, data_type: str, column_name: str, count: int) -> Dict[str, Any]:
        """Prepare column metadata for AI prompt"""
        metadata = {
            "name": column_name,
            "type": data_type,
            "how_much": count,
        }

        if isinstance(rule, dict):
            if rule.get("type") == "ai_generated":
                # AI rule with specific instructions
                metadata.update({
                    "description": rule.get("description", f"Generate {data_type} data"),
                    "context_columns": rule.get("context_columns", []),
                    "max_length": rule.get("max_length", 100),
                    "generation_type": "rule_based"
                })
            else:
                # Other dict rule
                metadata.update({
                    "rule": rule,
                    "description": rule.get("description", f"Generate {data_type} data with complex rule"),
                    "generation_type": "fallback"
                })
        elif isinstance(rule, str):
            # String rule
            metadata.update({
                "rule": rule,
                "description": f"Generate {data_type} data following rule: {rule}",
                "generation_type": "fallback"
            })
        else:
            # Default case
            metadata.update({
                "description": f"Generate random {data_type} data",
                "generation_type": "fallback"
            })

        return metadata

    def _generate_prompt(self, column_metadata: Dict[str, Any]) -> str:
        """Generate prompt for AI"""
        generation_type = column_metadata.get("generation_type", "fallback")

        if generation_type == "rule_based":
            return self._generate_rule_based_prompt(column_metadata)
        else:
            return self._generate_fallback_prompt(column_metadata)

    def _generate_rule_based_prompt(self, column_metadata: Dict[str, Any]) -> str:
        """Generate prompt for rule-based AI generation"""
        description = column_metadata.get("description", "")
        context_columns = column_metadata.get("context_columns", [])
        max_length = column_metadata.get("max_length", 100)
        column_name = column_metadata.get("name", "column")
        data_type = column_metadata.get("type", "str")
        count = column_metadata.get("how_much", 100)

        prompt = f"""You are a synthetic data generator. Generate {count} realistic values for a column named '{column_name}' of type '{data_type}'.

Requirements:
- {description}
- Maximum length: {max_length} characters
- Return ONLY a CSV with header '{column_name}' and {count} data rows
- No explanations, no markdown formatting
- Each value should be realistic and diverse
"""

        if context_columns:
            prompt += f"Consider that this column relates to: {', '.join(context_columns)}\n"

        prompt += f"\nGenerate {count} diverse, realistic values now:"
        return prompt.strip()

    def _generate_fallback_prompt(self, column_metadata: Dict[str, Any]) -> str:
        """Generate prompt for fallback AI generation"""
        return f"""You are a synthetic data generator. I will give you a JSON containing column metadata.
You need to return only synthetic CSV data for that column with the specified number of rows.
The CSV should contain only the header (column name) and synthetic data, and nothing else — no explanations, no markdown.

Here's the input JSON:
{json.dumps(column_metadata, indent=2)}

Now generate the data accordingly.""".strip()

    def _parse_csv_response(self, csv_data: str, column_name: str, data_type: str) -> List[Any]:
        """Parse CSV response and convert to appropriate data types"""
        try:
            csv_file = io.StringIO(csv_data)
            reader = csv.DictReader(csv_file)

            values = []
            for row in reader:
                value = row.get(column_name) or next(iter(row.values()), None)
                if value is not None:
                    converted_value = self._convert_ai_value(value, data_type)
                    if converted_value is not None:
                        values.append(converted_value)

            return values

        except Exception as e:
            self.logger.warning(f"Failed to parse CSV response: {e}")
            return self._parse_simple_response(csv_data, data_type)

    def _parse_simple_response(self, response_data: str, data_type: str) -> List[Any]:
        """Simple fallback parser for non-CSV responses"""
        lines = response_data.strip().split('\n')
        values = []

        start_idx = 1 if lines and any(word in lines[0].lower()
                                     for word in ['name', 'column', 'header']) else 0

        for line in lines[start_idx:]:
            line = line.strip()
            if line and not line.startswith('#'):
                converted_value = self._convert_ai_value(line, data_type)
                if converted_value is not None:
                    values.append(converted_value)

        return values

    def _convert_ai_value(self, value: str, data_type: str) -> Any:
        """Convert AI generated value to appropriate data type"""
        if not value or value.lower() in ['null', 'none', '']:
            return None

        try:
            if data_type.lower() in ['int', 'integer']:
                import re
                numeric_match = re.search(r'-?\d+', str(value))
                if numeric_match:
                    return int(numeric_match.group())
                return int(value)

            elif data_type.lower() in ['float', 'double', 'decimal']:
                import re
                numeric_match = re.search(r'-?\d+\.?\d*', str(value))
                if numeric_match:
                    return float(numeric_match.group())
                return float(value)

            elif data_type.lower() in ['bool', 'boolean']:
                return str(value).lower() in ['true', '1', 'yes', 'on']

            elif data_type.lower() in ['date']:
                for date_format in ['%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y']:
                    try:
                        return datetime.strptime(str(value), date_format).date()
                    except ValueError:
                        continue
                return str(value)

            else:
                return str(value).strip('"\'')

        except (ValueError, TypeError):
            return str(value)

    def estimate_cost(self, rule: Any, data_type: str, count: int) -> float:
        """Estimate cost for generation (to be overridden by specific providers)"""
        return 0.0

    def get_cost_limit(self) -> float:
        """Get cost limit for this provider"""
        return getattr(self.config, 'cost_limit_usd', None)

    def get_configuration(self) -> Dict[str, Any]:
        """Get provider configuration details"""
        return {
            "enabled": getattr(self.config, 'enabled', True),
            "model": getattr(self.config, 'model', 'unknown'),
            "temperature": getattr(self.config, 'temperature', 0.7),
            "max_tokens": getattr(self.config, 'max_tokens', 1000),
            "timeout_seconds": getattr(self.config, 'timeout_seconds', 30),
            "retry_attempts": getattr(self.config, 'retry_attempts', 3),
            "fallback_enabled": getattr(self.config, 'fallback_enabled', True)
        }