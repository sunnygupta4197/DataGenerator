"""
OpenAI Provider for Synthetic Data Generation

This module provides OpenAI integration for AI-powered data generation.
"""

from typing import Any, Dict
from .base_provider import BaseAIProvider

# OpenAI imports with fallback handling
try:
    import openai

    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False


class OpenAIProvider(BaseAIProvider):
    """OpenAI provider for synthetic data generation"""

    def __init__(self, logger, config):
        super().__init__(logger, config)

        if not OPENAI_AVAILABLE:
            self.logger.warning("OpenAI package not available")
            return

        if not config.enabled:
            self.logger.info("OpenAI integration is disabled in configuration")
            return

        # Initialize OpenAI
        self._initialize_openai()

    def _initialize_openai(self):
        """Initialize OpenAI with configuration"""
        try:
            api_key = self.config.get_api_key()
            if not api_key:
                self.logger.warning("OpenAI enabled but no API key available")
                return

            openai.api_key = api_key

            # Test connection
            if self._test_connection():
                self._available = True
                self.logger.info(f"OpenAI initialized successfully with model: {self.config.model}")
            else:
                self.logger.error("OpenAI connection test failed")

        except Exception as e:
            self.logger.error(f"Failed to initialize OpenAI: {e}")

    def _test_connection(self) -> bool:
        """Test OpenAI connection"""
        try:
            test_response = openai.ChatCompletion.create(
                model=self.config.model,
                messages=[{"role": "user", "content": "Test"}],
                max_tokens=1,
                timeout=self.config.timeout_seconds
            )
            return True
        except Exception as e:
            self.logger.error(f"OpenAI connection test failed: {e}")
            return False

    def _generate_response(self, prompt: str) -> str:
        """Generate response from OpenAI"""
        try:
            response = openai.ChatCompletion.create(
                model=self.config.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=self.config.temperature,
                max_tokens=self.config.max_tokens,
                timeout=self.config.timeout_seconds
            )

            return response['choices'][0]['message']['content'].strip()

        except Exception as e:
            # Retry logic could be implemented here
            self.logger.error(f"OpenAI request failed: {e}")
            raise

    def get_provider_name(self) -> str:
        """Get the name of the provider"""
        return "openai"

    def estimate_cost(self, rule: Any, data_type: str, count: int) -> float:
        """Estimate cost for OpenAI generation"""
        # Rough cost estimation
        estimated_prompt_tokens = len(str(rule)) // 4
        estimated_response_tokens = count * 10

        # Cost per 1K tokens (approximate rates as of 2024)
        cost_rates = {
            "gpt-3.5-turbo": 0.002,
            "gpt-4": 0.03,
            "gpt-4-turbo-preview": 0.01,
            "gpt-4o": 0.005,
            "gpt-4o-mini": 0.0015
        }

        rate = cost_rates.get(self.config.model, 0.002)  # Default fallback rate
        estimated_cost = ((estimated_prompt_tokens + estimated_response_tokens) / 1000) * rate

        self.logger.debug(f"Estimated OpenAI cost: ${estimated_cost:.4f}")
        return estimated_cost

    def get_configuration(self) -> Dict[str, Any]:
        """Get OpenAI-specific configuration details"""
        base_config = super().get_configuration()
        base_config.update({
            "provider": "openai",
            "api_key_available": bool(getattr(self.config, 'get_api_key', lambda: None)()),
            "cache_size": getattr(self.config, 'cache_size', 100)
        })
        return base_config