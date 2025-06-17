"""
AI Providers Package for Synthetic Data Generation

This package provides AI-powered data generation capabilities using multiple providers
including OpenAI and Mistral AI.
"""

from enum import Enum
from typing import Dict, Any, Optional
import logging


class AIProvider(Enum):
    """Enumeration of supported AI providers"""
    OPENAI = "openai"
    MISTRAL = "mistral"


class AIProviderManager:
    """Manager class for AI providers with fallback support"""

    def __init__(self, logger: logging.Logger, ai_config=None):
        self.logger = logger
        self.ai_config = ai_config

        # Provider instances
        self.providers = {}
        self.active_provider = None
        self.fallback_providers = []

        # Shared resources
        self._ai_cache = {}  # cache_key -> list of values
        self._cache_usage_count = {}  # cache_key -> current usage index
        self._default_cache_size = 100
        self._cost_tracker = {}  # provider -> cost

        # Performance tracking (per provider)
        self._provider_stats = {
            AIProvider.OPENAI.value: {"requests": 0, "cache_hits": 0, "failures": 0},
            AIProvider.MISTRAL.value: {"requests": 0, "cache_hits": 0, "failures": 0}
        }

        if ai_config:
            self._initialize_providers()

    def _initialize_providers(self):
        """Initialize all available AI providers"""
        from .openai_provider import OpenAIProvider
        from .mistral_provider import MistralProvider

        # Initialize OpenAI if configured
        if hasattr(self.ai_config, 'openai') and self.ai_config.openai:
            try:
                openai_provider = OpenAIProvider(self.logger, self.ai_config.openai)
                if openai_provider.is_available():
                    self.providers[AIProvider.OPENAI] = openai_provider
                    self._cost_tracker[AIProvider.OPENAI.value] = 0.0
                    self.logger.info("OpenAI provider initialized successfully")
            except Exception as e:
                self.logger.error(f"Failed to initialize OpenAI provider: {e}")

        # Initialize Mistral if configured
        if hasattr(self.ai_config, 'mistral') and self.ai_config.mistral:
            try:
                mistral_provider = MistralProvider(self.logger, self.ai_config.mistral)
                if mistral_provider.is_available():
                    self.providers[AIProvider.MISTRAL] = mistral_provider
                    self._cost_tracker[AIProvider.MISTRAL.value] = 0.0
                    self.logger.info("Mistral provider initialized successfully")
            except Exception as e:
                self.logger.error(f"Failed to initialize Mistral provider: {e}")

        # Set primary and fallback providers
        self._set_provider_priorities()

    def _set_provider_priorities(self):
        """Set primary and fallback AI providers based on configuration"""
        # Determine primary provider
        primary_provider = getattr(self.ai_config, 'primary_provider', 'openai')

        if primary_provider == 'mistral' and AIProvider.MISTRAL in self.providers:
            self.active_provider = AIProvider.MISTRAL
        elif primary_provider == 'openai' and AIProvider.OPENAI in self.providers:
            self.active_provider = AIProvider.OPENAI
        else:
            # Auto-select first available provider
            if self.providers:
                self.active_provider = next(iter(self.providers.keys()))

        # Set fallback providers
        self.fallback_providers = [
            provider for provider in self.providers.keys()
            if provider != self.active_provider
        ]

        if self.active_provider:
            self.logger.info(f"Primary AI provider: {self.active_provider.value}")
            if self.fallback_providers:
                fallback_names = [p.value for p in self.fallback_providers]
                self.logger.info(f"Fallback AI providers: {', '.join(fallback_names)}")

    def generate_with_ai(self, rule: Any, data_type: str, column_name: str = "generated_column") -> Any:
        """Generate value using AI with provider fallback"""
        # Try primary provider first
        if self.active_provider:
            try:
                return self._generate_with_provider(
                    self.active_provider, rule, data_type, column_name
                )
            except Exception as e:
                self.logger.warning(f"{self.active_provider.value} generation failed: {e}")
                self._provider_stats[self.active_provider.value]["failures"] += 1

        # Try fallback providers
        for provider in self.fallback_providers:
            try:
                self.logger.info(f"Trying fallback provider: {provider.value}")
                return self._generate_with_provider(
                    provider, rule, data_type, column_name
                )
            except Exception as e:
                self.logger.warning(f"{provider.value} fallback failed: {e}")
                self._provider_stats[provider.value]["failures"] += 1

        # All providers failed
        raise Exception("All AI providers failed to generate data")

    def _generate_with_provider(self, provider: AIProvider, rule: Any, data_type: str, column_name: str) -> Any:
        """Generate value using specific AI provider"""
        provider_instance = self.providers[provider]

        # Check cost limit
        if self._check_cost_limit(provider):
            raise Exception(f"Cost limit reached for {provider.value}")

        # Create cache key
        cache_key = self._create_cache_key(rule, data_type, column_name, provider)

        # Check cache
        if cache_key in self._ai_cache:
            self._provider_stats[provider.value]["cache_hits"] += 1
            return self._get_cached_value(cache_key)

        # Generate new data
        self._generate_and_cache_data(provider_instance, rule, data_type, column_name, cache_key)
        return self._get_cached_value(cache_key)

    def _check_cost_limit(self, provider: AIProvider) -> bool:
        """Check if provider has exceeded cost limit"""
        provider_instance = self.providers[provider]
        cost_limit = provider_instance.get_cost_limit()
        if not cost_limit:
            return False
        return self._cost_tracker.get(provider.value, 0) >= cost_limit

    def _create_cache_key(self, rule: Any, data_type: str, column_name: str, provider: AIProvider) -> str:
        """Create cache key including provider"""
        import json
        rule_str = json.dumps(rule, sort_keys=True) if isinstance(rule, dict) else str(rule)
        return f"{provider.value}_{column_name}_{data_type}_{hash(rule_str)}"

    def _generate_and_cache_data(self, provider_instance, rule: Any, data_type: str, column_name: str, cache_key: str):
        """Generate and cache data using AI provider"""
        try:
            # Track request
            provider_name = provider_instance.get_provider_name()
            self._provider_stats[provider_name]["requests"] += 1

            # Generate data
            values = provider_instance.generate_data(rule, data_type, column_name, self._default_cache_size)

            if values:
                self._ai_cache[cache_key] = values
                self._cache_usage_count[cache_key] = 0
                self.logger.info(f"Cached {len(values)} {provider_name} values for {cache_key}")

                # Track cost
                estimated_cost = provider_instance.estimate_cost(rule, data_type, self._default_cache_size)
                self._cost_tracker[provider_name] = self._cost_tracker.get(provider_name, 0) + estimated_cost

            else:
                self.logger.warning(f"No valid data received from {provider_name} for {cache_key}")

        except Exception as e:
            self.logger.error(f"Failed to generate data: {e}")
            raise

    def _get_cached_value(self, cache_key: str) -> Any:
        """Get next value from cache, cycling through available values"""
        if cache_key not in self._ai_cache or not self._ai_cache[cache_key]:
            raise Exception("No cached values available")

        current_index = self._cache_usage_count.get(cache_key, 0)
        cached_values = self._ai_cache[cache_key]
        value = cached_values[current_index % len(cached_values)]
        self._cache_usage_count[cache_key] = current_index + 1
        return value

    def switch_primary_provider(self, provider_name: str) -> bool:
        """Switch primary AI provider"""
        try:
            provider = AIProvider(provider_name.lower())
            if provider in self.providers:
                old_provider = self.active_provider
                self.active_provider = provider

                # Update fallback providers
                self.fallback_providers = [
                    p for p in self.providers.keys() if p != self.active_provider
                ]

                self.logger.info(f"Switched primary provider from {old_provider.value if old_provider else 'None'} "
                                 f"to {provider.value}")
                return True
            else:
                self.logger.error(f"Provider {provider_name} not available")
                return False
        except ValueError:
            self.logger.error(f"Invalid provider name: {provider_name}")
            return False

    def get_statistics(self) -> dict:
        """Get comprehensive statistics about AI usage"""
        stats = {
            "providers": {},
            "cache_info": {
                "total_cached_rules": len(self._ai_cache),
                "total_cached_values": sum(len(values) for values in self._ai_cache.values()),
                "cache_usage": dict(self._cache_usage_count)
            },
            "active_provider": self.active_provider.value if self.active_provider else None,
            "fallback_providers": [p.value for p in self.fallback_providers]
        }

        for provider_enum, provider_instance in self.providers.items():
            provider_name = provider_enum.value
            provider_stats = self._provider_stats[provider_name]

            total_requests = provider_stats["requests"] + provider_stats["cache_hits"]
            cache_hit_rate = (provider_stats["cache_hits"] / total_requests) if total_requests > 0 else 0

            stats["providers"][provider_name] = {
                "available": provider_instance.is_available(),
                "requests": provider_stats["requests"],
                "cache_hits": provider_stats["cache_hits"],
                "failures": provider_stats["failures"],
                "cache_hit_rate": cache_hit_rate,
                "estimated_cost_usd": self._cost_tracker.get(provider_name, 0),
                "cost_limit_usd": provider_instance.get_cost_limit(),
                "configuration": provider_instance.get_configuration()
            }

        return stats

    def reset_statistics(self):
        """Reset AI performance statistics"""
        for provider in self._provider_stats:
            self._provider_stats[provider] = {"requests": 0, "cache_hits": 0, "failures": 0}

        self._cost_tracker = {provider: 0.0 for provider in self._cost_tracker}
        self.logger.info("AI performance statistics reset")

    def clear_cache(self, cache_key: str = None):
        """Clear AI cache for specific key or all keys"""
        if cache_key:
            self._ai_cache.pop(cache_key, None)
            self._cache_usage_count.pop(cache_key, None)
        else:
            self._ai_cache.clear()
            self._cache_usage_count.clear()
        self.logger.info(f"Cleared AI cache: {cache_key or 'all'}")

    def is_available(self) -> bool:
        """Check if any AI providers are available"""
        return len(self.providers) > 0


# Export main classes
__all__ = ['AIProvider', 'AIProviderManager']