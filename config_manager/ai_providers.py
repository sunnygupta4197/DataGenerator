import os
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, Type, List
from enum import Enum
from dataclasses import dataclass, asdict, field
import importlib


class AIProvider(Enum):
    """Supported AI providers"""
    OPENAI = "openai"
    MISTRAL = "mistral"

    @classmethod
    def all(cls):
        """Get all providers as list"""
        return list(cls)

    @classmethod
    def values(cls):
        """Get all provider values as list"""
        return [provider.value for provider in cls]


# Base provider config class
@dataclass
class BaseProviderConfig(ABC):
    """Base class for all AI provider configurations"""
    enabled: bool = False
    api_key: Optional[str] = None
    api_key_env_var: str = ""
    api_key_file: Optional[str] = None
    model: str = ""
    max_tokens: int = 2000
    temperature: float = 0.7
    cache_size: int = 100
    timeout_seconds: int = 30
    retry_attempts: int = 3
    cost_limit_usd: Optional[float] = None
    fallback_enabled: bool = True

    @abstractmethod
    def get_default_model(self) -> str:
        pass

    @abstractmethod
    def get_valid_models(self) -> List[str]:
        pass

    def get_api_key(self) -> Optional[str]:
        """Universal API key getter"""
        if self.api_key:
            return self.api_key
        if self.api_key_file and os.path.exists(self.api_key_file):
            try:
                with open(self.api_key_file, 'r') as f:
                    return f.read().strip()
            except:
                pass
        return os.getenv(self.api_key_env_var)

    def is_available(self) -> bool:
        return self.enabled and self.get_api_key() is not None


# Provider-specific configs inherit from base
@dataclass
class OpenAIConfig(BaseProviderConfig):
    api_key_env_var: str = "OPENAI_API_KEY"
    model: str = "gpt-3.5-turbo"

    def get_default_model(self) -> str:
        return "gpt-3.5-turbo"

    def get_valid_models(self) -> List[str]:
        return ['gpt-3.5-turbo', 'gpt-3.5-turbo-16k', 'gpt-4', 'gpt-4-turbo-preview']


@dataclass
class MistralConfig(BaseProviderConfig):
    api_key_env_var: str = "MISTRAL_API_KEY"
    model: str = "mistral-small"

    def get_default_model(self) -> str:
        return "mistral-small"

    def get_valid_models(self) -> List[str]:
        return ['mistral-tiny', 'mistral-small', 'mistral-medium', 'mistral-large']


class AIProviderRegistry:
    """Registry for all AI providers - automatically handles all providers"""

    _providers: Dict[AIProvider, Type[BaseProviderConfig]] = {
        AIProvider.OPENAI: OpenAIConfig,
        AIProvider.MISTRAL: MistralConfig,
    }

    _cost_rates: Dict[AIProvider, Dict[str, float]] = {
        AIProvider.OPENAI: {
            "gpt-3.5-turbo": 0.002,
            "gpt-4": 0.03
        },
        AIProvider.MISTRAL: {
            "mistral-small": 0.0006,
            "mistral-large": 0.008
        }
    }

    @classmethod
    def get_config_class(cls, provider: AIProvider) -> Type[BaseProviderConfig]:
        return cls._providers[provider]

    @classmethod
    def create_config(cls, provider: AIProvider, **kwargs) -> BaseProviderConfig:
        config_class = cls.get_config_class(provider)
        return config_class(**kwargs)

    @classmethod
    def get_all_providers(cls) -> List[AIProvider]:
        return list(cls._providers.keys())

    @classmethod
    def get_cost_rate(cls, provider: AIProvider, model: str) -> float:
        return cls._cost_rates.get(provider, {}).get(model, 0.001)


# Enhanced AIConfig using registry
@dataclass
class AIConfig:
    """Master AI configuration - works with any number of providers"""
    primary_provider: AIProvider = AIProvider.OPENAI
    enable_fallback: bool = True
    shared_cache_size: int = 200
    _provider_configs: Dict[AIProvider, BaseProviderConfig] = field(default_factory=dict)

    openai: Optional[OpenAIConfig] = None
    mistral: Optional[MistralConfig] = None

    def __post_init__(self):
        # Initialize all provider configs if not provided
        for provider in AIProviderRegistry.get_all_providers():
            if provider not in self._provider_configs:
                self._provider_configs[provider] = AIProviderRegistry.create_config(provider)

    def get_provider_config(self, provider: AIProvider) -> BaseProviderConfig:
        """Get config for any provider - no manual checks needed"""
        return getattr(self, provider.value)

    def enable_provider(self, provider: AIProvider, **kwargs) -> bool:
        """Enable any provider - no manual checks needed"""
        config = self.get_provider_config(provider)
        config.enabled = True

        # Apply any provided kwargs
        for key, value in kwargs.items():
            if hasattr(config, key):
                setattr(config, key, value)
        return True

    def disable_provider(self, provider: AIProvider) -> bool:
        """Disable any provider - no manual checks needed"""
        self.get_provider_config(provider).enabled = False
        return True

    def get_active_providers(self) -> List[AIProvider]:
        """Get all active providers dynamically"""
        return [p for p in AIProvider.all()
                if getattr(self, p.value, None) and getattr(self, p.value).enabled]

    def switch_primary_provider(self, provider: AIProvider) -> bool:
        """Switch primary provider - validates automatically"""
        if self.get_provider_config(provider).enabled:
            self.primary_provider = provider
            return True
        return False

    def get_primary_provider_config(self) -> BaseProviderConfig:
        """Get primary provider config"""
        return self.get_provider_config(self.primary_provider)
