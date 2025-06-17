"""
Mistral AI Provider for Synthetic Data Generation

This module provides Mistral AI integration for AI-powered data generation.
"""

from typing import Any, Dict
from .base_provider import BaseAIProvider

# Mistral AI imports with fallback handling
try:
    from mistralai import Mistral

    MISTRAL_AVAILABLE = True
    MISTRAL_CLIENT_VERSION = "new"
except ImportError:
    try:
        # Fallback to old client (v0.4.2 and below)
        from mistralai.client import MistralClient

        MISTRAL_AVAILABLE = True
        MISTRAL_CLIENT_VERSION = "old"
    except ImportError:
        MISTRAL_AVAILABLE = False
        MISTRAL_CLIENT_VERSION = None

# Additional imports for old client
try:
    from mistralai.models.chat_completion import ChatMessage

    MISTRAL_CHAT_MESSAGE_AVAILABLE = True
except ImportError:
    MISTRAL_CHAT_MESSAGE_AVAILABLE = False
    ChatMessage = None


class MistralProvider(BaseAIProvider):
    """Mistral AI provider for synthetic data generation"""

    def __init__(self, logger, config):
        super().__init__(logger, config)
        self.client = None
        self.client_version = MISTRAL_CLIENT_VERSION

        if not MISTRAL_AVAILABLE:
            self.logger.warning("Mistral AI package not available")
            return

        if not config.enabled:
            self.logger.info("Mistral AI integration is disabled in configuration")
            return

        # Initialize Mistral
        self._initialize_mistral()

    def _initialize_mistral(self):
        """Initialize Mistral AI with configuration"""
        try:
            api_key = self.config.get_api_key()
            if not api_key:
                self.logger.warning("Mistral AI enabled but no API key available")
                return

            # Create Mistral client based on available version
            if MISTRAL_CLIENT_VERSION == "new":
                # New client (v1.0+)
                self.client = Mistral(api_key=api_key)
            elif MISTRAL_CLIENT_VERSION == "old":
                # Old client (v0.4.2 and below)
                self.client = MistralClient(api_key=api_key)
            else:
                self.logger.error("No compatible Mistral client available")
                return

            # Test connection
            if self._test_connection():
                self._available = True
                self.logger.info(f"Mistral AI initialized successfully with model: {self.config.model} "
                                 f"(client: {MISTRAL_CLIENT_VERSION})")
            else:
                self.logger.error("Mistral AI connection test failed")

        except Exception as e:
            self.logger.error(f"Failed to initialize Mistral AI: {e}")

    def _test_connection(self) -> bool:
        """Test Mistral AI connection"""
        try:
            if self.client_version == "new":
                # New client API
                response = self.client.chat.complete(
                    model=self.config.model,
                    messages=[
                        {
                            "role": "user",
                            "content": "Test connection"
                        }
                    ],
                    max_tokens=10
                )
                return response is not None
            elif self.client_version == "old":
                # Old client API
                if not MISTRAL_CHAT_MESSAGE_AVAILABLE:
                    self.logger.error("ChatMessage not available for old Mistral client")
                    return False

                response = self.client.chat(
                    model=self.config.model,
                    messages=[ChatMessage(role="user", content="Test connection")],
                    max_tokens=10
                )
                return response is not None
            else:
                return False
        except Exception as e:
            self.logger.error(f"Mistral connection test error: {e}")
            return False

    def _generate_response(self, prompt: str) -> str:
        """Generate response from Mistral AI"""
        try:
            if self.client_version == "new":
                # New client API (v1.0+)
                response = self.client.chat.complete(
                    model=self.config.model,
                    messages=[
                        {
                            "role": "user",
                            "content": prompt
                        }
                    ],
                    temperature=self.config.temperature,
                    max_tokens=self.config.max_tokens
                )
                return response.choices[0].message.content.strip()
            elif self.client_version == "old":
                # Old client API (v0.4.2 and below)
                if not MISTRAL_CHAT_MESSAGE_AVAILABLE:
                    raise Exception("ChatMessage not available for old Mistral client")

                response = self.client.chat(
                    model=self.config.model,
                    messages=[ChatMessage(role="user", content=prompt)],
                    temperature=self.config.temperature,
                    max_tokens=self.config.max_tokens
                )
                return response.choices[0].message.content.strip()
            else:
                raise Exception("Unknown Mistral client version")

        except Exception as e:
            # Retry logic could be implemented here
            self.logger.error(f"Mistral AI request failed: {e}")
            raise

    def get_provider_name(self) -> str:
        """Get the name of the provider"""
        return "mistral"

    def estimate_cost(self, rule: Any, data_type: str, count: int) -> float:
        """Estimate cost for Mistral AI generation"""
        # Rough cost estimation
        estimated_prompt_tokens = len(str(rule)) // 4
        estimated_response_tokens = count * 10

        # Cost per 1K tokens (approximate rates as of 2024)
        cost_rates = {
            "mistral-tiny": 0.0002,
            "mistral-small": 0.0006,
            "mistral-medium": 0.0027,
            "mistral-large": 0.008,
            "open-mistral-7b": 0.00025,
            "open-mixtral-8x7b": 0.0007,
            "mistral-small-latest": 0.001,
            "mistral-large-latest": 0.004
        }

        rate = cost_rates.get(self.config.model, 0.0006)  # Default fallback rate
        estimated_cost = ((estimated_prompt_tokens + estimated_response_tokens) / 1000) * rate

        self.logger.debug(f"Estimated Mistral AI cost: ${estimated_cost:.4f}")
        return estimated_cost

    def get_configuration(self) -> Dict[str, Any]:
        """Get Mistral-specific configuration details"""
        base_config = super().get_configuration()
        base_config.update({
            "provider": "mistral",
            "client_version": self.client_version,
            "api_key_available": bool(getattr(self.config, 'get_api_key', lambda: None)()),
            "cache_size": getattr(self.config, 'cache_size', 100)
        })
        return base_config