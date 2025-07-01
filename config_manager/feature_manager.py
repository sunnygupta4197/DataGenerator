import importlib
import logging
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, Any


class FeatureStatus(Enum):
    """Feature status enumeration"""
    AVAILABLE = "available"
    MISSING_DEPENDENCY = "missing_dependency"
    CONFIGURATION_ERROR = "configuration_error"
    DISABLED = "disabled"
    ENABLED = "enabled"


class DependencyType(Enum):
    """Types of dependencies"""
    PACKAGE = "package"
    API_KEY = "api_key"
    FILE = "file"
    ENVIRONMENT_VAR = "environment_var"
    CONFIG_VALUE = "config_value"
    FEATURE = "feature"  # Depends on another feature


@dataclass
class Dependency:
    """Represents a single dependency"""
    name: str
    type: DependencyType
    required: bool = True
    check_method: Optional[str] = None  # Custom check method name
    error_message: Optional[str] = None
    install_hint: Optional[str] = None


@dataclass
class FeatureResult:
    """Result of feature validation/enablement"""
    feature_name: str
    status: FeatureStatus
    enabled: bool
    dependencies_met: List[str]
    dependencies_missing: List[str]
    error_message: Optional[str] = None
    warnings: List[str] = None
    suggestions: List[str] = None

    def __post_init__(self):
        if self.warnings is None:
            self.warnings = []
        if self.suggestions is None:
            self.suggestions = []


class BaseFeature(ABC):
    """Base class for all features"""

    def __init__(self, name: str, config_path: str, dependencies: List[Dependency] = None):
        self.name = name
        self.config_path = config_path
        self.dependencies = dependencies or []
        self.logger = logging.getLogger(f"feature.{name}")

    @abstractmethod
    def is_enabled(self, config: Any) -> bool:
        """Check if feature is enabled in configuration"""
        pass

    @abstractmethod
    def enable(self, config: Any) -> bool:
        """Enable the feature in configuration"""
        pass

    @abstractmethod
    def disable(self, config: Any) -> bool:
        """Disable the feature in configuration"""
        pass

    def get_config_value(self, config: Any, path: str = None) -> Any:
        """Get configuration value using dot notation"""
        path = path or self.config_path
        parts = path.split('.')
        current = config

        for part in parts:
            if hasattr(current, part):
                current = getattr(current, part)
            elif isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return None
        return current

    def set_config_value(self, config: Any, value: Any, path: str = None) -> bool:
        """Set configuration value using dot notation"""
        path = path or self.config_path
        parts = path.split('.')
        current = config

        # Navigate to parent
        for part in parts[:-1]:
            if hasattr(current, part):
                current = getattr(current, part)
            elif isinstance(current, dict):
                if part not in current:
                    current[part] = {}
                current = current[part]
            else:
                return False

        # Set final value
        final_key = parts[-1]
        if hasattr(current, final_key):
            setattr(current, final_key, value)
        elif isinstance(current, dict):
            current[final_key] = value
        else:
            return False

        return True


class CoreFeature(BaseFeature):
    """Simple configuration-based feature"""

    def is_enabled(self, config: Any) -> bool:
        value = self.get_config_value(config)
        return bool(value) if value is not None else False

    def enable(self, config: Any) -> bool:
        return self.set_config_value(config, True)

    def disable(self, config: Any) -> bool:
        return self.set_config_value(config, False)


class AIFeature(BaseFeature):
    """AI provider feature with enhanced logic"""

    def __init__(self, name: str, config_path: str, provider_name: str,
                 api_key_env_var: str, package_name: str):
        dependencies = [
            Dependency(
                name=f"{provider_name}_api_key",
                type=DependencyType.API_KEY,
                check_method="check_api_key",
                error_message=f"No {provider_name} API key found",
                install_hint=f"Set {api_key_env_var} environment variable or add to config"
            ),
            Dependency(
                name=package_name,
                type=DependencyType.PACKAGE,
                error_message=f"{package_name} package not installed",
                install_hint=f"pip install {package_name}"
            )
        ]
        super().__init__(name, config_path, dependencies)
        self.provider_name = provider_name
        self.api_key_env_var = api_key_env_var
        self.package_name = package_name

    def check_api_key(self, config: Any) -> bool:
        """Check if API key is available from any source"""
        # Check config
        provider_config = self.get_config_value(config)
        if provider_config:
            if hasattr(provider_config, 'get_api_key'):
                return bool(provider_config.get_api_key())
            elif hasattr(provider_config, 'api_key') and provider_config.api_key:
                return True

        # Check environment
        return bool(os.getenv(self.api_key_env_var))

    def is_enabled(self, config: Any) -> bool:
        provider_config = self.get_config_value(config)
        if not provider_config:
            return False
        return getattr(provider_config, 'enabled', False)

    def enable(self, config: Any) -> bool:
        provider_config = self.get_config_value(config)
        if provider_config and hasattr(provider_config, 'enabled'):
            provider_config.enabled = True
            return True
        return self.set_config_value(config, True, f"{self.config_path}.enabled")

    def disable(self, config: Any) -> bool:
        provider_config = self.get_config_value(config)
        if provider_config and hasattr(provider_config, 'enabled'):
            provider_config.enabled = False
            return True
        return self.set_config_value(config, False, f"{self.config_path}.enabled")


class OutputFeature(BaseFeature):
    """Output format feature with package dependencies"""

    def __init__(self, format_name: str, package_name: str = None):
        dependencies = []
        if package_name:
            dependencies.append(Dependency(
                name=package_name,
                type=DependencyType.PACKAGE,
                error_message=f"{package_name} package required for {format_name} format",
                install_hint=f"pip install {package_name}"
            ))

        super().__init__(f"{format_name}_output", "output.format", dependencies)
        self.format_name = format_name

    def is_enabled(self, config: Any) -> bool:
        current_format = self.get_config_value(config)
        return current_format == self.format_name

    def enable(self, config: Any) -> bool:
        return self.set_config_value(config, self.format_name)

    def disable(self, config: Any) -> bool:
        # Cannot really "disable" an output format, but can change to default
        return self.set_config_value(config, "csv")


class FeatureManager:
    """Unified feature management system"""

    def __init__(self, logger: logging.Logger = None):
        self.logger = logger or logging.getLogger(__name__)
        self.features: Dict[str, BaseFeature] = {}
        self.dependency_checkers: Dict[DependencyType, callable] = {
            DependencyType.PACKAGE: self._check_package,
            DependencyType.API_KEY: self._check_api_key,
            DependencyType.FILE: self._check_file,
            DependencyType.ENVIRONMENT_VAR: self._check_env_var,
            DependencyType.CONFIG_VALUE: self._check_config_value,
            DependencyType.FEATURE: self._check_feature_dependency
        }
        self._register_default_features()

    def _register_default_features(self):
        """Register all default features"""

        # Core features
        self.register_feature(CoreFeature("parallel", "performance.enable_parallel"))
        self.register_feature(CoreFeature("streaming", "performance.enable_streaming"))
        self.register_feature(CoreFeature("data_masking", "security.enable_data_masking"))
        self.register_feature(CoreFeature("audit_logging", "security.audit_enabled"))
        self.register_feature(CoreFeature("business_rules", "validation.enable_business_rules"))
        self.register_feature(CoreFeature("quality_analysis", "validation.enable_data_quality_analysis"))
        self.register_feature(CoreFeature("anomaly_detection", "validation.enable_anomaly_detection"))

        # Encryption feature (requires key)
        encryption_deps = [Dependency(
            name="encryption_key",
            type=DependencyType.CONFIG_VALUE,
            check_method="check_encryption_key",
            error_message="No encryption key available",
            install_hint="Set encryption key in config, file, or environment variable"
        )]
        encryption_feature = CoreFeature("encryption", "security.enable_encryption")
        encryption_feature.dependencies = encryption_deps
        encryption_feature.check_encryption_key = self._check_encryption_key
        self.register_feature(encryption_feature)

        # AI features
        self.register_feature(AIFeature(
            "openai", "ai.openai", "OpenAI", "OPENAI_API_KEY", "openai"
        ))
        self.register_feature(AIFeature(
            "mistral", "ai.mistral", "Mistral", "MISTRAL_API_KEY", "mistralai"
        ))

        # Output features
        self.register_feature(OutputFeature("csv"))
        self.register_feature(OutputFeature("json"))
        self.register_feature(OutputFeature("parquet", "pyarrow"))
        self.register_feature(OutputFeature("excel", "openpyxl"))

        # AI fallback (depends on multiple AI providers)
        fallback_deps = [Dependency(
            name="multiple_ai_providers",
            type=DependencyType.FEATURE,
            check_method="check_multiple_ai",
            error_message="Multiple AI providers required for fallback",
            install_hint="Enable at least 2 AI providers"
        )]
        fallback_feature = CoreFeature("ai_fallback", "ai.enable_fallback")
        fallback_feature.dependencies = fallback_deps
        fallback_feature.check_multiple_ai = self._check_multiple_ai_providers
        self.register_feature(fallback_feature)

    def register_feature(self, feature: BaseFeature):
        """Register a new feature"""
        self.features[feature.name] = feature
        self.logger.debug(f"Registered feature: {feature.name}")

    def check_feature_dependencies(self, feature_name: str, config: Any) -> FeatureResult:
        """Check if a feature's dependencies are met"""
        if feature_name not in self.features:
            return FeatureResult(
                feature_name=feature_name,
                status=FeatureStatus.CONFIGURATION_ERROR,
                enabled=False,
                dependencies_met=[],
                dependencies_missing=[],
                error_message=f"Unknown feature: {feature_name}"
            )

        feature = self.features[feature_name]
        dependencies_met = []
        dependencies_missing = []
        warnings = []
        suggestions = []

        # Check each dependency
        for dep in feature.dependencies:
            try:
                if dep.check_method and hasattr(feature, dep.check_method):
                    # Use custom check method
                    check_result = getattr(feature, dep.check_method)(config)
                else:
                    # Use default checker
                    checker = self.dependency_checkers.get(dep.type)
                    if not checker:
                        dependencies_missing.append(f"{dep.name} (unknown dependency type)")
                        continue
                    check_result = checker(dep, config, feature)

                if check_result:
                    dependencies_met.append(dep.name)
                else:
                    dependencies_missing.append(dep.name)
                    if dep.error_message:
                        warnings.append(dep.error_message)
                    if dep.install_hint:
                        suggestions.append(dep.install_hint)

            except Exception as e:
                dependencies_missing.append(f"{dep.name} (check failed: {e})")
                warnings.append(f"Dependency check failed for {dep.name}: {e}")

        # Determine status
        all_required_met = all(
            dep.name in dependencies_met
            for dep in feature.dependencies
            if dep.required
        )

        if all_required_met:
            is_enabled = feature.is_enabled(config)
            status = FeatureStatus.ENABLED if is_enabled else FeatureStatus.AVAILABLE
        else:
            is_enabled = False
            status = FeatureStatus.MISSING_DEPENDENCY

        if dependencies_missing and feature_name in ['openai', 'mistral']:
            api_key_var = 'OPENAI_API_KEY' if feature_name == 'openai' else 'MISTRAL_API_KEY'
            suggestions.append(f"Set environment variable: export {api_key_var}='your-key-here'")
            suggestions.append(f"Or add 'api_key' to your {feature_name} config section")

        return FeatureResult(
            feature_name=feature_name,
            status=status,
            enabled=is_enabled,
            dependencies_met=dependencies_met,
            dependencies_missing=dependencies_missing,
            warnings=warnings,
            suggestions=suggestions
        )

    def enable_feature(self, feature_name: str, config: Any,
                       force: bool = False) -> FeatureResult:
        """Enable a feature if dependencies are met"""
        result = self.check_feature_dependencies(feature_name, config)

        if result.enabled and result.status == FeatureStatus.ENABLED:
            # Feature is already enabled, don't log again
            result.error_message = None  # Clear any error
            return result

        if result.status == FeatureStatus.MISSING_DEPENDENCY and not force:
            result.error_message = f"Cannot enable {feature_name}: missing dependencies"
            return result

        if feature_name not in self.features:
            result.error_message = f"Unknown feature: {feature_name}"
            return result

        feature = self.features[feature_name]

        try:
            success = feature.enable(config)
            if success:
                result.enabled = True
                result.status = FeatureStatus.ENABLED
                self.logger.info(f"✅ Enabled feature: {feature_name}")
            else:
                result.error_message = f"Failed to enable {feature_name}"
                self.logger.warning(f"⚠️ Failed to enable feature: {feature_name}")
        except Exception as e:
            result.error_message = f"Error enabling {feature_name}: {e}"
            self.logger.error(f"❌ Error enabling feature {feature_name}: {e}")

        return result

    def disable_feature(self, feature_name: str, config: Any) -> FeatureResult:
        """Disable a feature"""
        result = self.check_feature_dependencies(feature_name, config)

        if feature_name not in self.features:
            result.error_message = f"Unknown feature: {feature_name}"
            return result

        feature = self.features[feature_name]

        try:
            success = feature.disable(config)
            if success:
                result.enabled = False
                result.status = FeatureStatus.DISABLED
                self.logger.info(f"🔴 Disabled feature: {feature_name}")
            else:
                result.error_message = f"Failed to disable {feature_name}"
        except Exception as e:
            result.error_message = f"Error disabling {feature_name}: {e}"

        return result

    def enable_all_features(self, config: Any,
                            skip_missing_deps: bool = True) -> Dict[str, FeatureResult]:
        """Enable all available features"""
        results = {}
        enabled_count = 0
        skipped_count = 0

        self.logger.info("🚀 Attempting to enable all features...")

        for feature_name in self.features:
            result = self.check_feature_dependencies(feature_name, config)

            if result.status == FeatureStatus.MISSING_DEPENDENCY and skip_missing_deps:
                skipped_count += 1
                self.logger.info(f"⏭️ Skipping {feature_name}: {', '.join(result.dependencies_missing)}")
                results[feature_name] = result
                continue

            if result.status in [FeatureStatus.AVAILABLE, FeatureStatus.MISSING_DEPENDENCY]:
                enable_result = self.enable_feature(
                    feature_name, config,
                    force=not skip_missing_deps
                )
                if enable_result.enabled:
                    enabled_count += 1
                results[feature_name] = enable_result
            else:
                results[feature_name] = result

        self.logger.info(f"✅ Feature enablement complete: {enabled_count} enabled, {skipped_count} skipped")
        return results

    def get_feature_status_summary(self, config: Any) -> Dict[str, Any]:
        """Get comprehensive status summary of all features"""
        summary = {
            'total_features': len(self.features),
            'enabled': [],
            'available': [],
            'missing_dependencies': [],
            'errors': [],
            'by_category': {}
        }

        for feature_name in self.features:
            result = self.check_feature_dependencies(feature_name, config)

            if result.status == FeatureStatus.ENABLED:
                summary['enabled'].append(feature_name)
            elif result.status == FeatureStatus.AVAILABLE:
                summary['available'].append(feature_name)
            elif result.status == FeatureStatus.MISSING_DEPENDENCY:
                summary['missing_dependencies'].append({
                    'feature': feature_name,
                    'missing': result.dependencies_missing
                })
            else:
                summary['errors'].append({
                    'feature': feature_name,
                    'error': result.error_message
                })

            # Categorize by type
            category = self._get_feature_category(feature_name)
            if category not in summary['by_category']:
                summary['by_category'][category] = []
            summary['by_category'][category].append({
                'name': feature_name,
                'status': result.status.value,
                'enabled': result.enabled
            })

        return summary

    def _get_feature_category(self, feature_name: str) -> str:
        """Categorize features for better organization"""
        if feature_name in ['streaming', 'parallel']:
            return 'performance'
        elif feature_name in ['data_masking', 'encryption', 'audit_logging']:
            return 'security'
        elif feature_name in ['business_rules', 'quality_analysis', 'anomaly_detection']:
            return 'validation'
        elif feature_name in ['openai', 'mistral', 'ai_fallback']:
            return 'ai'
        elif 'output' in feature_name:
            return 'output'
        else:
            return 'other'

    # Dependency checkers
    def _check_package(self, dep: Dependency, config: Any, feature: BaseFeature) -> bool:
        """Check if a Python package is available"""
        try:
            importlib.import_module(dep.name)
            return True
        except ImportError:
            return False

    def _check_api_key(self, dep: Dependency, config: Any, feature: BaseFeature) -> bool:
        """Check if API key is available"""
        # This is handled by custom check methods in AI features
        return False

    def _check_file(self, dep: Dependency, config: Any, feature: BaseFeature) -> bool:
        """Check if a file exists"""
        file_path = feature.get_config_value(config, dep.name)
        return file_path and os.path.exists(file_path)

    def _check_env_var(self, dep: Dependency, config: Any, feature: BaseFeature) -> bool:
        """Check if environment variable exists"""
        return bool(os.getenv(dep.name))

    def _check_config_value(self, dep: Dependency, config: Any, feature: BaseFeature) -> bool:
        """Check if configuration value exists"""
        value = feature.get_config_value(config, dep.name)
        return value is not None and value != ""

    def _check_feature_dependency(self, dep: Dependency, config: Any, feature: BaseFeature) -> bool:
        """Check if another feature is enabled"""
        return dep.name in self.features and self.features[dep.name].is_enabled(config)

    def _check_encryption_key(self, config: Any) -> bool:
        """Check if encryption key is available"""
        security_config = getattr(config, 'security', None)
        if not security_config:
            return False

        # Check direct key
        if hasattr(security_config, 'encryption_key') and security_config.encryption_key:
            return True

        # Check key file
        if hasattr(security_config, 'encryption_key_file') and security_config.encryption_key_file:
            return os.path.exists(security_config.encryption_key_file)

        # Check environment variable
        key_env_var = getattr(security_config, 'key_env_var', 'DATA_ENCRYPTION_KEY')
        return bool(os.getenv(key_env_var))

    def _check_multiple_ai_providers(self, config: Any) -> bool:
        """Check if multiple AI providers are available"""
        ai_features = ['openai', 'mistral']
        available_count = 0

        for ai_feature_name in ai_features:
            if ai_feature_name in self.features:
                result = self.check_feature_dependencies(ai_feature_name, config)
                if result.status in [FeatureStatus.AVAILABLE, FeatureStatus.ENABLED]:
                    available_count += 1

        return available_count >= 2


# Usage example and integration helper
class ConfigurationIntegration:
    """Helper class to integrate with existing configuration system"""

    def __init__(self, config_manager, feature_manager: FeatureManager = None):
        self.config_manager = config_manager
        self.feature_manager = feature_manager or FeatureManager(config_manager.logger)

    def apply_feature_overrides(self, config, enable_all: bool = False,
                                features_to_enable: List[str] = None,
                                features_to_disable: List[str] = None) -> List[str]:
        """Apply feature-based overrides to configuration"""
        applied_overrides = []

        features_actually_enabled = []
        features_actually_disabled = []
        features_already_enabled = []
        features_already_disabled = []

        features_to_enable = list(set(features_to_enable or []))
        features_to_disable = list(set(features_to_disable or []))

        conflicts = set(features_to_enable) & set(features_to_disable)
        if conflicts:
            applied_overrides.append(
                f"⚠️ Conflicting feature requests: {', '.join(sorted(conflicts))} are in both enable and disable lists. "
                f"Disable will take precedence."
            )

        all_available_features = set(self.feature_manager.features.keys())
        if enable_all:
            # Start with all features, then subtract explicit disables
            target_enabled_features = all_available_features - set(features_to_disable)
            applied_overrides.append("🚀 enable_all_features requested")
        else:
            # Start with only explicitly requested features, minus conflicts
            target_enabled_features = set(features_to_enable) - set(features_to_disable)

        for feature_name in sorted(target_enabled_features):
            if feature_name not in self.feature_manager.features:
                applied_overrides.append(f"feature.{feature_name}: unknown feature ❓")
                continue

            result = self.feature_manager.check_feature_dependencies(feature_name, config)
            if result.enabled and result.status == FeatureStatus.ENABLED:
                # Feature is truly enabled (config=true AND dependencies met)
                features_already_enabled.append(feature_name)
                continue
            elif result.status == FeatureStatus.AVAILABLE:
                # Dependencies met but config disabled - enable it
                enable_result = self.feature_manager.enable_feature(feature_name, config)
                if enable_result.enabled:
                    features_actually_enabled.append(feature_name)
            elif result.status == FeatureStatus.MISSING_DEPENDENCY:
                # Check if config is enabled to give better error message
                feature = self.feature_manager.features[feature_name]
                config_enabled = feature.is_enabled(config)

                if config_enabled:
                    enhanced_message = self._get_enhanced_dependency_message(feature_name, result)
                    applied_overrides.append(f"{enhanced_message} ❌")

                    disable_result = self.feature_manager.disable_feature(feature_name, config)
                    if not disable_result.enabled:
                        applied_overrides.append(f"feature.{feature_name}: auto-disabled due to missing dependencies 🔴")
                else:
                    missing_deps = ', '.join(result.dependencies_missing)
                    applied_overrides.append(f"feature.{feature_name}: skipped - missing {missing_deps} ⏭️")

        # Process disables with state checking
        for feature_name in sorted(features_to_disable):
            if feature_name not in self.feature_manager.features:
                continue

            result = self.feature_manager.check_feature_dependencies(feature_name, config)

            if not result.enabled:
                # Feature is already effectively disabled
                features_already_disabled.append(feature_name)
                continue

            # Try to disable
            disable_result = self.feature_manager.disable_feature(feature_name, config)
            if not disable_result.enabled:
                features_actually_disabled.append(feature_name)
        # LOG SUMMARY ONLY - No individual feature logs to avoid duplication
        if features_actually_enabled:
            applied_overrides.append(f"✅ Newly enabled: {', '.join(features_actually_enabled)}")

        if features_actually_disabled:
            applied_overrides.append(f"🔴 Newly disabled: {', '.join(features_actually_disabled)}")

        if features_already_enabled:
            applied_overrides.append(f"📋 Already enabled: {', '.join(features_already_enabled)}")

        if features_already_disabled:
            applied_overrides.append(f"📋 Already disabled: {', '.join(features_already_disabled)}")

        return applied_overrides

    def get_feature_summary_for_logging(self, config) -> Dict[str, Any]:
        """Get feature summary formatted for logging"""
        summary = self.feature_manager.get_feature_status_summary(config)

        return {
            'total_features': summary['total_features'],
            'enabled_count': len(summary['enabled']),
            'enabled_features': summary['enabled'],
            'missing_dependencies': summary['missing_dependencies'],
            'by_category': summary['by_category']
        }

    def _get_enhanced_dependency_message(self, feature_name: str, result: FeatureResult) -> str:
        """Get enhanced error message for missing dependencies with actionable solutions"""

        if feature_name == 'encryption':
            return (
                f"feature.{feature_name}: encryption requested but no key found. "
                f"Add 'encryption_key', 'encryption_key_file', or set DATA_ENCRYPTION_KEY env var"
            )
        elif feature_name == 'openai':
            missing_deps = result.dependencies_missing
            if 'openai_api_key' in missing_deps:
                return (
                    f"feature.{feature_name}: missing API key. "
                    f"Set OPENAI_API_KEY environment variable or add 'api_key' to openai config section"
                )
            elif 'openai' in missing_deps:
                return (
                    f"feature.{feature_name}: missing openai package. "
                    f"Install with: pip install openai"
                )
            else:
                return f"feature.{feature_name}: missing {', '.join(missing_deps)}"

        elif feature_name == 'mistral':
            missing_deps = result.dependencies_missing
            if 'mistral_api_key' in missing_deps:
                return (
                    f"feature.{feature_name}: missing API key. "
                    f"Set MISTRAL_API_KEY environment variable or add 'api_key' to mistral config section"
                )
            elif 'mistralai' in missing_deps:
                return (
                    f"feature.{feature_name}: missing mistralai package. "
                    f"Install with: pip install mistralai"
                )
            else:
                return f"feature.{feature_name}: missing {', '.join(missing_deps)}"

        elif feature_name in ['parquet_output', 'excel_output']:
            package_map = {'parquet_output': 'pyarrow', 'excel_output': 'openpyxl'}
            package_name = package_map.get(feature_name, 'unknown')
            return (
                f"feature.{feature_name}: missing {package_name} package. "
                f"Install with: pip install {package_name}"
            )
        else:
            # Generic fallback for other features
            missing = ', '.join(result.dependencies_missing)
            return f"feature.{feature_name}: missing {missing}"