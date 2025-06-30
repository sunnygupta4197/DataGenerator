import os
import json
import logging
from typing import Dict, Any, List, Optional, Union, Tuple
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, asdict, field
from enum import Enum
import tempfile
import shutil
from contextlib import contextmanager
from .json_reader import JSONConfigReader
from .schema_validator import SchemaValidator


class Environment(Enum):
    """Environment types"""
    DEVELOPMENT = "development"
    TESTING = "testing"
    STAGING = "staging"
    PRODUCTION = "production"


class AIProvider(Enum):
    """Supported AI providers"""
    OPENAI = "openai"
    MISTRAL = "mistral"


@dataclass
class DatabaseConfig:
    """Database connection configuration"""
    host: str = "localhost"
    port: int = 5432
    database: str = "testdata"
    username: str = "postgres"
    password: str = ""
    ssl_mode: str = "disable"
    connection_timeout: int = 30


@dataclass
class PerformanceConfig:
    """Performance-related configuration"""
    max_workers: int = 4
    batch_size: int = 10000
    streaming_batch_size: int = 1000
    max_memory_mb: int = 1000
    enable_parallel: bool = True
    enable_streaming: bool = True
    cache_size_limit: int = 50000
    use_processes: bool = False
    optimization_level: str = "balanced"

    def __post_init__(self):
        # Validate performance settings
        if self.max_workers < 1:
            self.max_workers = 1
        elif self.max_workers > 32:  # Reasonable upper limit
            self.max_workers = 32

        if self.batch_size < 100:
            self.batch_size = 100
        elif self.batch_size > 1000000:
            self.batch_size = 1000000

        if self.max_memory_mb < 100:
            self.max_memory_mb = 100


@dataclass
class SecurityConfig:
    """Enhanced Security configuration with masking rules support"""

    # Basic security settings
    enable_data_masking: bool = False
    enable_encryption: bool = False
    audit_enabled: bool = False

    # Masking configuration
    masking_rules: Dict[str, str] = field(default_factory=dict)
    sensitivity_map: Dict[str, str] = field(default_factory=dict)
    encrypt_fields: List[str] = field(default_factory=list)

    # Advanced security settings
    encryption_algorithm: str = "AES-256-GCM"
    encryption_key: Optional[str] = None
    encryption_key_file: Optional[str] = None
    key_source: str = "environment"  # environment, config, file
    key_env_var: str = "DATA_ENCRYPTION_KEY"

    # Compliance settings
    compliance_profile: Optional[str] = None  # GDPR, HIPAA, PCI_DSS, SOX
    auto_detect_pii: bool = False
    strict_mode: bool = False
    require_explicit_consent: bool = False

    # Audit configuration
    audit_log_level: str = "detailed"  # basic, detailed, verbose
    audit_file: Optional[str] = None
    include_masked_values: bool = False
    include_original_values: bool = False
    retention_days: int = 90

    # Pattern-based masking
    pattern_based_masking: Dict[str, str] = field(default_factory=dict)

    # Validation settings
    validate_masking_effectiveness: bool = True
    min_masking_percentage: float = 70.0
    validate_patterns: bool = True

    def __post_init__(self):
        """Post-initialization validation and setup"""
        # Validate masking rule types
        valid_masking_types = ["partial", "full", "hash", "custom", "format_preserving", "deterministic"]
        for field_name, mask_type in self.masking_rules.items():
            if isinstance(mask_type, str) and mask_type not in valid_masking_types:
                raise ValueError(f"Invalid masking type '{mask_type}' for field '{field_name}'. "
                                 f"Valid types: {valid_masking_types}")

        # Validate sensitivity levels
        valid_sensitivity_levels = ["PUBLIC", "INTERNAL", "PII", "SENSITIVE"]
        for field_name, sensitivity in self.sensitivity_map.items():
            if sensitivity not in valid_sensitivity_levels:
                raise ValueError(f"Invalid sensitivity level '{sensitivity}' for field '{field_name}'. "
                                 f"Valid levels: {valid_sensitivity_levels}")

        # Set up default masking rules if auto-detect is enabled
        if self.auto_detect_pii and not self.masking_rules:
            self.masking_rules.update(self._get_default_masking_rules())

        # Set up default sensitivity map if auto-detect is enabled
        if self.auto_detect_pii and not self.sensitivity_map:
            self.sensitivity_map.update(self._get_default_sensitivity_map())

        # Apply compliance profile settings
        if self.compliance_profile:
            self._apply_compliance_profile()

    def _get_default_masking_rules(self) -> Dict[str, str]:
        """Get default masking rules for common PII fields"""
        return {
            "email": "partial",
            "phone": "partial",
            "phone_number": "partial",
            "ssn": "full",
            "social_security": "full",
            "credit_card": "full",
            "card_number": "full",
            "salary": "hash",
            "income": "hash",
            "name": "partial",
            "first_name": "partial",
            "last_name": "partial",
            "address": "partial",
            "zipcode": "partial",
            "postal_code": "partial"
        }

    def _get_default_sensitivity_map(self) -> Dict[str, str]:
        """Get default sensitivity levels for common fields"""
        return {
            "email": "PII",
            "phone": "PII",
            "phone_number": "PII",
            "ssn": "SENSITIVE",
            "social_security": "SENSITIVE",
            "credit_card": "SENSITIVE",
            "card_number": "SENSITIVE",
            "salary": "SENSITIVE",
            "income": "SENSITIVE",
            "name": "PII",
            "first_name": "PII",
            "last_name": "PII",
            "address": "PII",
            "zipcode": "PII",
            "postal_code": "PII",
            "id": "INTERNAL",
            "user_id": "INTERNAL",
            "employee_id": "INTERNAL",
            "department": "INTERNAL",
            "status": "PUBLIC",
            "category": "PUBLIC",
            "product_id": "PUBLIC"
        }

    def _apply_compliance_profile(self):
        """Apply settings based on compliance profile"""
        if self.compliance_profile == "GDPR":
            self.strict_mode = True
            self.audit_enabled = True
            self.require_explicit_consent = True
            self.min_masking_percentage = 95.0

        elif self.compliance_profile == "HIPAA":
            self.strict_mode = True
            self.audit_enabled = True
            self.enable_encryption = True
            self.min_masking_percentage = 99.0

        elif self.compliance_profile == "PCI_DSS":
            self.strict_mode = True
            self.audit_enabled = True
            self.enable_encryption = True
            # Focus on payment card data
            self.masking_rules.update({
                "credit_card": "full",
                "card_number": "full",
                "cvv": "full",
                "expiry": "full"
            })

        elif self.compliance_profile == "SOX":
            self.audit_enabled = True
            self.retention_days = 2555  # 7 years
            # Focus on financial data
            self.masking_rules.update({
                "salary": "hash",
                "income": "hash",
                "revenue": "hash",
                "profit": "hash"
            })

    def add_masking_rule(self, field_pattern: str, masking_type: str):
        """Add a new masking rule"""
        valid_types = ["partial", "full", "hash", "custom", "format_preserving", "deterministic"]
        if masking_type not in valid_types:
            raise ValueError(f"Invalid masking type: {masking_type}. Valid types: {valid_types}")

        self.masking_rules[field_pattern] = masking_type

    def set_sensitivity_level(self, field_name: str, sensitivity_level: str):
        """Set sensitivity level for a field"""
        valid_levels = ["PUBLIC", "INTERNAL", "PII", "SENSITIVE"]
        if sensitivity_level not in valid_levels:
            raise ValueError(f"Invalid sensitivity level: {sensitivity_level}. Valid levels: {valid_levels}")

        self.sensitivity_map[field_name] = sensitivity_level

    def get_masking_rule_for_field(self, field_name: str) -> Optional[str]:
        """Get masking rule for a specific field, including pattern matching"""
        # Direct match first
        if field_name in self.masking_rules:
            return self.masking_rules[field_name]

        # Pattern matching
        field_lower = field_name.lower()
        for pattern, mask_type in self.masking_rules.items():
            pattern_lower = pattern.lower()

            # Wildcard pattern matching
            if pattern.startswith('*') and pattern.endswith('*'):
                # *email* pattern
                if pattern_lower.strip('*') in field_lower:
                    return mask_type
            elif pattern.startswith('*'):
                # *_email pattern
                if field_lower.endswith(pattern_lower[1:]):
                    return mask_type
            elif pattern.endswith('*'):
                # email_* pattern
                if field_lower.startswith(pattern_lower[:-1]):
                    return mask_type
            elif pattern_lower in field_lower:
                # Simple substring match
                return mask_type

        # Pattern-based masking
        for pattern, mask_type in self.pattern_based_masking.items():
            if self._match_pattern(field_name, pattern):
                return mask_type

        return None

    def _match_pattern(self, field_name: str, pattern: str) -> bool:
        """Match field name against pattern"""
        import re

        # Convert glob pattern to regex
        regex_pattern = pattern.replace('*', '.*').replace('?', '.')
        return bool(re.match(f"^{regex_pattern}$", field_name, re.IGNORECASE))

    def get_sensitivity_level(self, field_name: str) -> str:
        """Get sensitivity level for a field"""
        return self.sensitivity_map.get(field_name, "PUBLIC")

    def is_field_encrypted(self, field_name: str) -> bool:
        """Check if a field should be encrypted"""
        return field_name in self.encrypt_fields

    def validate_configuration(self) -> List[str]:
        """Validate security configuration and return any issues"""
        issues = []

        # Check for encryption without key
        if self.enable_encryption and not self.encryption_key and self.key_source == "config":
            issues.append("Encryption enabled but no encryption key provided")

        # Check for sensitive fields without masking
        for field_name, sensitivity in self.sensitivity_map.items():
            if sensitivity in ["PII", "SENSITIVE"] and not self.get_masking_rule_for_field(field_name):
                issues.append(f"Sensitive field '{field_name}' has no masking rule")

        # Check masking effectiveness
        if self.validate_masking_effectiveness:
            masked_fields = len(self.masking_rules)
            total_sensitive_fields = len([f for f, s in self.sensitivity_map.items()
                                          if s in ["PII", "SENSITIVE"]])

            if total_sensitive_fields > 0:
                masking_percentage = (masked_fields / total_sensitive_fields) * 100
                if masking_percentage < self.min_masking_percentage:
                    issues.append(
                        f"Masking coverage {masking_percentage:.1f}% below threshold {self.min_masking_percentage}%")

        return issues

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation"""
        return {
            "enable_data_masking": self.enable_data_masking,
            "enable_encryption": self.enable_encryption,
            "audit_enabled": self.audit_enabled,
            "masking_rules": self.masking_rules,
            "sensitivity_map": self.sensitivity_map,
            "encrypt_fields": self.encrypt_fields,
            "encryption_algorithm": self.encryption_algorithm,
            "compliance_profile": self.compliance_profile,
            "auto_detect_pii": self.auto_detect_pii,
            "strict_mode": self.strict_mode
        }

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'SecurityConfig':
        """Create SecurityConfig from dictionary"""
        # Handle legacy configuration keys
        legacy_mapping = {
            "data_masking_enabled": "enable_data_masking",
            "encryption_enabled": "enable_encryption",
            "masking_enabled": "enable_data_masking"
        }

        # Apply legacy key mapping
        for old_key, new_key in legacy_mapping.items():
            if old_key in config_dict:
                config_dict[new_key] = config_dict.pop(old_key)

        return cls(**config_dict)


@dataclass
class OpenAIConfig:
    """OpenAI integration configuration"""
    enabled: bool = False
    api_key: Optional[str] = None
    api_key_env_var: str = "OPENAI_API_KEY"
    api_key_file: Optional[str] = None
    model: str = "gpt-3.5-turbo"
    max_tokens: int = 2000
    temperature: float = 0.7
    cache_size: int = 100
    timeout_seconds: int = 30
    retry_attempts: int = 3
    fallback_enabled: bool = True
    cost_limit_usd: Optional[float] = None

    def __post_init__(self):
        """Load API key from environment if not provided"""
        if self.enabled and not self.api_key:
            self.api_key = os.getenv(self.api_key_env_var)
            if not self.api_key:
                print(f"Warning: OpenAI enabled but no API key found in {self.api_key_env_var}")
                self.enabled = False

    def get_api_key(self) -> Optional[str]:
        """Get API key from various sources in priority order"""
        # 1. Direct API key (highest priority)
        if self.api_key:
            return self.api_key

        # 2. API key from file
        if self.api_key_file and os.path.exists(self.api_key_file):
            try:
                with open(self.api_key_file, 'r') as f:
                    key = f.read().strip()
                    if key:
                        return key
            except Exception:
                pass

        # 3. Environment variable (lowest priority)
        return os.getenv(self.api_key_env_var)

    def is_available(self) -> bool:
        """Check if OpenAI is properly configured and available"""
        return self.enabled and self.get_api_key() is not None


@dataclass
class MistralConfig:
    """Mistral AI integration configuration"""
    enabled: bool = False
    api_key: Optional[str] = None
    api_key_env_var: str = "MISTRAL_API_KEY"
    api_key_file: Optional[str] = None
    model: str = "mistral-small"  # mistral-tiny, mistral-small, mistral-medium, mistral-large
    max_tokens: int = 2000
    temperature: float = 0.7
    cache_size: int = 100
    timeout_seconds: int = 30
    retry_attempts: int = 3
    fallback_enabled: bool = True
    cost_limit_usd: Optional[float] = None

    def __post_init__(self):
        """Load API key from environment if not provided"""
        if self.enabled and not self.api_key:
            self.api_key = os.getenv(self.api_key_env_var)
            if not self.api_key:
                print(f"Warning: Mistral AI enabled but no API key found in {self.api_key_env_var}")
                self.enabled = False

    def get_api_key(self) -> Optional[str]:
        """Get API key from various sources in priority order"""
        # 1. Direct API key (highest priority)
        if self.api_key:
            return self.api_key

        # 2. API key from file
        if self.api_key_file and os.path.exists(self.api_key_file):
            try:
                with open(self.api_key_file, 'r') as f:
                    key = f.read().strip()
                    if key:
                        return key
            except Exception:
                pass

        # 3. Environment variable (lowest priority)
        return os.getenv(self.api_key_env_var)

    def is_available(self) -> bool:
        """Check if Mistral AI is properly configured and available"""
        return self.enabled and self.get_api_key() is not None


@dataclass
class AIConfig:
    """Master AI configuration supporting multiple providers"""
    openai: Optional[OpenAIConfig] = None
    mistral: Optional[MistralConfig] = None
    primary_provider: str = "openai"  # "openai" or "mistral"
    enable_fallback: bool = True
    shared_cache_size: int = 200  # Shared cache for all AI providers

    def __post_init__(self):
        """Initialize default configurations if not provided"""
        if self.openai is None:
            self.openai = OpenAIConfig()
        if self.mistral is None:
            self.mistral = MistralConfig()

    def get_active_providers(self) -> List[str]:
        """Get list of enabled AI providers"""
        providers = []
        if self.openai and self.openai.enabled:
            providers.append("openai")
        if self.mistral and self.mistral.enabled:
            providers.append("mistral")
        return providers

    def get_primary_provider_config(self):
        """Get configuration for primary provider"""
        if self.primary_provider == "openai":
            return self.openai
        elif self.primary_provider == "mistral":
            return self.mistral
        return None

    def switch_primary_provider(self, provider: str) -> bool:
        """Switch primary provider"""
        if provider == "openai" and self.openai and self.openai.enabled:
            self.primary_provider = "openai"
            return True
        elif provider == "mistral" and self.mistral and self.mistral.enabled:
            self.primary_provider = "mistral"
            return True
        return False


@dataclass
class OutputConfig:
    """Output configuration"""
    format: str = "csv"
    directory: str = "./output/{timestamp}"
    report_directory: str = f'{directory}/reports'
    filename_template: str = "{table_name}_{timestamp}"
    compression: Optional[str] = None
    encoding: str = "utf-8"
    include_header: bool = True
    append_timestamp: bool = True
    create_directory: bool = True

    # Format-specific settings
    csv_delimiter: str = ","
    csv_quotechar: str = '"'
    csv_line_terminator: str = '\n'
    json_indent: Optional[int] = 2
    json_ensure_ascii: bool = False
    parquet_engine: str = "pyarrow"
    parquet_compression: str = "snappy"
    parquet_row_group_size: int = 10000

    # Fixed-width specific settings
    default_column_width: int = 15
    padding_char: str = ' '
    alignment: str = 'left'
    numeric_alignment: str = 'right'
    column_widths: Optional[Dict[str, int]] = field(default_factory=dict)
    truncate_char: Optional[str] = '…'
    enable_fixed_width: bool = False

    # Auto-sizing options
    auto_size_columns: bool = False
    max_auto_width: Optional[int] = 50
    min_auto_width: Optional[int] = 8

    # Excel specific settings
    excel_worksheet_name: str = 'Sheet1'
    excel_max_rows_per_sheet: int = 1000000

    # General settings
    buffer_size: int = 8192
    enable_progress: bool = True
    log_every_n_batches: int = 100

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    def __post_init__(self):
        """Validate output configuration and setup directory"""
        self._setup_output_directory()

    def _setup_output_directory(self):
        """Setup output directory"""
        valid_formats = ["csv", "json", "jsonl", "parquet", "sql_query", "xlsx", "dsv", "fixed", "fwf"]
        if self.format not in valid_formats:
            print(f"Warning: Invalid format '{self.format}', defaulting to 'csv'")
            self.format = "csv"

        # Create output directory if it doesn't exist
        self.directory = self.directory.format(timestamp=self.timestamp)
        self.report_directory = f'{self.directory}/reports'

        if self.create_directory and not os.path.exists(self.directory):
            try:
                os.makedirs(self.directory, exist_ok=True)
                os.makedirs(self.report_directory, exist_ok=True)
            except Exception as e:
                print(f"Warning: Could not create output directory {self.directory}: {e}")

    def change_directory(self, new_directory: str, cleanup_old: bool = True):
        """Change output directory and optionally cleanup_old one"""
        old_directory = getattr(self, 'directory', None)

        self.directory = f'{new_directory}/{self.timestamp}'
        self.report_directory = f'{self.directory}/reports'

        if cleanup_old and old_directory and os.path.exists(old_directory) and old_directory != new_directory:
            self._cleanup_directory_tree(old_directory)

        self._setup_output_directory()

    def _cleanup_directory_tree(self, directory_path: str):
        """Delete directory and its parent directories if they are empty"""
        current_dir = os.path.abspath(directory_path)
        try:
            while current_dir and current_dir != os.path.dirname(current_dir):
                if os.path.exists(current_dir):
                    if not os.listdir(current_dir):
                        os.rmdir(current_dir)
                        print(f'Deleted empty directory: {current_dir}')
                        current_dir = os.path.dirname(current_dir)
                    else:
                        print(f'Directory not empty, keeping: {current_dir}')
                        break
                else:
                    break
        except OSError as e:
            print(f"Warning: Could not remove {current_dir}: {e}")

    def get_output_path(self, table_name: str) -> str:
        """Generate output file path for a table"""
        filename = self.filename_template.format(
            table_name=table_name,
            timestamp=self.timestamp
        )

        # Add file extension
        if not filename.endswith(f".{self.format}"):
            filename = f"{filename}.{self.format}"

        # Add compression extension
        if self.compression:
            filename = f"{filename}.{self.compression}"

        return os.path.join(self.directory, filename)


@dataclass
class BusinessRule:
    """Business rule definition"""
    type: str  # conditional, range_dependency, mutual_exclusivity
    condition_column: Optional[str] = None
    condition_operator: Optional[str] = None
    condition_value: Optional[Any] = None
    requirement_column: Optional[str] = None
    requirement_value: Optional[Any] = None

    # For range_dependency rules
    income_column: Optional[str] = None
    score_column: Optional[str] = None
    income_threshold: Optional[float] = None
    score_threshold: Optional[float] = None

    # For mutual_exclusivity rules
    column1: Optional[str] = None
    column2: Optional[str] = None
    value1: Optional[Any] = None
    value2: Optional[Any] = None

    description: Optional[str] = None
    severity: str = "warning"  # info, warning, error
    enabled: bool = True


@dataclass
class ValidationConfig:
    """Validation configuration"""
    strict_mode: bool = False
    max_validation_errors: int = 100
    enable_business_rules: bool = True
    enable_data_quality_analysis: bool = True
    enable_anomaly_detection: bool = True
    quality_threshold: float = 0.8
    business_rules: List[Dict[str, Any]] = field(default_factory=list)

    # Validation thresholds
    max_null_percentage: float = 50.0
    min_uniqueness_ratio: float = 0.1
    outlier_detection_method: str = "iqr"  # iqr, zscore, isolation_forest
    outlier_threshold: float = 3.0

    # Pattern validation
    enable_pattern_validation: bool = True
    custom_patterns: Dict[str, str] = field(default_factory=dict)

    def __post_init__(self):
        """Convert business rules dictionaries to BusinessRule objects"""
        if self.business_rules and isinstance(self.business_rules[0], dict):
            self.business_rules = [
                BusinessRule(**rule) if isinstance(rule, dict) else rule
                for rule in self.business_rules
            ]


@dataclass
class LoggingConfig:
    """Logging configuration"""
    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    file_path: Optional[str] = None
    max_file_size_mb: int = 100
    backup_count: int = 5
    enhanced_format: bool = False
    console_output: bool = True

    # Advanced logging settings
    log_to_file: bool = True
    log_performance: bool = False
    log_memory_usage: bool = False
    log_sql_queries: bool = False

    def __post_init__(self):
        """Validate logging configuration"""
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if self.level.upper() not in valid_levels:
            print(f"Warning: Invalid log level '{self.level}', defaulting to 'INFO'")
            self.level = "INFO"

        # Create log directory if file_path is specified
        if self.file_path:
            log_dir = os.path.dirname(self.file_path)
            if log_dir and not os.path.exists(log_dir):
                try:
                    os.makedirs(log_dir, exist_ok=True)
                except Exception as e:
                    print(f"Warning: Could not create log directory {log_dir}: {e}")


@dataclass
class GenerationConfig:
    """Main configuration class with enhanced AI support"""
    environment: str = Environment.DEVELOPMENT.value
    locale: str = "en_GB"
    tables: List[Dict[str, Any]] = field(default_factory=list)
    rows: int = 100
    output_format: str = 'csv'

    performance: PerformanceConfig = field(default_factory=PerformanceConfig)
    security: SecurityConfig = field(default_factory=SecurityConfig)
    ai: AIConfig = field(default_factory=AIConfig)  # Enhanced AI configuration
    output: OutputConfig = field(default_factory=OutputConfig)
    validation: ValidationConfig = field(default_factory=ValidationConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    database: DatabaseConfig = field(default_factory=DatabaseConfig)

    # Backward compatibility properties
    @property
    def openai(self) -> OpenAIConfig:
        """Backward compatibility property for OpenAI config"""
        return self.ai.openai if self.ai else OpenAIConfig()

    @openai.setter
    def openai(self, value: OpenAIConfig):
        """Backward compatibility setter for OpenAI config"""
        if not self.ai:
            self.ai = AIConfig()
        self.ai.openai = value

    # Additional settings
    seed: Optional[int] = None
    debug_mode: bool = False
    dry_run: bool = False

    def __post_init__(self):
        # Initialize sub-configurations with defaults if not provided
        if self.database is None:
            self.database = DatabaseConfig()
        if self.performance is None:
            self.performance = PerformanceConfig()
        if self.security is None:
            self.security = SecurityConfig()
        if self.ai is None:
            self.ai = AIConfig()
        if self.output is None:
            self.output = OutputConfig()
        if self.validation is None:
            self.validation = ValidationConfig()
        if self.logging is None:
            self.logging = LoggingConfig()
        if self.tables is None:
            self.tables = []

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'GenerationConfig':
        """Create GenerationConfig from dictionary with proper nested object creation"""
        # Extract nested configurations
        performance_data = config_dict.pop('performance', {})
        security_data = config_dict.pop('security', {})
        ai_data = config_dict.pop('ai', {})
        output_data = config_dict.pop('output', {})
        validation_data = config_dict.pop('validation', {})
        logging_data = config_dict.pop('logging', {})
        database_data = config_dict.pop('database', {})

        # Handle legacy OpenAI configuration
        openai_data = config_dict.pop('openai', {})
        if openai_data and not ai_data.get('openai'):
            if 'openai' not in ai_data:
                ai_data['openai'] = openai_data

        # Create nested config objects
        performance_config = PerformanceConfig(**performance_data)
        security_config = SecurityConfig(**security_data)

        # Enhanced AI configuration handling
        ai_config = cls._create_ai_config(ai_data)

        output_config = OutputConfig(**output_data)
        validation_config = ValidationConfig(**validation_data)
        logging_config = LoggingConfig(**logging_data)
        database_config = DatabaseConfig(**database_data)

        # Create main config
        return cls(
            performance=performance_config,
            security=security_config,
            ai=ai_config,
            output=output_config,
            validation=validation_config,
            logging=logging_config,
            database=database_config,
            **config_dict  # Remaining top-level fields
        )

    @classmethod
    def _create_ai_config(cls, ai_data: Dict[str, Any]) -> AIConfig:
        """Create AI configuration with proper handling of sub-configurations"""
        openai_data = ai_data.pop('openai', {})
        mistral_data = ai_data.pop('mistral', {})

        openai_config = OpenAIConfig(**openai_data) if openai_data else None
        mistral_config = MistralConfig(**mistral_data) if mistral_data else None

        return AIConfig(
            openai=openai_config,
            mistral=mistral_config,
            **ai_data
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation"""
        config_dict = {
            "environment": self.environment,
            "locale": self.locale,
            "rows": self.rows,
            "output_format": self.output_format,
            "tables": self.tables,
            "performance": self.performance.__dict__,
            "security": self.security.__dict__,
            "output": self.output.__dict__,
            "validation": self.validation.__dict__,
            "logging": self.logging.__dict__,
            "database": self.database.__dict__,
            "seed": self.seed,
            "debug_mode": self.debug_mode,
            "dry_run": self.dry_run
        }

        # Enhanced AI configuration serialization
        if self.ai:
            ai_dict = {
                "primary_provider": self.ai.primary_provider,
                "enable_fallback": self.ai.enable_fallback,
                "shared_cache_size": self.ai.shared_cache_size
            }
            if self.ai.openai:
                ai_dict["openai"] = asdict(self.ai.openai)
            if self.ai.mistral:
                ai_dict["mistral"] = asdict(self.ai.mistral)
            config_dict["ai"] = ai_dict

        return config_dict

    def validate(self) -> List[str]:
        """Validate the entire configuration and return any issues"""
        issues = []

        # Validate basic settings
        if self.rows <= 0:
            issues.append("Rows must be greater than 0")

        if not self.tables:
            issues.append("No tables defined in configuration")

        # Validate nested configurations
        try:
            security_issues = self.security.validate_configuration()
            issues.extend([f"Security: {issue}" for issue in security_issues])
        except AttributeError:
            pass  # validate_configuration method may not exist

        # Validate AI configuration
        ai_issues = self._validate_ai_configuration()
        issues.extend([f"AI: {issue}" for issue in ai_issues])

        # Validate table configurations
        table_names = set()
        for i, table in enumerate(self.tables):
            table_name = table.get('table_name')
            if not table_name:
                issues.append(f"Table {i} missing table_name")
            elif table_name in table_names:
                issues.append(f"Duplicate table name: {table_name}")
            else:
                table_names.add(table_name)

            if not table.get('columns'):
                issues.append(f"Table {table_name} has no columns defined")

        return issues

    def _validate_ai_configuration(self) -> List[str]:
        """Validate AI configuration"""
        issues = []

        if not self.ai:
            return issues

        # Validate primary provider setting
        valid_providers = ["openai", "mistral"]
        if self.ai.primary_provider not in valid_providers:
            issues.append(f"Invalid primary provider: {self.ai.primary_provider}")

        # Validate OpenAI configuration
        if self.ai.openai and self.ai.openai.enabled:
            openai_issues = self._validate_openai_config(self.ai.openai)
            issues.extend([f"OpenAI: {issue}" for issue in openai_issues])

        # Validate Mistral configuration
        if self.ai.mistral and self.ai.mistral.enabled:
            mistral_issues = self._validate_mistral_config(self.ai.mistral)
            issues.extend([f"Mistral: {issue}" for issue in mistral_issues])

        # Check if at least one AI provider is available when AI is needed
        active_providers = self.ai.get_active_providers()
        if not active_providers:
            issues.append("No AI providers are enabled")

        # Validate primary provider is actually enabled
        primary_config = self.ai.get_primary_provider_config()
        if not primary_config or not primary_config.enabled:
            issues.append(f"Primary provider '{self.ai.primary_provider}' is not enabled")

        return issues

    def _validate_openai_config(self, openai_config: OpenAIConfig) -> List[str]:
        """Validate OpenAI configuration"""
        issues = []

        # Check API key availability
        api_key = openai_config.get_api_key()
        if not api_key:
            issues.append("No API key found in config, file, or environment variable")
        elif len(api_key) < 20:  # Basic sanity check
            issues.append("API key seems too short, please verify")

        # Validate model name
        valid_models = [
            'gpt-3.5-turbo', 'gpt-3.5-turbo-16k',
            'gpt-4', 'gpt-4-turbo-preview', 'gpt-4-32k'
        ]
        if openai_config.model not in valid_models:
            issues.append(f"Unknown model '{openai_config.model}', may not be supported")

        # Validate numeric parameters
        if openai_config.max_tokens <= 0:
            issues.append("max_tokens must be positive")
        elif openai_config.max_tokens > 4096:
            issues.append("max_tokens > 4096 may be expensive and slow")

        if not (0.0 <= openai_config.temperature <= 2.0):
            issues.append("temperature must be between 0.0 and 2.0")

        if openai_config.cache_size <= 0:
            issues.append("cache_size must be positive")

        if openai_config.timeout_seconds <= 0:
            issues.append("timeout_seconds must be positive")

        if openai_config.retry_attempts < 0:
            issues.append("retry_attempts cannot be negative")

        # Cost limit validation
        if openai_config.cost_limit_usd is not None:
            if openai_config.cost_limit_usd <= 0:
                issues.append("cost_limit_usd must be positive if specified")

        return issues

    def _validate_mistral_config(self, mistral_config: MistralConfig) -> List[str]:
        """Validate Mistral AI configuration"""
        issues = []

        # Check API key availability
        api_key = mistral_config.get_api_key()
        if not api_key:
            issues.append("No API key found in config, file, or environment variable")
        elif len(api_key) < 20:  # Basic sanity check
            issues.append("API key seems too short, please verify")

        # Validate model name
        valid_models = [
            # Current Mistral models (as of 2024)
            'mistral-tiny',
            'mistral-small',
            'mistral-medium',
            'mistral-large',
            'mistral-7b-instruct',
            'mixtral-8x7b-instruct',
            'mistral-small-latest',
            'mistral-medium-latest',
            'mistral-large-latest',
            # Add any new models here
            'open-mistral-7b',
            'open-mixtral-8x7b',
            'open-mixtral-8x22b',
            'mistral-small-2402',
            'mistral-large-2402'
        ]
        if mistral_config.model not in valid_models:
            issues.append(f"Unknown model '{mistral_config.model}', may not be supported")

        # Validate numeric parameters
        if mistral_config.max_tokens <= 0:
            issues.append("max_tokens must be positive")
        elif mistral_config.max_tokens > 8192:
            issues.append("max_tokens > 8192 may be expensive and slow")

        if not (0.0 <= mistral_config.temperature <= 1.0):
            issues.append("temperature must be between 0.0 and 1.0")

        if mistral_config.cache_size <= 0:
            issues.append("cache_size must be positive")

        if mistral_config.timeout_seconds <= 0:
            issues.append("timeout_seconds must be positive")

        if mistral_config.retry_attempts < 0:
            issues.append("retry_attempts cannot be negative")

        # Cost limit validation
        if mistral_config.cost_limit_usd is not None:
            if mistral_config.cost_limit_usd <= 0:
                issues.append("cost_limit_usd must be positive if specified")

        return issues


class ConfigurationManager:
    """
    Enhanced configuration management with multi-AI provider support,
    environment support, validation, templates, and backward compatibility
    """

    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.config_cache = {}
        self.environment_configs = {}
        self.template_configs = {}
        self.schema_validator = SchemaValidator()

        # Load default configurations
        self._load_default_configs()

    def _load_default_configs(self):
        """Load default configurations for different environments"""
        # Development environment
        dev_config = GenerationConfig(
            environment=Environment.DEVELOPMENT.value,
            rows=1000,
            performance=PerformanceConfig(
                max_workers=2,
                batch_size=5000,
                max_memory_mb=500
            ),
            security=SecurityConfig(
                enable_data_masking=False,
                enable_encryption=False,
                audit_enabled=False
            ),
            ai=AIConfig(
                openai=OpenAIConfig(
                    enabled=False,
                    model="gpt-3.5-turbo",
                    cache_size=50,
                    cost_limit_usd=5.0
                ),
                mistral=MistralConfig(
                    enabled=False,
                    model="mistral-small",
                    cache_size=50,
                    cost_limit_usd=3.0
                ),
                primary_provider="openai"
            ),
            validation=ValidationConfig(
                strict_mode=False,
                max_validation_errors=50
            )
        )

        # Testing environment
        test_config = GenerationConfig(
            environment=Environment.TESTING.value,
            rows=10000,
            performance=PerformanceConfig(
                max_workers=4,
                batch_size=10000,
                max_memory_mb=1000
            ),
            security=SecurityConfig(
                enable_data_masking=True,
                enable_encryption=False,
                audit_enabled=True
            ),
            ai=AIConfig(
                openai=OpenAIConfig(
                    enabled=False,
                    model="gpt-3.5-turbo",
                    cache_size=100,
                    cost_limit_usd=10.0
                ),
                mistral=MistralConfig(
                    enabled=False,
                    model="mistral-small",
                    cache_size=100,
                    cost_limit_usd=8.0
                ),
                primary_provider="mistral"
            ),
            validation=ValidationConfig(
                strict_mode=True,
                max_validation_errors=10
            )
        )

        # Production environment
        prod_config = GenerationConfig(
            environment=Environment.PRODUCTION.value,
            rows=1000000,
            performance=PerformanceConfig(
                max_workers=8,
                batch_size=50000,
                max_memory_mb=4000,
                enable_streaming=True
            ),
            security=SecurityConfig(
                enable_data_masking=True,
                enable_encryption=True,
                audit_enabled=True
            ),
            ai=AIConfig(
                openai=OpenAIConfig(
                    enabled=False,
                    model="gpt-3.5-turbo",
                    cache_size=200,
                    cost_limit_usd=50.0,
                    timeout_seconds=60,
                    retry_attempts=5
                ),
                mistral=MistralConfig(
                    enabled=False,
                    model="mistral-medium",
                    cache_size=200,
                    cost_limit_usd=40.0,
                    timeout_seconds=60,
                    retry_attempts=5
                ),
                primary_provider="mistral",
                enable_fallback=True
            ),
            validation=ValidationConfig(
                strict_mode=True,
                max_validation_errors=0,
                quality_threshold=0.95
            )
        )

        self.environment_configs = {
            Environment.DEVELOPMENT.value: dev_config,
            Environment.TESTING.value: test_config,
            Environment.PRODUCTION.value: prod_config
        }

    # ===================== CONFIGURATION LOADING =====================

    def load_configuration(self, config_path: str,
                           environment: str = None,
                           # Command-line argument overrides
                           rows: int = None,
                           max_workers: int = None,
                           max_memory: int = None,
                           batch_size: int = None,
                           output_format: str = None,
                           output_dir: str = None,
                           log_level: str = None,
                           enable_streaming: bool = None,
                           enable_parallel: bool = None,
                           enable_masking: bool = None,
                           enable_quality_analysis: bool = None,
                           enable_encryption: bool = None,
                           enable_all_features: bool = None,
                           # AI specific overrides
                           ai_enabled: bool = None,
                           openai_enabled: bool = None,
                           mistral_enabled: bool = None,
                           ai_primary_provider: str = None,
                           openai_model: str = None,
                           mistral_model: str = None,
                           # Additional overrides
                           **kwargs) -> 'GenerationConfig':
        """
        Enhanced configuration loading with comprehensive argument override support

        Args:
            config_path: Path to configuration file
            environment: Environment name for environment-specific overrides
            rows: Override number of rows per table
            max_workers: Override maximum worker threads/processes
            max_memory: Override maximum memory limit in MB
            batch_size: Override batch size for processing
            output_format: Override output format (csv, json, parquet, etc.)
            output_dir: Override output directory
            log_level: Override logging level
            enable_streaming: Override streaming enable/disable
            enable_parallel: Override parallel processing enable/disable
            enable_masking: Override data masking enable/disable
            enable_quality_analysis: Override quality analysis enable/disable
            enable_encryption: Override encryption enable/disable
            enable_all_features: Enable all optional features
            ai_enabled: Override AI integration enable/disable
            openai_enabled: Override OpenAI enable/disable
            mistral_enabled: Override Mistral AI enable/disable
            ai_primary_provider: Override primary AI provider
            openai_model: Override OpenAI model
            mistral_model: Override Mistral model
            **kwargs: Additional overrides
        """
        config_path = Path(config_path)

        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        # Load raw configuration
        raw_config, config_reader = self._load_json(config_path)

        # Apply argument overrides BEFORE parsing
        raw_config = self._apply_argument_overrides(
            raw_config,
            rows=rows,
            max_workers=max_workers,
            max_memory=max_memory,
            batch_size=batch_size,
            output_format=output_format,
            output_dir=output_dir,
            log_level=log_level,
            enable_streaming=enable_streaming,
            enable_parallel=enable_parallel,
            enable_masking=enable_masking,
            enable_quality_analysis=enable_quality_analysis,
            enable_encryption=enable_encryption,
            enable_all_features=enable_all_features,
            ai_enabled=ai_enabled,
            openai_enabled=openai_enabled,
            mistral_enabled=mistral_enabled,
            ai_primary_provider=ai_primary_provider,
            openai_model=openai_model,
            mistral_model=mistral_model,
            **kwargs
        )

        raw_config = config_reader.validate_schema(raw_config)

        # Parse configuration with overrides applied
        config = self._parse_config_dict(raw_config)

        # If enable_all_features was used, validate the result
        if enable_all_features:
            validation_results = self._validate_enable_all_features_result(config)
            for result in validation_results:
                self.logger.info(result)

        # Apply environment-specific overrides
        if environment:
            if not self.validate_environment_config(environment):
                available_envs = self.list_available_environments()
                self.logger.warning(f"⚠️  Unknown environment '{environment}'. Available: {', '.join(available_envs)}")

            config = self._apply_environment_overrides(config, environment)

        # Apply environment variables (lowest priority)
        config = self._apply_environment_variables(config)

        if config.ai and config.ai.get_active_providers():
            self._validate_ai_configuration_comprehensive(config)

        # Comprehensive validation including capacity checks
        self._validate_configuration_comprehensive(config, raw_config)

        if config.debug_mode:
            summary = self.get_config_summary(config)
            self.logger.info("📋 Configuration Summary:")
            self.logger.info(f"   Environment: {summary['environment']}")
            self.logger.info(f"   Tables: {summary['total_tables']}, Rows: {summary['total_rows']:,}")
            self.logger.info(f"   AI Enabled: {summary.get('ai', {}).get('enabled', False)}")
            if summary.get('ai', {}).get('enabled'):
                self.logger.info(f"   Primary Provider: {summary['ai']['primary_provider']}")

        # Cache configuration
        cache_key = f"{config_path}:{environment or 'default'}:{hash(str(sorted(kwargs.items())))}"
        self.config_cache[cache_key] = config

        return config

    def update_output_directory(self, config: GenerationConfig, new_directory: str,
                                cleanup_old: bool = False) -> bool:
        """Safely update output directory in configuration"""
        try:
            old_directory = config.output.directory
            old_report_directory = config.output.report_directory

            config.output.change_directory(new_directory, cleanup_old=cleanup_old)

            self.logger.info(f"📁 Successfully changed output directory:")
            self.logger.info(f"   From: {old_directory}")
            self.logger.info(f"   To:   {config.output.directory}")
            self.logger.info(f"   Report dir: {config.output.report_directory}")

            if cleanup_old:
                self.logger.info(f"🗑️  Cleaned up old directory: {old_directory}")

            return True

        except Exception as e:
            self.logger.error(f"❌ Failed to change output directory: {e}")
            return False

    def _validate_ai_configuration_comprehensive(self, config: GenerationConfig):
        """Comprehensive AI configuration validation with user feedback"""
        self.logger.info("🤖 Validating AI configuration...")

        # Validate API keys
        key_validation = self.validate_ai_keys(config)
        ai_issues = []

        for provider, status in key_validation.items():
            if not status['available']:
                ai_issues.append(f"{provider.upper()}: No API key found")
                self.logger.warning(f"🔑 {provider.upper()} API key not available")
            elif not status['valid_length']:
                ai_issues.append(f"{provider.upper()}: API key seems invalid (too short)")
                self.logger.warning(f"🔑 {provider.upper()} API key seems invalid (too short)")
            else:
                self.logger.info(f"🔑 {provider.upper()} API key found and appears valid")

        # Test connections only in debug mode (to avoid API costs during normal config loading)
        if config.debug_mode and not ai_issues:
            self.logger.info("🔗 Testing AI connections (debug mode)...")
            for provider in config.ai.get_active_providers():
                connection_result = self.test_ai_connection(config, provider)
                if connection_result['status'] == 'success':
                    self.logger.info(f"✅ {provider.upper()} connection test successful")
                elif connection_result['status'] == 'error':
                    self.logger.warning(f"⚠️  {provider.upper()} connection failed: {connection_result['message']}")
                else:
                    self.logger.info(f"ℹ️  {provider.upper()}: {connection_result['message']}")

    def _apply_argument_overrides(self, raw_config: Dict[str, Any], **overrides) -> Dict[str, Any]:
        """
        Apply command-line argument overrides to raw configuration
        This happens BEFORE parsing so validation can catch capacity issues
        """
        config = raw_config.copy()

        # Track what was overridden for logging
        applied_overrides = []

        # Top-level overrides
        if overrides.get('rows') is not None:
            old_value = config.get('rows', 'not set')
            config['rows'] = overrides['rows']
            applied_overrides.append(f"rows: {old_value} → {overrides['rows']}")

        if overrides.get('output_format') is not None:
            if 'output' not in config:
                config['output'] = {}
            old_value = config['output'].get('format', 'not set')
            config['output']['format'] = overrides['output_format']
            applied_overrides.append(f"output.format: {old_value} → {overrides['output_format']}")

        if overrides.get('log_level') is not None:
            if 'logging' not in config:
                config['logging'] = {}
            old_value = config['logging'].get('level', 'not set')
            config['logging']['level'] = overrides['log_level']
            applied_overrides.append(f"logging.level: {old_value} → {overrides['log_level']}")

        # Performance overrides
        performance_overrides = {
            'max_workers': overrides.get('max_workers'),
            'max_memory_mb': overrides.get('max_memory'),
            'batch_size': overrides.get('batch_size'),
            'enable_streaming': overrides.get('enable_streaming'),
            'enable_parallel': overrides.get('enable_parallel')
        }

        performance_updates = {}
        for key, value in performance_overrides.items():
            if value is not None:
                if 'performance' not in config:
                    config['performance'] = {}
                old_value = config['performance'].get(key, 'not set')
                config['performance'][key] = value
                performance_updates[key] = f"{old_value} → {value}"

        if performance_updates:
            applied_overrides.extend([f"performance.{k}: {v}" for k, v in performance_updates.items()])

        # Security overrides
        security_overrides = {
            'enable_data_masking': overrides.get('enable_masking'),
            'enable_encryption': overrides.get('enable_encryption')
        }

        security_updates = {}
        for key, value in security_overrides.items():
            if value is not None:
                if 'security' not in config:
                    config['security'] = {}
                old_value = config['security'].get(key, 'not set')
                config['security'][key] = value
                security_updates[key] = f"{old_value} → {value}"

        if security_updates:
            applied_overrides.extend([f"security.{k}: {v}" for k, v in security_updates.items()])

        # Validation overrides
        if overrides.get('enable_quality_analysis') is not None:
            if 'validation' not in config:
                config['validation'] = {}
            old_value = config['validation'].get('enable_data_quality_analysis', 'not set')
            config['validation']['enable_data_quality_analysis'] = overrides['enable_quality_analysis']
            applied_overrides.append(
                f"validation.enable_data_quality_analysis: {old_value} → {overrides['enable_quality_analysis']}")

        # Output directory override
        if overrides.get('output_dir') is not None:
            if 'output' not in config:
                config['output'] = {}
            old_value = config['output'].get('directory', 'not set')
            temp_output = OutputConfig(**config['output'])
            temp_output.change_directory(overrides['output_dir'])

            # Update config with new values
            config['output']['directory'] = temp_output.directory
            config['output']['report_directory'] = temp_output.report_directory

            applied_overrides.append(f"output.directory: {old_value} → {temp_output.directory}")
            applied_overrides.append(f"output.report_directory: auto-updated → {temp_output.report_directory}")

        # AI overrides
        ai_updates = self._apply_ai_overrides(config, overrides)
        applied_overrides.extend(ai_updates)

        # Enable all features override
        if overrides.get('enable_all_features'):
            self.logger.info("🚀 enable_all_features requested - checking AI availability...")

            # Pre-check AI availability
            available_ai = self._check_available_ai_providers()
            if available_ai:
                sources_info = self._get_key_sources_summary(available_ai)
                self.logger.info(f"🤖 Found AI providers: {', '.join(available_ai.keys())}")
            else:
                self.logger.warning("🔑 No AI keys found - will enable all other features")

            all_features_updates = self._enable_all_features(config)
            applied_overrides.extend(all_features_updates)

            # Provide feedback about AI status
            if available_ai:
                self.logger.info(f"✅ All features enabled including AI ({len(available_ai)} provider(s))")
                for provider, info in available_ai.items():
                    key_source = info['key_source']
                    model = info.get('recommended_model', 'default')
                    if info.get('validation_passed', False):
                        self.logger.info(f"   🔑 {provider.upper()}: {key_source} → {model}")
                    else:
                        error = info.get('error', 'unknown error')
                        self.logger.warning(f"   ⚠️  {provider.upper()}: {key_source} (error: {error})")
            else:
                self.logger.info("✅ All non-AI features enabled (no AI keys available)")
                self.logger.info("💡 To enable AI features, add keys via:")
                self.logger.info("   - Config file: 'ai.openai.api_key' or 'ai.mistral.api_key'")
                self.logger.info("   - Environment: OPENAI_API_KEY or MISTRAL_API_KEY")
                self.logger.info("   - Key files: Set 'api_key_file' paths in config")

        # Log applied overrides
        if applied_overrides:
            self.logger.info("🔧 Applied command-line overrides:")
            for override in applied_overrides:
                self.logger.info(f"  - {override}")

        return config

    def _get_key_sources_summary(self, available_providers: Dict[str, Dict[str, Any]]) -> str:
        """Get a readable summary of where keys were found"""
        sources = []
        for provider, info in available_providers.items():
            source = info['key_source']

            # Simplify source names for readability
            if source.startswith('config.'):
                simplified_source = 'config'
            elif source.startswith('file'):
                simplified_source = 'file'
            elif source.startswith('environment'):
                simplified_source = 'env'
            else:
                simplified_source = source

            sources.append(f"{provider}({simplified_source})")

        return ", ".join(sources)

    def _validate_enable_all_features_result(self, config: 'GenerationConfig') -> List[str]:
        """Validate that enable_all_features worked correctly"""
        validation_results = []

        # Check if core features are enabled
        if not config.performance.enable_streaming:
            validation_results.append("⚠️  Streaming not enabled despite enable_all_features")

        if not config.security.enable_data_masking:
            validation_results.append("⚠️  Data masking not enabled despite enable_all_features")

        # Check AI status
        if config.ai:
            active_providers = config.ai.get_active_providers()
            if active_providers:
                validation_results.append(f"✅ AI successfully enabled: {', '.join(active_providers)}")
            else:
                # Check if this is expected (no keys available)
                available_providers = self._check_available_ai_providers()
                if available_providers:
                    validation_results.append("⚠️  AI providers available but not enabled")
                else:
                    validation_results.append("ℹ️  AI not enabled (no API keys found) - this is expected")

        return validation_results

    def _check_available_ai_providers(self, config: Dict[str, Any] = None) -> Dict[str, Dict[str, Any]]:
        """Check which AI providers have valid keys available from ALL sources"""
        available = {}

        # Check OpenAI from all sources
        openai_info = self._check_provider_availability('openai', config)
        if openai_info:
            available['openai'] = openai_info

        # Check Mistral from all sources
        mistral_info = self._check_provider_availability('mistral', config)
        if mistral_info:
            available['mistral'] = mistral_info

        return available

    def _apply_ai_overrides(self, config: Dict[str, Any], overrides: Dict[str, Any]) -> List[str]:
        """Apply AI-specific overrides"""
        applied_overrides = []

        # Initialize AI config if needed
        if 'ai' not in config:
            config['ai'] = {}

        # AI primary provider override
        if overrides.get('ai_primary_provider') is not None:
            old_value = config['ai'].get('primary_provider', 'not set')
            new_provider = overrides['ai_primary_provider']

            valid_providers = ["openai", "mistral"]
            if new_provider in valid_providers:
                config['ai']['primary_provider'] = new_provider
                applied_overrides.append(f"ai.primary_provider: {old_value} → {new_provider}")
            else:
                applied_overrides.append(f"ai.primary_provider: INVALID '{new_provider}' (valid: {valid_providers})")
                self.logger.warning(f"Invalid AI provider '{new_provider}'. Valid options: {valid_providers}")

        # Enhanced provider enabling with validation
        if overrides.get('openai_enabled') is not None:
            if 'openai' not in config['ai']:
                config['ai']['openai'] = {}

            old_value = config['ai']['openai'].get('enabled', 'not set')
            config['ai']['openai']['enabled'] = overrides['openai_enabled']
            applied_overrides.append(f"ai.openai.enabled: {old_value} → {overrides['openai_enabled']}")

            # If enabling, check for API key
            if overrides['openai_enabled'] and not os.getenv('OPENAI_API_KEY'):
                applied_overrides.append(
                    "ai.openai.enabled: WARNING - No OPENAI_API_KEY environment variable found")

        if overrides.get('mistral_enabled') is not None:
            if 'mistral' not in config['ai']:
                config['ai']['mistral'] = {}

            old_value = config['ai']['mistral'].get('enabled', 'not set')
            config['ai']['mistral']['enabled'] = overrides['mistral_enabled']
            applied_overrides.append(f"ai.mistral.enabled: {old_value} → {overrides['mistral_enabled']}")

            # If enabling, check for API key
            if overrides['mistral_enabled'] and not os.getenv('MISTRAL_API_KEY'):
                applied_overrides.append(
                    "ai.mistral.enabled: WARNING - No MISTRAL_API_KEY environment variable found")

        return applied_overrides

    def _enable_all_features(self, config: Dict[str, Any]) -> List[str]:
        """Enable all optional features"""
        applied_overrides = []

        # Performance features
        if 'performance' not in config:
            config['performance'] = {}

        performance_features = {
            'enable_streaming': True,
            'enable_parallel': True
        }

        for key, value in performance_features.items():
            old_value = config['performance'].get(key, 'not set')
            config['performance'][key] = value
            applied_overrides.append(f"performance.{key}: {old_value} → {value}")

        # Security features
        if 'security' not in config:
            config['security'] = {}

        security_features = {
            'enable_data_masking': True,
            'enable_encryption': True,
            'audit_enabled': True
        }

        for key, value in security_features.items():
            old_value = config['security'].get(key, 'not set')
            config['security'][key] = value
            applied_overrides.append(f"security.{key}: {old_value} → {value}")

        # Validation features
        if 'validation' not in config:
            config['validation'] = {}

        validation_features = {
            'enable_data_quality_analysis': True,
            'enable_business_rules': True,
            'enable_anomaly_detection': True
        }

        for key, value in validation_features.items():
            old_value = config['validation'].get(key, 'not set')
            config['validation'][key] = value
            applied_overrides.append(f"validation.{key}: {old_value} → {value}")

        ai_overrides = self._enable_ai_features_smartly(config)
        applied_overrides.extend(ai_overrides)

        applied_overrides.append("🚀 All features enabled")
        return applied_overrides

    def _enable_ai_features_smartly(self, config: Dict[str, Any]) -> List[str]:
        """Smart AI features enabling - only enable if keys are available"""
        ai_overrides = []

        if 'ai' not in config:
            config['ai'] = {}

        # Check what AI providers can actually be enabled
        available_providers = self._check_available_ai_providers()

        if not available_providers:
            # No AI keys available
            ai_overrides.append("ai: No API keys found - AI features skipped")
            self.logger.warning("🔑 enable_all_features: No AI API keys found, skipping AI features")
            self.logger.info("💡 To enable AI: Set OPENAI_API_KEY or MISTRAL_API_KEY environment variables")
            return ai_overrides

        # At least one provider is available - configure AI optimally
        ai_overrides.extend(self._configure_optimal_ai_setup(config, available_providers))

        return ai_overrides

    def _check_provider_availability(self, provider: str, config: Dict[str, Any] = None) -> Optional[Dict[str, Any]]:
        """Check if a specific provider has valid keys from any source"""
        if provider == 'openai':
            env_var = 'OPENAI_API_KEY'
            config_path = ['ai', 'openai']
            key_validator = lambda key: key.startswith('sk-') and len(key.strip()) >= 20
            recommended_model = 'gpt-3.5-turbo'
        elif provider == 'mistral':
            env_var = 'MISTRAL_API_KEY'
            config_path = ['ai', 'mistral']
            key_validator = lambda key: len(key.strip()) >= 20 and not key.startswith('sk-')
            recommended_model = 'mistral-medium'
        else:
            return None

        # Source 1: Direct config key (highest priority)
        api_key, key_source = self._get_api_key_from_config(config, config_path)
        if api_key and key_validator(api_key):
            return {
                'key_source': key_source,
                'key_valid': True,
                'recommended_model': recommended_model,
                'api_key_length': len(api_key),
                'validation_passed': True
            }

        # Source 2: API key file
        api_key_file = self._get_config_value(config, config_path + ['api_key_file'])
        if api_key_file and os.path.exists(api_key_file):
            try:
                with open(api_key_file, 'r') as f:
                    file_key = f.read().strip()
                    if file_key and key_validator(file_key):
                        return {
                            'key_source': f'file ({api_key_file})',
                            'key_valid': True,
                            'recommended_model': recommended_model,
                            'api_key_length': len(file_key),
                            'validation_passed': True
                        }
            except (IOError, PermissionError) as e:
                # File exists but can't read it
                return {
                    'key_source': f'file ({api_key_file}) - ERROR',
                    'key_valid': False,
                    'error': str(e),
                    'validation_passed': False
                }

        # Source 3: Environment variable (lowest priority)
        env_key = os.getenv(env_var)
        if env_key and key_validator(env_key):
            return {
                'key_source': f'environment ({env_var})',
                'key_valid': True,
                'recommended_model': recommended_model,
                'api_key_length': len(env_key),
                'validation_passed': True
            }

        # No valid key found in any source
        return None

    def _get_api_key_from_config(self, config: Dict[str, Any], config_path: List[str]) -> Tuple[Optional[str], str]:
        """Get API key from config with source tracking"""
        if not config:
            return None, 'no_config'

        # Navigate to the provider config
        current = config
        for path_part in config_path:
            if not isinstance(current, dict) or path_part not in current:
                return None, 'path_not_found'
            current = current[path_part]

        # Check for direct api_key
        if isinstance(current, dict) and current.get('api_key'):
            api_key = current['api_key']
            if isinstance(api_key, str) and api_key.strip():
                return api_key.strip(), f'config.{".".join(config_path)}.api_key'

        return None, 'not_in_config'

    def _get_config_value(self, config: Dict[str, Any], path: List[str]) -> Any:
        """Safely get nested config value"""
        if not config:
            return None

        current = config
        for path_part in path:
            if not isinstance(current, dict) or path_part not in current:
                return None
            current = current[path_part]

        return current

    def _select_optimal_primary_provider(self, available_providers: Dict[str, Dict]) -> str:
        """Select the best primary provider for 'enable_all_features'"""
        # Preference order for "all features" mode
        preference_order = ['mistral', 'openai']  # Mistral often better price/performance

        for preferred in preference_order:
            if preferred in available_providers:
                return preferred

        # Fallback to first available
        return list(available_providers.keys())[0] if available_providers else 'openai'

    def _configure_optimal_ai_setup(self, config: Dict[str, Any], available_providers: Dict[str, Dict]) -> List[str]:
        """Configure AI with optimal settings for 'enable_all_features'"""
        ai_overrides = []

        # Determine primary provider preference
        primary_provider = self._select_optimal_primary_provider(available_providers)

        # Configure OpenAI if available
        if 'openai' in available_providers:
            if 'openai' not in config['ai']:
                config['ai']['openai'] = {}

            openai_config = {
                'enabled': True,
                'model': 'gpt-3.5-turbo',  # Reliable for all features
                'max_tokens': 4000,  # Higher for complex tasks
                'cache_size': 200,  # Larger cache
                'cost_limit_usd': 50.0,  # Higher limit for all features
                'temperature': 0.3,  # Lower for more consistent results
                'timeout_seconds': 60,  # Longer timeout
                'retry_attempts': 5  # More retries
            }

            for key, value in openai_config.items():
                old_value = config['ai']['openai'].get(key, 'not set')
                config['ai']['openai'][key] = value
                ai_overrides.append(f"ai.openai.{key}: {old_value} → {value}")

        # Configure Mistral if available
        if 'mistral' in available_providers:
            if 'mistral' not in config['ai']:
                config['ai']['mistral'] = {}

            mistral_config = {
                'enabled': True,
                'model': 'mistral-medium',  # Better model for all features
                'max_tokens': 4000,
                'cache_size': 200,
                'cost_limit_usd': 40.0,
                'temperature': 0.3,
                'timeout_seconds': 60,
                'retry_attempts': 5
            }

            for key, value in mistral_config.items():
                old_value = config['ai']['mistral'].get(key, 'not set')
                config['ai']['mistral'][key] = value
                ai_overrides.append(f"ai.mistral.{key}: {old_value} → {value}")

        # Configure AI-level settings
        ai_level_config = {
            'primary_provider': primary_provider,
            'enable_fallback': len(available_providers) > 1,  # Enable fallback if multiple providers
            'shared_cache_size': 400  # Larger shared cache
        }

        for key, value in ai_level_config.items():
            old_value = config['ai'].get(key, 'not set')
            config['ai'][key] = value
            ai_overrides.append(f"ai.{key}: {old_value} → {value}")

        # Log success
        provider_names = list(available_providers.keys())
        ai_overrides.append(f"🤖 AI enabled with providers: {', '.join(provider_names)}")

        return ai_overrides

    def _validate_configuration_comprehensive(self, config: 'GenerationConfig', raw_config: Dict[str, Any]):
        """
        Comprehensive validation including capacity checks
        """
        self.logger.info("🔍 Starting comprehensive configuration validation...")

        # Stage 1: Basic Configuration Validation
        basic_errors = self._validate_basic_configuration(config)
        if basic_errors:
            self._log_validation_errors("Basic Configuration", basic_errors)
            raise ValueError(
                f"Basic configuration validation failed:\n" + "\n".join(f"  - {error}" for error in basic_errors))

        # Stage 2: Schema Structure and Capacity Validation
        schema_errors, capacity_errors = self._validate_schema_and_capacity(config, raw_config)

        if capacity_errors:
            self._log_capacity_errors(capacity_errors, config)
            raise ValueError(
                f"CRITICAL: Schema capacity validation failed. Cannot generate data with current configuration.\n" +
                "\n".join(f"  - {error}" for error in capacity_errors))

        if schema_errors:
            self.logger.warning("Schema validation issues found (non-critical):")
            for error in schema_errors[:5]:  # Limit output
                self.logger.warning(f"  - {error}")

        self.logger.info("✅ Comprehensive configuration validation completed")

    def _validate_basic_configuration(self, config: 'GenerationConfig') -> List[str]:
        """Basic configuration validation"""
        errors = []

        if config.rows <= 0:
            errors.append("Rows must be greater than 0")

        if not config.tables:
            errors.append("No tables defined in configuration")

        if config.performance.max_workers <= 0:
            errors.append("Max workers must be positive")

        if config.performance.batch_size <= 0:
            errors.append("Batch size must be positive")

        if config.performance.max_memory_mb <= 0:
            errors.append("Max memory must be positive")

        return errors

    def _validate_schema_and_capacity(self, config: 'GenerationConfig', raw_config: Dict[str, Any]) -> tuple:
        """Enhanced schema validation with capacity checking"""
        schema_errors = []
        capacity_errors = []

        try:
            # Create schema structure for validator
            schema_for_validation = {
                'tables': config.tables,
                'rows': config.rows
            }

            # Run comprehensive schema validation
            errors, warnings, suggestions, critical_errors, corrected_schema = self.schema_validator.validate_schema(
                schema_for_validation)

            # Categorize results
            schema_errors.extend(errors)
            capacity_errors.extend(critical_errors)

            # Log suggestions
            if suggestions:
                self.logger.info("💡 Schema suggestions:")
                for suggestion in suggestions[:3]:
                    self.logger.info(f"  - {suggestion}")

        except Exception as e:
            schema_errors.append(f"Schema validation error: {str(e)}")

        return schema_errors, capacity_errors

    def _log_capacity_errors(self, errors: List[str], config: 'GenerationConfig'):
        """Log capacity errors with specific recommendations"""
        self.logger.error("🚨 CRITICAL CAPACITY VALIDATION FAILED:")
        self.logger.error(f"Cannot generate {config.rows:,} rows with current constraints:")

        for error in errors:
            self.logger.error(f"  - {error}")

        self.logger.error("\n💡 QUICK FIXES:")
        self.logger.error("  1. Reduce rows with: --rows 1000")
        self.logger.error("  2. Or edit your JSON config to increase range limits")
        self.logger.error("  3. Or use sequence rules for unlimited capacity")

        # Show specific capacity analysis
        self._show_capacity_analysis(config)

    def _show_capacity_analysis(self, config: 'GenerationConfig'):
        """Show detailed capacity analysis for troubleshooting"""
        self.logger.error("\n📊 CAPACITY ANALYSIS:")

        for table in config.tables:
            table_name = table.get('table_name', 'unknown')

            for column in table.get('columns', []):
                column_name = column.get('name', 'unknown')
                constraints = column.get('constraints', []) + column.get('constraint', [])

                if 'PK' in constraints or 'unique' in constraints:
                    capacity = self._calculate_column_capacity(column)
                    if capacity and capacity < config.rows:
                        self.logger.error(
                            f"  ❌ {table_name}.{column_name}: {capacity:,} capacity < {config.rows:,} needed")
                        self._suggest_capacity_fix(column, table_name, column_name, config.rows)

    def _calculate_column_capacity(self, column: Dict[str, Any]) -> Optional[int]:
        """Calculate maximum unique values a column can generate"""
        rule = column.get('rule', {})

        if isinstance(rule, dict):
            rule_type = rule.get('type', '').lower()

            if rule_type == 'range':
                min_val = rule.get('min')
                max_val = rule.get('max')
                if min_val is not None and max_val is not None:
                    return max_val - min_val + 1

            elif rule_type == 'choice':
                choices = rule.get('value', [])
                if isinstance(choices, list):
                    return len(choices)

        return None

    def _suggest_capacity_fix(self, column: Dict[str, Any], table_name: str, column_name: str, needed_rows: int):
        """Suggest specific capacity fixes"""
        rule = column.get('rule', {})

        if isinstance(rule, dict):
            rule_type = rule.get('type', '').lower()

            if rule_type == 'range':
                current_min = rule.get('min', 0)
                suggested_max = current_min + needed_rows - 1
                self.logger.error(f"      💡 Fix: Change max from {rule.get('max')} to {suggested_max}")

            elif rule_type == 'choice':
                self.logger.error(f"      💡 Fix: Add more choices or use sequence rule")

    def _log_validation_errors(self, category: str, errors: List[str]):
        """Log validation errors with proper formatting"""
        self.logger.error(f"❌ {category} validation failed:")
        for error in errors:
            self.logger.error(f"  - {error}")

    def _load_json(self, config_path: Path, rows: int = None) -> tuple[Any, JSONConfigReader]:
        """Load JSON configuration file"""
        try:
            config_reader = JSONConfigReader(config_path)
            config = config_reader.load_config()
            return config, config_reader
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON configuration: {e}")

    def _parse_config_dict(self, config_dict: Dict[str, Any]) -> GenerationConfig:
        """Parse configuration dictionary into GenerationConfig object"""
        # Handle nested configurations
        nested_configs = {}

        # Make a copy to avoid modifying the original
        config_dict = config_dict.copy()

        if 'database' in config_dict:
            nested_configs['database'] = DatabaseConfig(**config_dict.pop('database'))

        if 'performance' in config_dict:
            nested_configs['performance'] = PerformanceConfig(**config_dict.pop('performance'))

        if 'security' in config_dict:
            security_data = config_dict.pop('security', {})
            if isinstance(security_data, dict):
                # Filter out any unknown parameters
                valid_params = {
                    'enable_data_masking', 'masking_rules', 'sensitivity_map',
                    'encrypt_fields', 'audit_enabled', 'enable_encryption'
                }
                filtered_security = {k: v for k, v in security_data.items() if k in valid_params}
                nested_configs['security'] = SecurityConfig(**filtered_security)
            else:
                nested_configs['security'] = SecurityConfig()

        # Enhanced AI configuration parsing
        if 'ai' in config_dict:
            nested_configs['ai'] = self._parse_ai_config(config_dict.pop('ai'))
        elif 'openai' in config_dict:
            # Backward compatibility: convert old openai config to new ai config
            openai_data = config_dict.pop('openai')
            nested_configs['ai'] = AIConfig(
                openai=OpenAIConfig(**openai_data),
                mistral=MistralConfig(),
                primary_provider="openai"
            )

        if 'output' in config_dict:
            nested_configs['output'] = OutputConfig(**config_dict.pop('output'))

        if 'validation' in config_dict:
            nested_configs['validation'] = ValidationConfig(**config_dict.pop('validation'))

        if 'logging' in config_dict:
            nested_configs['logging'] = LoggingConfig(**config_dict.pop('logging'))

        # Create main configuration
        try:
            config = GenerationConfig(**config_dict, **nested_configs)
            return config
        except TypeError as e:
            raise ValueError(f"Invalid configuration structure: {e}")

    def _parse_ai_config(self, ai_data: Dict[str, Any]) -> AIConfig:
        """Parse AI configuration from dictionary"""
        ai_config_data = ai_data.copy()

        # Extract provider configurations
        openai_data = ai_config_data.pop('openai', {})
        mistral_data = ai_config_data.pop('mistral', {})

        # Create provider configurations
        openai_config = OpenAIConfig(**openai_data) if openai_data else OpenAIConfig()
        mistral_config = MistralConfig(**mistral_data) if mistral_data else MistralConfig()

        # Create AI configuration
        return AIConfig(
            openai=openai_config,
            mistral=mistral_config,
            **ai_config_data
        )

    def _apply_environment_overrides(self, config: GenerationConfig,
                                     environment: str) -> GenerationConfig:
        """Apply environment-specific configuration overrides"""
        if environment not in self.environment_configs:
            available_envs = self.list_available_environments()
            self.logger.warning(f"Unknown environment: {environment}")
            self.logger.warning(f"Available environments: {', '.join(available_envs)}")
            self.logger.warning("Continuing with base configuration...")
            return config

        env_config = self.environment_configs[environment]

        # Apply overrides selectively
        config.environment = environment

        # Performance overrides
        if env_config.performance:
            config.performance.max_workers = env_config.performance.max_workers
            config.performance.max_memory_mb = env_config.performance.max_memory_mb
            config.performance.enable_streaming = env_config.performance.enable_streaming

        # Security overrides
        if env_config.security:
            config.security.enable_data_masking = env_config.security.enable_data_masking
            config.security.enable_encryption = env_config.security.enable_encryption
            config.security.audit_enabled = env_config.security.audit_enabled

        # AI overrides
        if env_config.ai:
            # OpenAI overrides
            if env_config.ai.openai and config.ai.openai:
                config.ai.openai.enabled = env_config.ai.openai.enabled
                config.ai.openai.model = env_config.ai.openai.model
                config.ai.openai.cache_size = env_config.ai.openai.cache_size
                config.ai.openai.cost_limit_usd = env_config.ai.openai.cost_limit_usd

            # Mistral overrides
            if env_config.ai.mistral and config.ai.mistral:
                config.ai.mistral.enabled = env_config.ai.mistral.enabled
                config.ai.mistral.model = env_config.ai.mistral.model
                config.ai.mistral.cache_size = env_config.ai.mistral.cache_size
                config.ai.mistral.cost_limit_usd = env_config.ai.mistral.cost_limit_usd

            # AI-level overrides
            config.ai.primary_provider = env_config.ai.primary_provider
            config.ai.enable_fallback = env_config.ai.enable_fallback

        # Validation overrides
        if env_config.validation:
            config.validation.strict_mode = env_config.validation.strict_mode
            config.validation.max_validation_errors = env_config.validation.max_validation_errors

        return config

    def _apply_environment_variables(self, config: GenerationConfig) -> GenerationConfig:
        """Apply environment variable overrides"""
        env_mappings = {
            'DG_ROWS': ('rows', int),
            'DG_LOCALE': ('locale', str),
            'DG_OUTPUT_DIR': ('output.directory', str),
            'DG_OUTPUT_FORMAT': ('output.format', str),
            'DG_MAX_WORKERS': ('performance.max_workers', int),
            'DG_BATCH_SIZE': ('performance.batch_size', int),
            'DG_MAX_MEMORY_MB': ('performance.max_memory_mb', int),
            'DG_ENABLE_MASKING': ('security.enable_data_masking', bool),
            'DG_ENABLE_ENCRYPTION': ('security.enable_encryption', bool),
            'DG_LOG_LEVEL': ('logging.level', str),
            'DG_DB_HOST': ('database.host', str),
            'DG_DB_PORT': ('database.port', int),
            'DG_DB_NAME': ('database.database', str),

            # AI provider selection
            'DG_AI_PRIMARY_PROVIDER': ('ai.primary_provider', str),
            'DG_AI_ENABLE_FALLBACK': ('ai.enable_fallback', bool),

            # OpenAI specific environment variables
            'DG_OPENAI_ENABLED': ('ai.openai.enabled', bool),
            'DG_OPENAI_API_KEY': ('ai.openai.api_key', str),
            'DG_OPENAI_MODEL': ('ai.openai.model', str),
            'DG_OPENAI_MAX_TOKENS': ('ai.openai.max_tokens', int),
            'DG_OPENAI_TEMPERATURE': ('ai.openai.temperature', float),
            'DG_OPENAI_CACHE_SIZE': ('ai.openai.cache_size', int),
            'DG_OPENAI_COST_LIMIT': ('ai.openai.cost_limit_usd', float),

            # Mistral specific environment variables
            'DG_MISTRAL_ENABLED': ('ai.mistral.enabled', bool),
            'DG_MISTRAL_API_KEY': ('ai.mistral.api_key', str),
            'DG_MISTRAL_MODEL': ('ai.mistral.model', str),
            'DG_MISTRAL_MAX_TOKENS': ('ai.mistral.max_tokens', int),
            'DG_MISTRAL_TEMPERATURE': ('ai.mistral.temperature', float),
            'DG_MISTRAL_CACHE_SIZE': ('ai.mistral.cache_size', int),
            'DG_MISTRAL_COST_LIMIT': ('ai.mistral.cost_limit_usd', float),
        }

        for env_var, (config_path, data_type) in env_mappings.items():
            env_value = os.getenv(env_var)
            if env_value:
                try:
                    # Convert value to appropriate type
                    if data_type == bool:
                        converted_value = env_value.lower() in ('true', '1', 'yes', 'on')
                    else:
                        converted_value = data_type(env_value)

                    if config_path == 'output.directory':
                        success = self.update_output_directory(config, converted_value, cleanup_old=False)
                        if success:
                            self.logger.info(f"🌍 Environment override: {env_var} applied")
                        else:
                            self.logger.warning(f"🌍 Environment override: {env_var} failed to apply")
                    else:
                        # Set nested configuration value
                        self._set_nested_config_value(config, config_path, converted_value)

                except (ValueError, TypeError) as e:
                    self.logger.warning(f"Invalid environment variable {env_var}={env_value}: {e}")

        return config

    def _set_nested_config_value(self, config: GenerationConfig,
                                 config_path: str, value: Any):
        """Set nested configuration value using dot notation"""
        parts = config_path.split('.')
        current = config

        # Navigate to the parent object
        for part in parts[:-1]:
            current = getattr(current, part)

        # Set the final value
        setattr(current, parts[-1], value)

    # ===================== CONFIGURATION VALIDATION =====================

    def _validate_configuration(self, config: GenerationConfig):
        """Comprehensive configuration validation"""
        errors = []
        warnings = []

        # Basic validation
        if config.rows <= 0:
            errors.append("Rows must be positive")

        if not config.tables:
            warnings.append("No tables defined in configuration")

        # Performance validation
        if config.performance.max_workers <= 0:
            errors.append("Max workers must be positive")

        if config.performance.batch_size <= 0:
            errors.append("Batch size must be positive")

        if config.performance.max_memory_mb <= 0:
            errors.append("Max memory must be positive")

        # Output validation
        if not config.output.directory:
            errors.append("Output directory must be specified")

        if config.output.format not in ["csv", "json", "jsonl", "parquet", "sql_query", "xlsx", "dsv", "fixed", "fwf"]:
            errors.append(f"Unsupported output format: {config.output.format}")

        # Database validation
        if config.database.port <= 0 or config.database.port > 65535:
            errors.append("Database port must be between 1 and 65535")

        # Security validation
        if config.security.enable_encryption and not config.security.encryption_key_file:
            warnings.append("Encryption enabled but no key file specified")

        # AI validation
        self._validate_ai_config_comprehensive(config.ai, errors, warnings)

        # Validation settings validation
        if config.validation.quality_threshold < 0 or config.validation.quality_threshold > 1:
            errors.append("Quality threshold must be between 0 and 1")

        if not config.output.directory:
            errors.append("Output directory must be specified")
        else:
            # Check if directory can be created
            try:
                # Test directory creation without actually creating it
                from pathlib import Path
                output_path = Path(config.output.directory)

                # Check parent directory is writable
                parent_dir = output_path.parent
                if not parent_dir.exists():
                    warnings.append(f"Parent directory does not exist: {parent_dir}")
                elif not os.access(parent_dir, os.W_OK):
                    errors.append(f"No write permission for parent directory: {parent_dir}")

                # Check report directory too
                report_path = Path(config.output.report_directory)
                if not report_path.parent.exists():
                    warnings.append(f"Report directory parent does not exist: {report_path.parent}")

            except Exception as e:
                warnings.append(f"Could not validate output directory: {e}")

        # Table schema validation
        for i, table in enumerate(config.tables):
            table_errors = self._validate_table_schema(table, i)
            errors.extend(table_errors)

        # Log results
        if warnings:
            for warning in warnings:
                self.logger.warning(f"Configuration warning: {warning}")

        if errors:
            error_msg = "Configuration validation failed:\n" + "\n".join(f"  - {error}" for error in errors)
            raise ValueError(error_msg)

        self.logger.info("Configuration validation passed")

    def _validate_ai_config_comprehensive(self, ai_config: AIConfig, errors: List[str], warnings: List[str]):
        """Comprehensive AI configuration validation"""
        if not ai_config:
            return

        # Validate primary provider
        valid_providers = ["openai", "mistral"]
        if ai_config.primary_provider not in valid_providers:
            errors.append(
                f"AI: Invalid primary provider '{ai_config.primary_provider}'. Valid options: {valid_providers}")

        # Validate OpenAI configuration if enabled
        if ai_config.openai and ai_config.openai.enabled:
            openai_errors, openai_warnings = self._validate_provider_config(ai_config.openai, "OpenAI")
            errors.extend(openai_errors)
            warnings.extend(openai_warnings)

        # Validate Mistral configuration if enabled
        if ai_config.mistral and ai_config.mistral.enabled:
            mistral_errors, mistral_warnings = self._validate_provider_config(ai_config.mistral, "Mistral")
            errors.extend(mistral_errors)
            warnings.extend(mistral_warnings)

        # Check if at least one provider is available
        active_providers = ai_config.get_active_providers()
        if not active_providers:
            warnings.append("AI: No AI providers are enabled")

        # Validate primary provider is actually enabled
        primary_config = ai_config.get_primary_provider_config()
        if primary_config and not primary_config.enabled:
            warnings.append(f"AI: Primary provider '{ai_config.primary_provider}' is not enabled")

    def _validate_provider_config(self, provider_config, provider_name: str) -> tuple[List[str], List[str]]:
        """Validate individual AI provider configuration"""
        errors = []
        warnings = []

        # Check API key availability
        api_key = provider_config.get_api_key()
        if not api_key:
            errors.append(f"{provider_name}: No API key found in config, file, or environment variable")
        elif len(api_key) < 20:  # Basic sanity check
            warnings.append(f"{provider_name}: API key seems too short, please verify")

        # Validate model name based on provider
        if provider_name == "OpenAI":
            valid_models = ['gpt-3.5-turbo', 'gpt-3.5-turbo-16k', 'gpt-4', 'gpt-4-turbo-preview', 'gpt-4-32k']
        else:  # Mistral
            valid_models = ['mistral-tiny', 'mistral-small', 'mistral-medium', 'mistral-large']

        if provider_config.model not in valid_models:
            warnings.append(f"{provider_name}: Unknown model '{provider_config.model}', may not be supported")

        # Validate numeric parameters
        if provider_config.max_tokens <= 0:
            errors.append(f"{provider_name}: max_tokens must be positive")
        elif provider_config.max_tokens > 4096:
            warnings.append(f"{provider_name}: max_tokens > 4096 may be expensive and slow")

        # Temperature validation (different ranges for different providers)
        temp_max = 2.0 if provider_name == "OpenAI" else 1.0
        if not (0.0 <= provider_config.temperature <= temp_max):
            errors.append(f"{provider_name}: temperature must be between 0.0 and {temp_max}")

        if provider_config.cache_size <= 0:
            errors.append(f"{provider_name}: cache_size must be positive")
        elif provider_config.cache_size > 1000:
            warnings.append(f"{provider_name}: Large cache_size may consume significant memory")

        if provider_config.timeout_seconds <= 0:
            errors.append(f"{provider_name}: timeout_seconds must be positive")

        if provider_config.retry_attempts < 0:
            errors.append(f"{provider_name}: retry_attempts cannot be negative")

        # Cost limit validation
        if provider_config.cost_limit_usd is not None:
            if provider_config.cost_limit_usd <= 0:
                errors.append(f"{provider_name}: cost_limit_usd must be positive if specified")
            elif provider_config.cost_limit_usd < 1.0:
                warnings.append(f"{provider_name}: Very low cost limit may restrict functionality")

        # Check file path if specified
        if hasattr(provider_config, 'api_key_file') and provider_config.api_key_file:
            if not os.path.exists(provider_config.api_key_file):
                errors.append(f"{provider_name}: API key file not found: {provider_config.api_key_file}")
            else:
                # Check file permissions
                try:
                    with open(provider_config.api_key_file, 'r') as f:
                        content = f.read().strip()
                        if not content:
                            warnings.append(f"{provider_name}: API key file is empty")
                except PermissionError:
                    errors.append(f"{provider_name}: Cannot read API key file: {provider_config.api_key_file}")

        return errors, warnings

    def _validate_table_schema(self, table: Dict[str, Any], table_index: int) -> List[str]:
        """Validate individual table schema"""
        errors = []

        if 'table_name' not in table:
            errors.append(f"Table {table_index}: Missing table_name")

        if 'columns' not in table:
            errors.append(f"Table {table_index}: Missing columns")
            return errors

        if not isinstance(table['columns'], list):
            errors.append(f"Table {table_index}: Columns must be a list")
            return errors

        # Validate columns
        column_names = set()
        for j, column in enumerate(table['columns']):
            if not isinstance(column, dict):
                errors.append(f"Table {table_index}, Column {j}: Must be a dictionary")
                continue

            if 'name' not in column:
                errors.append(f"Table {table_index}, Column {j}: Missing name")
                continue

            column_name = column['name']
            if column_name in column_names:
                errors.append(f"Table {table_index}: Duplicate column name '{column_name}'")
            column_names.add(column_name)

            if 'type' not in column:
                errors.append(f"Table {table_index}, Column '{column_name}': Missing type")

        return errors

    # ===================== AI PROVIDER UTILITIES =====================

    def test_ai_connection(self, config: GenerationConfig, provider: str = None) -> Dict[str, Any]:
        """Test AI provider connection and return status"""
        if not config.ai:
            return {
                'status': 'disabled',
                'message': 'AI integration is disabled in configuration'
            }

        # Determine which provider to test
        if provider:
            if provider == "openai":
                return self._test_openai_connection(config.ai.openai)
            elif provider == "mistral":
                return self._test_mistral_connection(config.ai.mistral)
            else:
                return {
                    'status': 'error',
                    'message': f'Unknown provider: {provider}'
                }
        else:
            # Test primary provider
            primary_provider = config.ai.primary_provider
            primary_config = config.ai.get_primary_provider_config()

            if primary_provider == "openai":
                return self._test_openai_connection(primary_config)
            elif primary_provider == "mistral":
                return self._test_mistral_connection(primary_config)
            else:
                return {
                    'status': 'error',
                    'message': f'Invalid primary provider: {primary_provider}'
                }

    def _test_openai_connection(self, openai_config: OpenAIConfig) -> Dict[str, Any]:
        """Test OpenAI connection"""
        if not openai_config or not openai_config.enabled:
            return {
                'status': 'disabled',
                'message': 'OpenAI integration is disabled in configuration'
            }

        api_key = openai_config.get_api_key()
        if not api_key:
            return {
                'status': 'error',
                'message': 'No API key found'
            }

        try:
            # Try to import openai
            import openai

            # Set API key
            openai.api_key = api_key

            # Test with a simple completion
            response = openai.ChatCompletion.create(
                model=openai_config.model,
                messages=[{"role": "user", "content": "Test connection"}],
                max_tokens=10,
                timeout=openai_config.timeout_seconds
            )

            return {
                'status': 'success',
                'message': 'OpenAI connection successful',
                'model': openai_config.model,
                'response_id': response.get('id', 'unknown')
            }

        except ImportError:
            return {
                'status': 'error',
                'message': 'OpenAI package not installed. Run: pip install openai'
            }
        except Exception as e:
            return {
                'status': 'error',
                'message': f'OpenAI connection failed: {str(e)}'
            }

    def _test_mistral_connection(self, mistral_config: MistralConfig) -> Dict[str, Any]:
        """Test Mistral AI connection"""
        if not mistral_config or not mistral_config.enabled:
            return {
                'status': 'disabled',
                'message': 'Mistral AI integration is disabled in configuration'
            }

        api_key = mistral_config.get_api_key()
        if not api_key:
            return {
                'status': 'error',
                'message': 'No API key found'
            }

        try:
            # Try to import mistralai
            try:
                from mistralai import Mistral
                client = Mistral(api_key=api_key)
                client_version = "new"
            except ImportError:
                # Fallback to old client
                try:
                    from mistralai.client import MistralClient
                    client = MistralClient(api_key=api_key)
                    client_version = "old"
                except ImportError:
                    return {
                        'status': 'error',
                        'message': 'Mistral AI package not installed. Run: pip install mistralai'
                    }
            try:
                if client_version == "new":
                    response = client.chat.complete(
                        model=mistral_config.model,
                        messages=[
                            {
                                "role": "user",
                                "content": "Test connection"
                            }
                        ],
                        max_tokens=10
                    )
                else:  # old client
                    from mistralai.models.chat_completion import ChatMessage
                    response = client.chat(
                        model=mistral_config.model,
                        messages=[ChatMessage(role="user", content="Test connection")],
                        max_tokens=10
                    )

                return {
                    'status': 'success',
                    'message': f'Mistral AI connection successful (client: {client_version})',
                    'model': mistral_config.model,
                    'client_version': client_version,
                    'response_id': getattr(response, 'id', 'unknown')
                }

            except Exception as e:
                return {
                    'status': 'error',
                    'message': f'Mistral AI connection failed: {str(e)}'
                }

        except ImportError:
            return {
                'status': 'error',
                'message': 'Mistral AI package not installed. Run: pip install mistralai'
            }
        except Exception as e:
            return {
                'status': 'error',
                'message': f'Mistral AI connection failed: {str(e)}'
            }

    def get_ai_status(self, config: GenerationConfig) -> Dict[str, Any]:
        """Get comprehensive AI configuration status"""
        if not config.ai:
            return {
                'enabled': False,
                'message': 'AI configuration not found'
            }

        status = {
            'enabled': bool(config.ai.get_active_providers()),
            'primary_provider': config.ai.primary_provider,
            'enable_fallback': config.ai.enable_fallback,
            'active_providers': config.ai.get_active_providers(),
            'providers': {}
        }

        # Get OpenAI status
        if config.ai.openai:
            status['providers']['openai'] = self._get_provider_status(config.ai.openai, 'OpenAI')

        # Get Mistral status
        if config.ai.mistral:
            status['providers']['mistral'] = self._get_provider_status(config.ai.mistral, 'Mistral')

        return status

    def _get_provider_status(self, provider_config, provider_name: str) -> Dict[str, Any]:
        """Get status for individual AI provider"""
        status = {
            'enabled': provider_config.enabled,
            'api_key_source': None,
            'api_key_available': False,
            'model': provider_config.model,
            'cache_size': provider_config.cache_size,
            'cost_limit_usd': provider_config.cost_limit_usd,
            'configuration_valid': True,
            'errors': [],
            'warnings': []
        }

        if provider_config.enabled:
            # Check API key source
            if provider_config.api_key:
                status['api_key_source'] = 'direct_config'
                status['api_key_available'] = True
            elif hasattr(provider_config, 'api_key_file') and provider_config.api_key_file and os.path.exists(
                    provider_config.api_key_file):
                status['api_key_source'] = 'file'
                try:
                    with open(provider_config.api_key_file, 'r') as f:
                        content = f.read().strip()
                        status['api_key_available'] = bool(content)
                except:
                    status['api_key_available'] = False
            elif os.getenv(provider_config.api_key_env_var):
                status['api_key_source'] = 'environment_variable'
                status['api_key_available'] = True
            else:
                status['api_key_source'] = 'none'
                status['api_key_available'] = False
                status['errors'].append('No API key found')

            # Validate configuration
            if not status['api_key_available']:
                status['configuration_valid'] = False

            # Test connection if requested
            if status['api_key_available']:
                if provider_name == 'OpenAI':
                    connection_test = self._test_openai_connection(provider_config)
                else:
                    connection_test = self._test_mistral_connection(provider_config)

                status['connection_test'] = connection_test
                if connection_test['status'] != 'success':
                    status['configuration_valid'] = False

        return status

    # ===================== CONFIGURATION TEMPLATES WITH AI =====================

    def generate_config_template_with_ai(self, table_schemas: List[Dict[str, Any]],
                                         template_type: str = "basic",
                                         enable_ai: bool = False,
                                         primary_provider: str = "openai") -> GenerationConfig:
        """Generate configuration template with AI settings"""
        config = self.generate_config_template(table_schemas, template_type)

        if enable_ai:
            if template_type == "basic":
                config.ai = AIConfig(
                    openai=OpenAIConfig(
                        enabled=(primary_provider == "openai"),
                        model="gpt-3.5-turbo",
                        cache_size=50,
                        cost_limit_usd=5.0
                    ),
                    mistral=MistralConfig(
                        enabled=(primary_provider == "mistral"),
                        model="mistral-small",
                        cache_size=50,
                        cost_limit_usd=3.0
                    ),
                    primary_provider=primary_provider
                )
            elif template_type == "advanced":
                config.ai = AIConfig(
                    openai=OpenAIConfig(
                        enabled=False,
                        model="gpt-3.5-turbo",
                        cache_size=100,
                        cost_limit_usd=15.0,
                        temperature=0.7,
                        timeout_seconds=45
                    ),
                    mistral=MistralConfig(
                        enabled=False,
                        model="mistral-medium",
                        cache_size=100,
                        cost_limit_usd=12.0,
                        temperature=0.7,
                        timeout_seconds=45
                    ),
                    primary_provider=primary_provider,
                    enable_fallback=True
                )
            elif template_type == "production":
                config.ai = AIConfig(
                    openai=OpenAIConfig(
                        enabled=False,
                        model="gpt-3.5-turbo",
                        cache_size=200,
                        cost_limit_usd=100.0,
                        temperature=0.5,
                        timeout_seconds=60,
                        retry_attempts=5,
                        fallback_enabled=True
                    ),
                    mistral=MistralConfig(
                        enabled=False,
                        model="mistral-large",
                        cache_size=200,
                        cost_limit_usd=80.0,
                        temperature=0.5,
                        timeout_seconds=60,
                        retry_attempts=5,
                        fallback_enabled=True
                    ),
                    primary_provider=primary_provider,
                    enable_fallback=True
                )

        return config

    # ===================== CONFIGURATION SAVING WITH AI =====================

    def save_configuration(self, config: GenerationConfig,
                           output_path: str, format: str = 'json',
                           include_sensitive: bool = False):
        """Save configuration to file with option to exclude sensitive data"""
        output_path = Path(output_path)
        if not config.output.directory or not Path(config.output.directory).exists():
            self.logger.warning("⚠️  Output directory doesn't exist, recreating...")
            # Trigger directory creation
            config.output._setup_output_directory()

        # Convert config to dictionary
        config_dict = self._config_to_dict(config, include_sensitive)

        self._save_json(config_dict, output_path)

        self.logger.info(f"💾 Configuration saved to: {output_path}")
        self.logger.info(f"📁 Output will be saved to: {config.output.directory}")
        self.logger.info(f"📊 Reports will be saved to: {config.output.report_directory}")

        summary = self.get_config_summary(config)
        self.logger.info(
            f"📋 Summary: {summary['total_tables']} tables, {summary['total_rows']:,} rows, {summary['output_format']} format")

        if summary.get('ai', {}).get('enabled'):
            active_providers = summary['ai']['active_providers']
            self.logger.info(
                f"🤖 AI: {summary['ai']['primary_provider']} primary, {len(active_providers)} active provider(s)")

    def _config_to_dict(self, config: GenerationConfig, include_sensitive: bool = False) -> Dict[str, Any]:
        """Convert configuration object to dictionary with option to exclude sensitive data"""
        config_dict = {}

        # Main fields
        config_dict['environment'] = config.environment
        config_dict['locale'] = config.locale
        config_dict['rows'] = config.rows
        config_dict['tables'] = config.tables

        # Sub-configurations
        if config.database:
            db_config = asdict(config.database)
            if not include_sensitive:
                db_config['password'] = '[HIDDEN]'
            config_dict['database'] = db_config

        if config.performance:
            config_dict['performance'] = asdict(config.performance)

        if config.security:
            sec_config = asdict(config.security)
            if not include_sensitive and sec_config.get('encryption_key_file'):
                sec_config['encryption_key_file'] = '[HIDDEN]'
            config_dict['security'] = sec_config

        # Enhanced AI configuration serialization
        if config.ai:
            ai_config = {
                'primary_provider': config.ai.primary_provider,
                'enable_fallback': config.ai.enable_fallback,
                'shared_cache_size': config.ai.shared_cache_size
            }

            if config.ai.openai:
                openai_config = asdict(config.ai.openai)
                if not include_sensitive:
                    # Hide sensitive OpenAI data
                    if openai_config.get('api_key'):
                        openai_config['api_key'] = '[HIDDEN]'
                    if openai_config.get('api_key_file'):
                        openai_config['api_key_file'] = '[HIDDEN]'
                ai_config['openai'] = openai_config

            if config.ai.mistral:
                mistral_config = asdict(config.ai.mistral)
                if not include_sensitive:
                    # Hide sensitive Mistral data
                    if mistral_config.get('api_key'):
                        mistral_config['api_key'] = '[HIDDEN]'
                    if mistral_config.get('api_key_file'):
                        mistral_config['api_key_file'] = '[HIDDEN]'
                ai_config['mistral'] = mistral_config

            config_dict['ai'] = ai_config

        if config.output:
            config_dict['output'] = asdict(config.output)

        if config.validation:
            config_dict['validation'] = asdict(config.validation)

        if config.logging:
            config_dict['logging'] = asdict(config.logging)

        return config_dict

    # ===================== ENHANCED CONFIGURATION WIZARD =====================

    def create_config_wizard_with_ai(self) -> GenerationConfig:
        """Interactive configuration wizard with AI options"""
        print("=== Enhanced Data Generator Configuration Wizard ===")
        print("This wizard will help you create a configuration file with AI support.")
        print()

        # Basic settings
        environment = input("Environment (development/testing/production) [development]: ").strip() or "development"
        rows = int(input("Number of rows to generate [1000]: ").strip() or "1000")
        locale = input("Locale [en_GB]: ").strip() or "en_GB"

        # Output settings
        output_format = input("Output format (csv/json/parquet) [csv]: ").strip() or "csv"
        output_dir = input("Output directory [./output]: ").strip() or "./output"

        # Performance settings
        max_workers = int(input("Max workers [4]: ").strip() or "4")
        batch_size = int(input("Batch size [10000]: ").strip() or "10000")

        # Security settings
        enable_masking = input("Enable data masking (y/n) [n]: ").strip().lower() in ['y', 'yes']
        audit_enabled = input("Enable audit logging (y/n) [y]: ").strip().lower() in ['y', 'yes', '']

        # AI settings
        print("\n--- AI Integration Settings ---")
        enable_ai = input("Enable AI integration (y/n) [n]: ").strip().lower() in ['y', 'yes']

        ai_config = AIConfig()
        if enable_ai:
            print("Choose AI providers to enable:")
            print("1. OpenAI only")
            print("2. Mistral AI only")
            print("3. Both OpenAI and Mistral AI")

            provider_choice = input("Choose option (1/2/3) [1]: ").strip() or "1"

            if provider_choice in ["1", "3"]:
                # OpenAI configuration
                print("\n--- OpenAI Configuration ---")
                openai_config = self._configure_openai_wizard()
                ai_config.openai = openai_config

            if provider_choice in ["2", "3"]:
                # Mistral configuration
                print("\n--- Mistral AI Configuration ---")
                mistral_config = self._configure_mistral_wizard()
                ai_config.mistral = mistral_config

            # Primary provider selection
            if provider_choice == "3":
                primary = input("Primary AI provider (openai/mistral) [openai]: ").strip() or "openai"
                ai_config.primary_provider = primary
                ai_config.enable_fallback = input(
                    "Enable fallback to secondary provider (y/n) [y]: ").strip().lower() in ['y', 'yes', '']
            elif provider_choice == "1":
                ai_config.primary_provider = "openai"
            elif provider_choice == "2":
                ai_config.primary_provider = "mistral"

        output_config = OutputConfig(format=output_format)
        if output_dir != "./output":
            output_config.change_directory(output_dir)

        # Create configuration
        config = GenerationConfig(
            environment=environment,
            rows=rows,
            locale=locale,
            performance=PerformanceConfig(
                max_workers=max_workers,
                batch_size=batch_size
            ),
            security=SecurityConfig(
                enable_data_masking=enable_masking,
                audit_enabled=audit_enabled
            ),
            ai=ai_config,
            output=OutputConfig(
                format=output_format,
                directory=output_dir
            )
        )

        print("\n=== Configuration Created Successfully ===")

        # Show AI status
        if enable_ai:
            print("\n--- AI Configuration Status ---")
            ai_status = self.get_ai_status(config)
            print(f"AI Enabled: {ai_status['enabled']}")
            print(f"Primary Provider: {ai_status['primary_provider']}")
            print(f"Active Providers: {', '.join(ai_status['active_providers'])}")

            for provider_name, provider_status in ai_status['providers'].items():
                print(f"\n{provider_name.upper()}:")
                print(f"  Enabled: {provider_status['enabled']}")
                print(f"  API Key Available: {provider_status['api_key_available']}")
                print(f"  Model: {provider_status['model']}")

                if provider_status['errors']:
                    print("  Errors:")
                    for error in provider_status['errors']:
                        print(f"    - {error}")

            print("\n--- Cost Estimation ---")
            estimated_requests = max(rows // 1000, 10)  # Estimate based on data size
            cost_estimates = self.estimate_ai_costs(config, estimated_requests)

            if cost_estimates:
                print(f"💰 Estimated costs for ~{estimated_requests} AI requests:")
                for provider, cost in cost_estimates.items():
                    print(f"   {provider.upper()}: ~${cost:.4f}")

                total_cost = sum(cost_estimates.values())
                if total_cost > 10.0:
                    print(f"⚠️  Total estimated cost: ${total_cost:.2f} (consider setting cost limits)")
            else:
                print("   No cost estimates available (providers not configured)")

            # ENHANCEMENT: Add model recommendations
            print("\n--- Model Recommendations ---")
            recommendations = self.get_ai_model_recommendations("general")
            print("💡 Recommended models for general use:")
            for provider, model in recommendations.items():
                if provider in ai_status['active_providers']:
                    current_model = ai_status['providers'][provider]['model']
                    if current_model == model:
                        print(f"   {provider.upper()}: {model} ✅ (currently selected)")
                    else:
                        print(f"   {provider.upper()}: {model} (you selected: {current_model})")

        return config

    def _configure_openai_wizard(self) -> OpenAIConfig:
        """OpenAI configuration wizard"""
        print("OpenAI API Key Configuration:")
        print("1. Enter API key directly (not recommended for production)")
        print("2. Use environment variable OPENAI_API_KEY")
        print("3. Use API key file")

        key_method = input("Choose method (1/2/3) [2]: ").strip() or "2"

        openai_config = OpenAIConfig(enabled=True)

        if key_method == "1":
            api_key = input("Enter OpenAI API key: ").strip()
            openai_config.api_key = api_key if api_key else None
        elif key_method == "3":
            key_file = input("Path to API key file: ").strip()
            openai_config.api_key_file = key_file if key_file else None
        # For method 2, we keep default environment variable

        model = input("OpenAI model [gpt-3.5-turbo]: ").strip() or "gpt-3.5-turbo"
        cache_size = int(input("Cache size [100]: ").strip() or "100")
        cost_limit = input("Cost limit in USD [10.0]: ").strip()

        openai_config.model = model
        openai_config.cache_size = cache_size
        openai_config.cost_limit_usd = float(cost_limit) if cost_limit else 10.0

        return openai_config

    def _configure_mistral_wizard(self) -> MistralConfig:
        """Mistral AI configuration wizard"""
        print("Mistral AI API Key Configuration:")
        print("1. Enter API key directly (not recommended for production)")
        print("2. Use environment variable MISTRAL_API_KEY")
        print("3. Use API key file")

        key_method = input("Choose method (1/2/3) [2]: ").strip() or "2"

        mistral_config = MistralConfig(enabled=True)

        if key_method == "1":
            api_key = input("Enter Mistral API key: ").strip()
            mistral_config.api_key = api_key if api_key else None
        elif key_method == "3":
            key_file = input("Path to API key file: ").strip()
            mistral_config.api_key_file = key_file if key_file else None
        # For method 2, we keep default environment variable

        print("Available Mistral models:")
        print("1. mistral-tiny (fastest, cheapest)")
        print("2. mistral-small (balanced)")
        print("3. mistral-medium (better quality)")
        print("4. mistral-large (best quality)")

        model_choice = input("Choose model (1/2/3/4) [2]: ").strip() or "2"
        model_map = {
            "1": "mistral-tiny",
            "2": "mistral-small",
            "3": "mistral-medium",
            "4": "mistral-large"
        }
        model = model_map.get(model_choice, "mistral-small")

        cache_size = int(input("Cache size [100]: ").strip() or "100")
        cost_limit = input("Cost limit in USD [8.0]: ").strip()

        mistral_config.model = model
        mistral_config.cache_size = cache_size
        mistral_config.cost_limit_usd = float(cost_limit) if cost_limit else 8.0

        return mistral_config

    def generate_config_template(self, table_schemas: List[Dict[str, Any]],
                                 template_type: str = "basic") -> GenerationConfig:
        """Generate configuration template from table schemas"""
        if template_type == "basic":
            return self._generate_basic_template(table_schemas)
        elif template_type == "advanced":
            return self._generate_advanced_template(table_schemas)
        elif template_type == "production":
            return self._generate_production_template(table_schemas)
        else:
            raise ValueError(f"Unknown template type: {template_type}")

    def _generate_basic_template(self, table_schemas: List[Dict[str, Any]]) -> GenerationConfig:
        """Generate basic configuration template"""
        tables = []

        for schema in table_schemas:
            table = {
                'table_name': schema.get('name', 'unnamed_table'),
                'columns': []
            }

            # Generate columns from schema
            for column_info in schema.get('columns', []):
                column = {
                    'name': column_info.get('name'),
                    'type': self._map_database_type_to_generator_type(column_info.get('type')),
                    'nullable': column_info.get('nullable', True)
                }

                # Add constraints
                if column_info.get('primary_key'):
                    column['constraints'] = ['PK']

                # Add basic rules based on column name and type
                rule = self._infer_rule_from_column(column_info)
                if rule:
                    column['rule'] = rule

                table['columns'].append(column)

            tables.append(table)

        config = GenerationConfig(
            environment=Environment.DEVELOPMENT.value,
            tables=tables,
            rows=1000,
            ai=AIConfig()  # Default AI config with both providers disabled
        )

        custom_output_dir = os.getenv('DG_TEMPLATE_OUTPUT_DIR')
        if custom_output_dir:
            success = self.update_output_directory(config, custom_output_dir, cleanup_old=False)
            if not success:
                self.logger.warning(f"⚠️  Failed to set custom template output directory, using default")
            else:
                self.logger.info(f"📁 Template output directory set to: {config.output.directory}")

        return config

    def _generate_advanced_template(self, table_schemas: List[Dict[str, Any]]) -> GenerationConfig:
        """Generate advanced configuration template with more features"""
        config = self._generate_basic_template(table_schemas)

        # Enhanced performance settings
        config.performance = PerformanceConfig(
            max_workers=4,
            batch_size=10000,
            enable_parallel=True,
            enable_streaming=True
        )

        # Enhanced security settings
        config.security = SecurityConfig(
            enable_data_masking=True,
            audit_enabled=True
        )

        # Enhanced validation
        config.validation = ValidationConfig(
            strict_mode=True,
            enable_data_quality_analysis=True,
            enable_anomaly_detection=True
        )

        model_recommendations = self.get_ai_model_recommendations("general")

        # AI enabled with moderate settings
        config.ai = AIConfig(
            openai=OpenAIConfig(
                enabled=False,
                model=model_recommendations.get("openai", "gpt-3.5-turbo"),
                cache_size=100,
                cost_limit_usd=15.0
            ),
            mistral=MistralConfig(
                enabled=False,
                model=model_recommendations.get("mistral", "mistral-small"),
                cache_size=100,
                cost_limit_usd=12.0
            ),
            primary_provider="openai"
        )

        # Add foreign key relationships
        self._infer_foreign_keys(config.tables)

        return config

    def _generate_production_template(self, table_schemas: List[Dict[str, Any]]) -> GenerationConfig:
        """Generate production-ready configuration template"""
        config = self._generate_advanced_template(table_schemas)

        config.environment = Environment.PRODUCTION.value
        config.rows = 100000

        # Production performance settings
        config.performance = PerformanceConfig(
            max_workers=8,
            batch_size=50000,
            max_memory_mb=4000,
            enable_parallel=True,
            enable_streaming=True
        )

        # Production security settings
        config.security = SecurityConfig(
            enable_data_masking=True,
            enable_encryption=True,
            audit_enabled=True
        )

        # Production validation
        config.validation = ValidationConfig(
            strict_mode=True,
            max_validation_errors=0,
            quality_threshold=0.95,
            enable_data_quality_analysis=True,
            enable_anomaly_detection=True
        )

        # Production AI settings
        config.ai = AIConfig(
            openai=OpenAIConfig(
                enabled=False,
                model="gpt-3.5-turbo",
                cache_size=200,
                cost_limit_usd=100.0,
                temperature=0.5,
                timeout_seconds=60,
                retry_attempts=5,
                fallback_enabled=True
            ),
            mistral=MistralConfig(
                enabled=False,
                model="mistral-large",
                cache_size=200,
                cost_limit_usd=80.0,
                temperature=0.5,
                timeout_seconds=60,
                retry_attempts=5,
                fallback_enabled=True
            ),
            primary_provider="mistral",
            enable_fallback=True
        )

        # Production output settings
        config.output = OutputConfig(
            format="parquet",
            compression="snappy",
            directory="./production_output"
        )

        return config

    def _map_database_type_to_generator_type(self, db_type: str) -> str:
        """Map database types to generator types"""
        if not db_type:
            return 'str'

        db_type_lower = db_type.lower()

        type_mapping = {
            'integer': 'int',
            'int': 'int',
            'bigint': 'int',
            'smallint': 'int',
            'decimal': 'float',
            'numeric': 'float',
            'real': 'float',
            'double': 'float',
            'varchar': 'str',
            'char': 'str',
            'text': 'str',
            'string': 'str',
            'boolean': 'bool',
            'bool': 'bool',
            'date': 'date',
            'datetime': 'datetime',
            'timestamp': 'datetime',
            'uuid': 'uuid'
        }

        for db_type_pattern, generator_type in type_mapping.items():
            if db_type_pattern in db_type_lower:
                return generator_type

        return 'str'  # Default to string

    def _infer_rule_from_column(self, column_info: Dict[str, Any]) -> Union[dict, str, None]:
        """Infer generation rule from column information"""
        column_name = column_info.get('name', '').lower()
        column_type = column_info.get('type', '').lower()

        # Email detection
        if 'email' in column_name:
            return {'type': 'email'}

        # Phone detection
        if 'phone' in column_name:
            return {'type': 'phone_number'}

        # Name detection
        if any(name_part in column_name for name_part in ['first_name', 'last_name', 'name']):
            return 'name'

        # Age detection
        if 'age' in column_name:
            return {'type': 'range', 'min': 18, 'max': 80}

        # Date detection
        if any(date_part in column_name for date_part in ['date', 'created', 'updated', 'birth']):
            return {'type': 'date_range', 'start': '2020-01-01', 'end': '2024-12-31'}

        # ID detection
        if column_name.endswith('_id') or column_name == 'id':
            if 'int' in column_type:
                return {'type': 'range', 'min': 1, 'max': 999999}
            else:
                return {'type': 'uuid'}

        # Status/Category detection
        if any(status_part in column_name for status_part in ['status', 'type', 'category']):
            return {'type': 'choice', 'value': ['ACTIVE', 'INACTIVE', 'PENDING']}

        return None

    def _infer_foreign_keys(self, tables: List[Dict[str, Any]]):
        """Infer foreign key relationships between tables"""
        # Build table and column registry
        table_columns = {}
        for table in tables:
            table_name = table['table_name']
            table_columns[table_name] = [col['name'] for col in table['columns']]

        # Look for foreign key patterns
        for table in tables:
            foreign_keys = []

            for column in table['columns']:
                column_name = column['name']

                # Look for foreign key pattern (table_name_id -> table_name.id)
                if column_name.endswith('_id') and column_name != 'id':
                    potential_table = column_name[:-3]  # Remove '_id'

                    # Check if table exists and has 'id' column
                    if potential_table in table_columns and 'id' in table_columns[potential_table]:
                        foreign_keys.append({
                            'parent_table': potential_table,
                            'parent_column': 'id',
                            'child_column': column_name
                        })

            if foreign_keys:
                table['foreign_keys'] = foreign_keys

    def _save_json(self, config_dict: Dict[str, Any], output_path: Path):
        """Save configuration as JSON"""
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(config_dict, f, indent=2, ensure_ascii=False)

    # ===================== CONFIGURATION UTILITIES =====================

    @contextmanager
    def temporary_config(self, config: GenerationConfig):
        """Context manager for temporary configuration changes"""
        temp_file = None
        try:
            # Create temporary file
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                temp_file = f.name

            # Save configuration to temp file
            self.save_configuration(config, temp_file, 'json')

            yield temp_file
        finally:
            # Clean up temp file
            if temp_file and os.path.exists(temp_file):
                os.unlink(temp_file)

    def merge_configurations(self, base_config: GenerationConfig,
                             override_config: GenerationConfig) -> GenerationConfig:
        """Merge two configurations with override taking precedence"""
        # Convert to dictionaries for easier merging
        base_dict = self._config_to_dict(base_config)
        override_dict = self._config_to_dict(override_config)

        # Deep merge dictionaries
        merged_dict = self._deep_merge_dicts(base_dict, override_dict)

        merged_config = self._parse_config_dict(merged_dict)

        if (override_config.output.directory != "./output" and
                override_config.output.directory != base_config.output.directory):
            # Use change_directory to properly update both directory and report_directory
            merged_config.output.change_directory(override_config.output.directory)

            self.logger.info(f"📁 Merged config: Updated output directory to {merged_config.output.directory}")
            self.logger.info(f"📁 Merged config: Updated report directory to {merged_config.output.report_directory}")

        return merged_config

    def _deep_merge_dicts(self, base: Dict[str, Any],
                          override: Dict[str, Any]) -> Dict[str, Any]:
        """Deep merge two dictionaries"""
        result = base.copy()

        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge_dicts(result[key], value)
            else:
                result[key] = value

        return result

    def validate_environment_config(self, environment: str) -> bool:
        """Validate if environment configuration is available"""
        return environment in self.environment_configs

    def list_available_environments(self) -> List[str]:
        """List all available environment configurations"""
        return list(self.environment_configs.keys())

    def get_config_summary(self, config: GenerationConfig) -> Dict[str, Any]:
        """Get a summary of configuration settings"""
        summary = {
            'environment': config.environment,
            'total_tables': len(config.tables),
            'total_rows': config.rows,
            'output_format': config.output.format,
            'performance': {
                'max_workers': config.performance.max_workers,
                'batch_size': config.performance.batch_size,
                'streaming_enabled': config.performance.enable_streaming,
                'parallel_enabled': config.performance.enable_parallel
            },
            'security': {
                'masking_enabled': config.security.enable_data_masking,
                'encryption_enabled': config.security.enable_encryption,
                'audit_enabled': config.security.audit_enabled
            },
            'validation': {
                'strict_mode': config.validation.strict_mode,
                'quality_analysis': config.validation.enable_data_quality_analysis
            }
        }

        # Enhanced AI summary
        if config.ai:
            summary['ai'] = {
                'enabled': bool(config.ai.get_active_providers()),
                'primary_provider': config.ai.primary_provider,
                'active_providers': config.ai.get_active_providers(),
                'fallback_enabled': config.ai.enable_fallback
            }

            # OpenAI details
            if config.ai.openai:
                summary['ai']['openai'] = {
                    'enabled': config.ai.openai.enabled,
                    'model': config.ai.openai.model if config.ai.openai.enabled else None,
                    'api_key_available': config.ai.openai.is_available(),
                    'cache_size': config.ai.openai.cache_size,
                    'cost_limit_usd': config.ai.openai.cost_limit_usd
                }

            # Mistral details
            if config.ai.mistral:
                summary['ai']['mistral'] = {
                    'enabled': config.ai.mistral.enabled,
                    'model': config.ai.mistral.model if config.ai.mistral.enabled else None,
                    'api_key_available': config.ai.mistral.is_available(),
                    'cache_size': config.ai.mistral.cache_size,
                    'cost_limit_usd': config.ai.mistral.cost_limit_usd
                }
        else:
            summary['ai'] = {'enabled': False}

        return summary

    def switch_ai_provider(self, config: GenerationConfig, provider: str) -> bool:
        """Switch primary AI provider in configuration"""
        if not config.ai:
            self.logger.error("AI configuration not found")
            return False

        return config.ai.switch_primary_provider(provider)

    def enable_ai_provider(self, config: GenerationConfig, provider: str,
                           model: str = None, cost_limit: float = None) -> bool:
        """Enable and configure an AI provider"""
        if not config.ai:
            config.ai = AIConfig()

        if provider == "openai":
            if not config.ai.openai:
                config.ai.openai = OpenAIConfig()

            config.ai.openai.enabled = True
            if model:
                config.ai.openai.model = model
            if cost_limit:
                config.ai.openai.cost_limit_usd = cost_limit

            self.logger.info(f"OpenAI enabled with model: {config.ai.openai.model}")
            return True

        elif provider == "mistral":
            if not config.ai.mistral:
                config.ai.mistral = MistralConfig()

            config.ai.mistral.enabled = True
            if model:
                config.ai.mistral.model = model
            if cost_limit:
                config.ai.mistral.cost_limit_usd = cost_limit

            self.logger.info(f"Mistral AI enabled with model: {config.ai.mistral.model}")
            return True

        else:
            self.logger.error(f"Unknown AI provider: {provider}")
            return False

    def disable_ai_provider(self, config: GenerationConfig, provider: str) -> bool:
        """Disable an AI provider"""
        if not config.ai:
            return False

        if provider == "openai" and config.ai.openai:
            config.ai.openai.enabled = False
            self.logger.info("OpenAI disabled")
            return True

        elif provider == "mistral" and config.ai.mistral:
            config.ai.mistral.enabled = False
            self.logger.info("Mistral AI disabled")
            return True

        else:
            self.logger.error(f"Unknown AI provider: {provider}")
            return False

    def get_ai_cost_summary(self, config: GenerationConfig) -> Dict[str, Any]:
        """Get AI cost summary and limits"""
        if not config.ai:
            return {'enabled': False}

        summary = {
            'enabled': True,
            'providers': {}
        }

        if config.ai.openai and config.ai.openai.enabled:
            summary['providers']['openai'] = {
                'cost_limit_usd': config.ai.openai.cost_limit_usd,
                'model': config.ai.openai.model,
                'cache_size': config.ai.openai.cache_size
            }

        if config.ai.mistral and config.ai.mistral.enabled:
            summary['providers']['mistral'] = {
                'cost_limit_usd': config.ai.mistral.cost_limit_usd,
                'model': config.ai.mistral.model,
                'cache_size': config.ai.mistral.cache_size
            }

        return summary

    def validate_ai_keys(self, config: GenerationConfig) -> Dict[str, Any]:
        """Validate AI API keys availability"""
        results = {}

        if config.ai:
            if config.ai.openai and config.ai.openai.enabled:
                api_key = config.ai.openai.get_api_key()
                results['openai'] = {
                    'available': bool(api_key),
                    'source': 'environment' if os.getenv(config.ai.openai.api_key_env_var) else 'config',
                    'valid_length': len(api_key) >= 20 if api_key else False
                }

            if config.ai.mistral and config.ai.mistral.enabled:
                api_key = config.ai.mistral.get_api_key()
                results['mistral'] = {
                    'available': bool(api_key),
                    'source': 'environment' if os.getenv(config.ai.mistral.api_key_env_var) else 'config',
                    'valid_length': len(api_key) >= 20 if api_key else False
                }

        return results

    def create_minimal_ai_config(self, provider: str = "openai",
                                 enable_fallback: bool = True) -> GenerationConfig:
        """Create minimal configuration with AI enabled"""
        config = GenerationConfig(
            environment=Environment.DEVELOPMENT.value,
            rows=1000,
            tables=[],
            ai=AIConfig(
                primary_provider=provider,
                enable_fallback=enable_fallback
            )
        )

        if provider == "openai" or enable_fallback:
            config.ai.openai = OpenAIConfig(
                enabled=(provider == "openai"),
                model="gpt-3.5-turbo",
                cache_size=50,
                cost_limit_usd=5.0
            )

        if provider == "mistral" or enable_fallback:
            config.ai.mistral = MistralConfig(
                enabled=(provider == "mistral"),
                model="mistral-small",
                cache_size=50,
                cost_limit_usd=3.0
            )

        return config

    def _parse_security_config(self, config_dict: Dict[str, Any]) -> SecurityConfig:
        """Parse security configuration with proper error handling"""
        try:
            security_data = config_dict.get('security', {})

            # Handle the case where security is a boolean (legacy)
            if isinstance(security_data, bool):
                return SecurityConfig(enable_data_masking=security_data)

            # Handle dictionary configuration
            if isinstance(security_data, dict):
                return SecurityConfig.from_dict(security_data)

            # Default security config
            return SecurityConfig()

        except TypeError as e:
            # More helpful error message
            available_params = [
                'enable_data_masking', 'enable_encryption', 'audit_enabled',
                'masking_rules', 'sensitivity_map', 'encrypt_fields'
            ]

            raise TypeError(
                f"Invalid security configuration parameter. "
                f"Available parameters: {available_params}. "
                f"Original error: {e}"
            )

    # ===================== AI PROVIDER MANAGEMENT =====================

    def list_available_ai_models(self, provider: str) -> List[str]:
        """List available models for an AI provider"""
        if provider == "openai":
            return [
                'gpt-3.5-turbo',
                'gpt-3.5-turbo-16k',
                'gpt-4',
                'gpt-4-turbo-preview',
                'gpt-4-32k'
            ]
        elif provider == "mistral":
            return [
                'mistral-tiny',
                'mistral-small',
                'mistral-medium',
                'mistral-large'
            ]
        else:
            return []

    def get_ai_model_recommendations(self, use_case: str = "general") -> Dict[str, str]:
        """Get AI model recommendations based on use case"""
        recommendations = {
            "general": {
                "openai": "gpt-3.5-turbo",
                "mistral": "mistral-small"
            },
            "cost_effective": {
                "openai": "gpt-3.5-turbo",
                "mistral": "mistral-tiny"
            },
            "high_quality": {
                "openai": "gpt-4",
                "mistral": "mistral-large"
            },
            "production": {
                "openai": "gpt-3.5-turbo",
                "mistral": "mistral-medium"
            }
        }

        return recommendations.get(use_case, recommendations["general"])

    def estimate_ai_costs(self, config: GenerationConfig,
                          estimated_requests: int = 100) -> Dict[str, float]:
        """Estimate AI costs based on configuration and usage"""
        costs = {}

        if not config.ai:
            return costs

        # Cost per 1K tokens (approximate)
        cost_rates = {
            "openai": {
                "gpt-3.5-turbo": 0.002,
                "gpt-3.5-turbo-16k": 0.003,
                "gpt-4": 0.03,
                "gpt-4-turbo-preview": 0.01,
                "gpt-4-32k": 0.06
            },
            "mistral": {
                "mistral-tiny": 0.0002,
                "mistral-small": 0.0006,
                "mistral-medium": 0.0027,
                "mistral-large": 0.008
            }
        }

        # Estimate tokens per request (rough estimate)
        tokens_per_request = 1500  # prompt + response

        if config.ai.openai and config.ai.openai.enabled:
            model = config.ai.openai.model
            rate = cost_rates["openai"].get(model, 0.002)
            estimated_cost = (estimated_requests * tokens_per_request / 1000) * rate
            costs["openai"] = round(estimated_cost, 4)

        if config.ai.mistral and config.ai.mistral.enabled:
            model = config.ai.mistral.model
            rate = cost_rates["mistral"].get(model, 0.0006)
            estimated_cost = (estimated_requests * tokens_per_request / 1000) * rate
            costs["mistral"] = round(estimated_cost, 4)

        return costs

    def export_ai_config(self, config: GenerationConfig,
                         include_keys: bool = False) -> Dict[str, Any]:
        """Export AI configuration for sharing or backup"""
        if not config.ai:
            return {}

        export_data = {
            "primary_provider": config.ai.primary_provider,
            "enable_fallback": config.ai.enable_fallback,
            "shared_cache_size": config.ai.shared_cache_size,
            "providers": {}
        }

        if config.ai.openai:
            openai_data = {
                "enabled": config.ai.openai.enabled,
                "model": config.ai.openai.model,
                "max_tokens": config.ai.openai.max_tokens,
                "temperature": config.ai.openai.temperature,
                "cache_size": config.ai.openai.cache_size,
                "timeout_seconds": config.ai.openai.timeout_seconds,
                "retry_attempts": config.ai.openai.retry_attempts,
                "cost_limit_usd": config.ai.openai.cost_limit_usd
            }

            if include_keys:
                openai_data["api_key_env_var"] = config.ai.openai.api_key_env_var
                openai_data["api_key_file"] = config.ai.openai.api_key_file

            export_data["providers"]["openai"] = openai_data

        if config.ai.mistral:
            mistral_data = {
                "enabled": config.ai.mistral.enabled,
                "model": config.ai.mistral.model,
                "max_tokens": config.ai.mistral.max_tokens,
                "temperature": config.ai.mistral.temperature,
                "cache_size": config.ai.mistral.cache_size,
                "timeout_seconds": config.ai.mistral.timeout_seconds,
                "retry_attempts": config.ai.mistral.retry_attempts,
                "cost_limit_usd": config.ai.mistral.cost_limit_usd
            }

            if include_keys:
                mistral_data["api_key_env_var"] = config.ai.mistral.api_key_env_var
                mistral_data["api_key_file"] = config.ai.mistral.api_key_file

            export_data["providers"]["mistral"] = mistral_data

        return export_data