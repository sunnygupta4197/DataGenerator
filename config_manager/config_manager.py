import os
import json
import logging
from typing import Dict, Any, List, Optional, Union
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, asdict, field
from enum import Enum
import tempfile
from contextlib import contextmanager

from config_manager.readers import ConfigReader
from .readers import ConfigReader
from .schema_validator import SchemaValidator
from .ai_providers import AIProvider, AIConfig, OpenAIConfig, MistralConfig
# Add to imports section
from .feature_manager import (
    FeatureManager,
    ConfigurationIntegration,
    FeatureStatus
)


class Environment(Enum):
    """Environment types"""
    DEVELOPMENT = "development"
    TESTING = "testing"
    STAGING = "staging"
    PRODUCTION = "production"


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

        if self.enable_encryption and self.key_source == "environment" and not self.encryption_key:
            pass

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

    def has_valid_encryption_key(self) -> bool:
        """Check if a valid encryption key is available"""
        if self.encryption_key and len(self.encryption_key.strip()) >= 32:
            return True

        if self.encryption_key_file and os.path.exists(self.encryption_key_file):
            try:
                with open(self.encryption_key_file, 'r') as f:
                    key = f.read().strip()
                    return len(key) >= 32
            except:
                return False

        env_key = os.getenv(self.key_env_var)
        if env_key and len(env_key.strip()) >= 32:
            return True

        return False

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
    delimiter: str = ","
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
        valid_formats = ["csv", "tsv", "json", "jsonl", "parquet", "sql_query", "xlsx", "dsv", "fixed", "fwf"]
        if self.format not in valid_formats:
            print(f"Warning: Invalid format '{self.format}', defaulting to 'csv'")
            self.format = "csv"

        # Create output directory if it doesn't exist
        self.directory = self.directory.format(timestamp=self.timestamp)
        self.report_directory = f'{self.directory}/reports'

        if self.create_directory:
            try:
                if not os.path.exists(self.directory):
                    os.makedirs(self.directory, exist_ok=True)
                if not os.path.exists(self.report_directory):
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



import json
import re
from typing import Dict, Any, Union

class CustomJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles regex patterns properly"""
    
    def default(self, obj):
        if hasattr(obj, '__dict__'):
            return obj.__dict__
        return super().default(obj)
    
    def encode(self, obj):
        """Override encode to handle regex patterns"""
        # First do normal encoding
        json_str = super().encode(obj)
        
        # Then fix double-escaped backslashes in regex patterns
        # This pattern looks for regex-like strings with double backslashes
        # and converts them back to single backslashes
        json_str = self._fix_regex_patterns(json_str)
        
        return json_str
    
    def _fix_regex_patterns(self, json_str: str) -> str:
        """Fix double-escaped backslashes in regex patterns"""
        # Common regex patterns that get double-escaped
        patterns_to_fix = [
            r'\\\\d',  # \\d -> \d
            r'\\\\w',  # \\w -> \w  
            r'\\\\s',  # \\s -> \s
            r'\\\\.',  # \\. -> \.
            r'\\\\+',  # \\+ -> \+
            r'\\\\*',  # \\* -> \*
            r'\\\\?',  # \\? -> \?
            r'\\\\^',  # \\^ -> \^
            r'\\\\$',  # \\$ -> \$
            r'\\\\[',  # \\[ -> \[
            r'\\\\]',  # \\] -> \]
            r'\\\\{',  # \\{ -> \{
            r'\\\\}',  # \\} -> \}
            r'\\\\(',  # \\( -> \(
            r'\\\\)',  # \\) -> \)
            r'\\\\|',  # \\| -> \|
        ]
        
        for pattern in patterns_to_fix:
            # Replace double backslashes with single backslashes for regex patterns
            json_str = json_str.replace(pattern, pattern[2:])  # Remove first two backslashes
        
        return json_str


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

        self.feature_manager = FeatureManager(self.logger)
        self.feature_integration = ConfigurationIntegration(self, self.feature_manager)

    def _load_default_configs(self):
        """Load default configurations using feature management"""

        # Development - minimal features
        dev_config = self._create_environment_config(
            Environment.DEVELOPMENT.value,
            rows=1000,
            enabled_features=['data_masking'],
            ai_config={'openai': {'enabled': False, 'model': 'gpt-3.5-turbo'}}
        )

        # Testing - validation features
        test_config = self._create_environment_config(
            Environment.TESTING.value,
            rows=10000,
            enabled_features=['data_masking', 'quality_analysis', 'business_rules'],
            ai_config={'mistral': {'enabled': False, 'model': 'mistral-small'}}
        )

        # Production - all available features
        prod_config = self._create_environment_config(
            Environment.PRODUCTION.value,
            rows=1000000,
            enabled_features='all',  # Enable all available features
            ai_config={
                'openai': {'enabled': False, 'model': 'gpt-4'},
                'mistral': {'enabled': False, 'model': 'mistral-medium'},
                'enable_fallback': True
            }
        )

        self.environment_configs = {
            Environment.DEVELOPMENT.value: dev_config,
            Environment.TESTING.value: test_config,
            Environment.PRODUCTION.value: prod_config
        }

    def _create_environment_config(self, environment: str, rows: int,
                                   enabled_features: Union[List[str], str],
                                   ai_config: Dict[str, Any] = None) -> GenerationConfig:
        """Create environment configuration using feature management"""

        # Base configuration
        config = GenerationConfig(
            environment=environment,
            rows=rows,
            performance=PerformanceConfig(
                max_workers=2 if environment == 'development' else 8,
                batch_size=5000 if environment == 'development' else 50000,
                max_memory_mb=500 if environment == 'development' else 4000
            ),
            security=SecurityConfig(),
            ai=AIConfig(),
            output=OutputConfig(),
            validation=ValidationConfig(
                enable_business_rules=False,  # Start disabled
                enable_data_quality_analysis=False,  # Start disabled
                enable_anomaly_detection=False  # Start disabled
            ),
            logging=LoggingConfig()
        )

        # Apply AI configuration
        if ai_config:
            if 'openai' in ai_config:
                config.ai.openai = OpenAIConfig(**ai_config['openai'])
            if 'mistral' in ai_config:
                config.ai.mistral = MistralConfig(**ai_config['mistral'])
            if 'enable_fallback' in ai_config:
                config.ai.enable_fallback = ai_config['enable_fallback']

        # Enable features using feature manager
        if enabled_features == 'all':
            self.feature_manager.enable_all_features(config, skip_missing_deps=True)
        elif isinstance(enabled_features, list):
            for feature_name in enabled_features:
                self.feature_manager.enable_feature(feature_name, config, force=False)

        self.logger.debug(f"Created {environment} config with {rows} rows (features will be set via overrides)")

        return config

    # ===================== CONFIGURATION LOADING =====================

    def load_configuration(self, config_path: str,
                           **kwargs) -> 'GenerationConfig':
        """
        Enhanced configuration loading with comprehensive argument override support

        Args:
            config_path: Path to configuration file
            **kwargs: Additional overrides
        """
        config_path = Path(config_path)

        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        # Load raw configuration
        raw_config = self._load_config(config_path)

        # for exporting the excel/csv config to json
        if kwargs.get('save_config', False) and not config_path.suffix in ['.json', '.jsonl']:
            path = os.path.dirname(raw_config['output']['directory'])
            output_file_name = f'generated_config_from_{config_path.suffix.replace('.', '')}.json'
            output_file_path = os.path.join(path, output_file_name)
            self.logger.info(f"Saving new generated config in file: {output_file_path}")
            self._save_json(raw_config, Path(output_file_path))
            exit(0)

        # Apply argument overrides BEFORE parsing
        raw_config = self._apply_argument_overrides(
            raw_config,
            **kwargs
        )

        raw_config = self.schema_validator.validate_schema(raw_config)

        # for exporting the modified JSON (i.e., sql datatypes to python data types)
        if kwargs.get('save_config', False):
            path = os.path.dirname(raw_config['output']['directory'])
            output_file_name = 'generated_config.json'
            output_file_path = os.path.join(path, output_file_name)
            self.logger.info(f"Saving new generated config in file: {output_file_path}")
            self.schema_validator.save_schema_only(output_file_path)
            exit(0)

        # Parse configuration with overrides applied
        config = self._parse_config_dict(raw_config)
        feature_summary = self.feature_manager.get_feature_status_summary(config)
        if feature_summary['missing_dependencies']:
            self.logger.warning("🚨 Some configured features have missing dependencies:")
            for missing in feature_summary['missing_dependencies']:
                self.logger.warning(f"  - {missing['feature']}: missing {', '.join(missing['missing'])}")

        self._validate_configuration(config)

        return config

    def update_output_directory(self, config: GenerationConfig, new_directory: str,
                                cleanup_old: bool = False) -> bool:
        """Safely update output directory in configuration"""
        try:
            old_directory = config.output.directory
            old_report_directory = config.output.report_directory

            if old_report_directory == old_directory:
                self.logger.info(f"No change in the output directory")
                return True

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

    def _apply_argument_overrides(self, raw_config: Dict[str, Any], **overrides) -> Dict[str, Any]:
        """Apply command-line argument overrides using unified feature management"""
        config = raw_config.copy()
        applied_overrides = []

        # Apply basic overrides (rows, output_format, etc.)
        basic_overrides = self._apply_basic_overrides(config, overrides)
        
        temp_config = self._parse_config_dict(config)

        applied_overrides.extend(basic_overrides)

        enable_all = overrides.get('enable_all_features', False)

        feature_overrides = self.feature_integration.apply_feature_overrides(
            temp_config,
            enable_all=enable_all,
            features_to_enable=self._extract_features_to_enable(overrides),
            features_to_disable=self._extract_features_to_disable(overrides) if not enable_all else []
        )

        applied_overrides.extend(feature_overrides)

        self._sync_temp_config_to_raw(temp_config, config)

        ai_overrides = self._apply_ai_configuration_overrides(config, overrides)
        applied_overrides.extend(ai_overrides)

        # Log applied overrides
        if applied_overrides:
            self.logger.info("🔧 Applied command-line overrides:")
            for override in applied_overrides:
                self.logger.info(f"  - {override}")

        return config

    def _extract_features_to_enable(self, overrides: Dict[str, Any]) -> List[str]:
        """Extract feature names to enable from overrides"""
        features_to_enable = []

        feature_mapping = {
            'enable_parallel': 'parallel',
            'enable_streaming': 'streaming',
            'enable_data_masking': 'data_masking',
            'enable_encryption': 'encryption',
            'enable_data_quality_analysis': 'quality_analysis',
            'enable_anomaly_detection': 'anomaly_detection',
            'enable_business_rules': 'business_rules',
            'enable_audit_logging': 'audit_logging',
            'openai_enabled': 'openai',
            'mistral_enabled': 'mistral'
        }

        for override_key, feature_name in feature_mapping.items():
            if overrides.get(override_key) is True:
                features_to_enable.append(feature_name)

        # Handle ai_enabled special case
        if overrides.get('ai_enabled') is True:
            features_to_enable.extend(['openai', 'mistral'])

        return features_to_enable

    def _extract_features_to_disable(self, overrides: Dict[str, Any]) -> List[str]:
        """Extract feature names to disable from overrides"""
        features_to_disable = []

        feature_mapping = {
            'enable_data_masking': 'data_masking',
            'enable_encryption': 'encryption',
            'enable_data_quality_analysis': 'quality_analysis',
            'enable_anomaly_detection': 'anomaly_detection',
            'enable_business_rules': 'business_rules',
            'enable_audit_logging': 'audit_logging',
            'openai_enabled': 'openai',
            'mistral_enabled': 'mistral'
        }

        for override_key, feature_name in feature_mapping.items():
            if overrides.get(override_key) is False:
                features_to_disable.append(feature_name)

        # Handle ai_enabled special case
        if overrides.get('ai_enabled') is False:
            features_to_disable.extend(['openai', 'mistral'])

        return features_to_disable

    def _check_database_config(self, config: Any) -> bool:
        """Check if database configuration is complete"""
        db_config = getattr(config, 'database', None)
        if not db_config:
            return False

        required_fields = ['host', 'port', 'database', 'username']
        return all(getattr(db_config, field, None) for field in required_fields)

    def _check_compression_support(self, config: Any) -> bool:
        """Check if compression libraries are available"""
        try:
            import gzip
            import bz2
            import lzma
            return True
        except ImportError:
            return False

    def _apply_basic_overrides(self, config: Dict[str, Any], overrides: Dict[str, Any]) -> List[str]:
        """Apply basic non-feature overrides"""
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

        # Output directory override
        if overrides.get('output_dir') is not None:
            if 'output' not in config:
                config['output'] = {}
            old_value = config['output'].get('directory', 'not set')
            config['output']['directory'] = overrides['output_dir']
            config['output']['report_directory'] = os.path.join(overrides['output_dir'], 'reports')
            applied_overrides.append(f"output.directory: {old_value} → {overrides['output_dir']}")

        return applied_overrides

    def _sync_temp_config_to_raw(self, temp_config: Any, raw_config: Dict[str, Any]):
        """Sync changes from temp_config back to raw_config"""
        # Sync performance settings
        required_sections = ['security', 'validation', 'ai']
        for section in required_sections:
            if section not in raw_config:
                raw_config[section] = {}

        # Sync security settings
        if hasattr(temp_config, 'security'):
            if 'security' not in raw_config:
                raw_config['security'] = {}
            raw_config['security']['enable_data_masking'] = temp_config.security.enable_data_masking
            raw_config['security']['enable_encryption'] = temp_config.security.enable_encryption
            raw_config['security']['audit_enabled'] = temp_config.security.audit_enabled

        # Sync validation settings
        if hasattr(temp_config, 'validation'):
            if 'validation' not in raw_config:
                raw_config['validation'] = {}
            raw_config['validation']['enable_business_rules'] = temp_config.validation.enable_business_rules
            raw_config['validation'][
                'enable_data_quality_analysis'] = temp_config.validation.enable_data_quality_analysis
            raw_config['validation']['enable_anomaly_detection'] = temp_config.validation.enable_anomaly_detection

        # Sync AI settings
        if hasattr(temp_config, 'ai'):
            if 'ai' not in raw_config:
                raw_config['ai'] = {}

            if hasattr(temp_config.ai, 'openai') and temp_config.ai.openai:
                if 'openai' not in raw_config['ai']:
                    raw_config['ai']['openai'] = {}
                raw_config['ai']['openai']['enabled'] = temp_config.ai.openai.enabled

            if hasattr(temp_config.ai, 'mistral') and temp_config.ai.mistral:
                if 'mistral' not in raw_config['ai']:
                    raw_config['ai']['mistral'] = {}
                raw_config['ai']['mistral']['enabled'] = temp_config.ai.mistral.enabled

    def _apply_ai_configuration_overrides(self, config: Dict[str, Any], overrides: Dict[str, Any]) -> List[str]:
        """Apply AI-specific configuration overrides (not feature enables, but model/settings changes)"""
        applied_overrides = []

        # Initialize AI config if needed
        if 'ai' not in config:
            config['ai'] = {}

        # AI primary provider override
        if overrides.get('ai_primary_provider') is not None:
            from .ai_providers import AIProvider  # Import here to avoid circular imports
            old_value = config['ai'].get('primary_provider', 'not set')
            new_provider = overrides['ai_primary_provider']

            valid_providers = [provider.value for provider in AIProvider]
            if new_provider in valid_providers:
                config['ai']['primary_provider'] = new_provider
                applied_overrides.append(f"ai.primary_provider: {old_value} → {new_provider}")
            else:
                applied_overrides.append(f"ai.primary_provider: INVALID '{new_provider}' (valid: {valid_providers})")
                self.logger.warning(f"Invalid AI provider '{new_provider}'. Valid options: {valid_providers}")

        # Model overrides (these are configuration changes, not feature enables)
        if overrides.get('openai_model') is not None:
            if 'openai' not in config['ai']:
                config['ai']['openai'] = {}
            old_value = config['ai']['openai'].get('model', 'not set')
            config['ai']['openai']['model'] = overrides['openai_model']
            applied_overrides.append(f"ai.openai.model: {old_value} → {overrides['openai_model']}")

        if overrides.get('mistral_model') is not None:
            if 'mistral' not in config['ai']:
                config['ai']['mistral'] = {}
            old_value = config['ai']['mistral'].get('model', 'not set')
            config['ai']['mistral']['model'] = overrides['mistral_model']
            applied_overrides.append(f"ai.mistral.model: {old_value} → {overrides['mistral_model']}")

        return applied_overrides

    def get_feature_status(self, config: 'GenerationConfig') -> Dict[str, Any]:
        """Get current feature status using unified management"""
        return self.feature_manager.get_feature_status_summary(config)

    def _validate_configuration(self, config: 'GenerationConfig'):
        """
        Comprehensive validation including capacity checks
        """
        self.logger.info("🔍 Starting comprehensive configuration validation...")

        errors = []
        warnings = []

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

        feature_summary = self.get_feature_status(config)
        # Validate enabled features
        for feature_name in feature_summary['enabled']:
            feature_details = self.get_feature_details(feature_name, config)
            if feature_details.get('dependencies_missing'):
                warnings.append(
                    f"Feature '{feature_name}' enabled but missing: {', '.join(feature_details['dependencies_missing'])}")

        # Check for conflicting features
        conflicts = self._check_feature_conflicts(config)
        if conflicts:
            warnings.extend(conflicts)

        # Check feature recommendations
        recommendations = self._get_feature_recommendations(config)
        if recommendations:
            for rec in recommendations:
                self.logger.info(f"💡 Recommendation: {rec}")

        if errors:
            self._log_validation_errors("Configuration", errors)
            raise ValueError(f"Configuration validation failed:\n" + "\n".join(f"  - {error}" for error in errors))

        if warnings:
            self.logger.warning("⚠️ Configuration warnings:")
            for warning in warnings:
                self.logger.warning(f"  - {warning}")

        self.logger.info("✅ Comprehensive configuration validation completed")

    def _check_feature_conflicts(self, config: 'GenerationConfig') -> List[str]:
        """Check for conflicting feature combinations"""
        conflicts = []
        encryption_details = self.get_feature_details('encryption', config)
        streaming_details = self.get_feature_details('streaming', config)

        if encryption_details.get('enabled') and streaming_details.get('enabled'):
            conflicts.append("Encryption + Streaming may impact performance significantly")

        # Check if multiple AI providers are enabled but no fallback
        ai_features = [provider.value for provider in AIProvider.all()]
        enabled_ai = []

        for ai_feature in ai_features:
            ai_details = self.get_feature_details(ai_feature, config)
            if ai_details.get('enabled'):
                enabled_ai.append(ai_feature)

        if len(enabled_ai) > 1 and not config.ai.enable_fallback:
            conflicts.append(f"Multiple AI providers enabled ({', '.join(enabled_ai)}) but fallback disabled")

        return conflicts

    def _get_feature_recommendations(self, config: 'GenerationConfig') -> List[str]:
        """Get feature recommendations based on current configuration"""
        recommendations = []

        # USE get_feature_details instead of direct access
        if config.rows > 100000:
            parallel_details = self.get_feature_details('parallel', config)
            if parallel_details.get('status') == 'available' and not parallel_details.get('enabled'):
                recommendations.append("Enable parallel processing for better performance with large datasets")

            # Check environment-specific recommendations
        if config.environment == Environment.PRODUCTION.value:
            masking_details = self.get_feature_details('data_masking', config)
            if not masking_details.get('enabled'):
                recommendations.append("Consider enabling data masking for production environment")

        if config.environment == Environment.TESTING.value:
            quality_details = self.get_feature_details('quality_analysis', config)
            if not quality_details.get('enabled'):
                recommendations.append("Enable quality analysis for comprehensive testing")

        return recommendations

    def _log_feature_validation_summary(self, feature_summary: Dict[str, Any]):
        """Log feature validation summary"""
        self.logger.info("📊 Feature Status Summary:")
        self.logger.info(f"   ✅ Enabled: {len(feature_summary['enabled'])} features")
        self.logger.info(f"   🟡 Available: {len(feature_summary['available'])} features")
        self.logger.info(f"   ❌ Missing deps: {len(feature_summary['missing_dependencies'])} features")

        if feature_summary['enabled']:
            self.logger.info(f"   🚀 Active: {', '.join(feature_summary['enabled'])}")

        if feature_summary['missing_dependencies']:
            self.logger.info("   ⏭️ Skipped features:")
            for missing in feature_summary['missing_dependencies']:
                deps = ', '.join(missing['missing'])
                self.logger.info(f"      - {missing['feature']}: missing {deps}")

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

    def _load_config(self, config_path: Path, args: dict = None) -> Union[dict, None]:
        """Load JSON configuration file"""
        try:
            config_reader = ConfigReader(config_path)
            config = config_reader.read_config()
            return config
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
                    'enable_data_masking', 'enable_encryption', 'audit_enabled',
                    'masking_rules', 'sensitivity', 'sensitivity_map', 'encrypt_fields',
                    'auto_detect_pii', 'strict_mode', 'compliance_profile'
                }
                filtered_security = {k: v for k, v in security_data.items() if k in valid_params}
                nested_configs['security'] = SecurityConfig(**filtered_security)
            else:
                nested_configs['security'] = SecurityConfig()

        # Enhanced AI configuration parsing
        if 'ai' in config_dict:
            nested_configs['ai'] = self._parse_ai_config(config_dict.pop('ai'))

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
            self.logger.error(f"❌ Configuration parsing error: {e}")
            self.logger.error(f"Config dict keys: {list(config_dict.keys())}")
            self.logger.error(f"Nested config keys: {list(nested_configs.keys())}")
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
        ai_config = AIConfig(
            openai=openai_config,
            mistral=mistral_config,
            **ai_config_data
        )

        temp_config = type('TempConfig', (), {'ai': ai_config})()

        # Check OpenAI
        if openai_config.enabled:
            result = self.feature_manager.check_feature_dependencies('openai', temp_config)
            if result.status == FeatureStatus.MISSING_DEPENDENCY:
                openai_config.enabled = False
                self.logger.error(f"❌ OpenAI disabled: {', '.join(result.dependencies_missing)}")

        # Check Mistral
        if mistral_config.enabled:
            result = self.feature_manager.check_feature_dependencies('mistral', temp_config)
            if result.status == FeatureStatus.MISSING_DEPENDENCY:
                mistral_config.enabled = False
                self.logger.error(f"❌ Mistral disabled: {', '.join(result.dependencies_missing)}")

        return ai_config

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

    def _save_json(self, config_dict: Dict[str, Any], output_path: Path):
        """Save configuration as JSON"""
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(config_dict, f, indent=2, ensure_ascii=False, cls=CustomJSONEncoder)

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

    def get_config_summary(self, config: GenerationConfig) -> Dict[str, Any]:
        """Get comprehensive summary using existing feature management methods"""

        # USE EXISTING get_feature_status method
        feature_summary = self.get_feature_status(config)

        summary = {
            'environment': config.environment,
            'total_tables': len(config.tables),
            'total_rows': config.rows,
            'output_format': config.output.format,
        }

        # USE get_feature_details for specific feature status
        performance_features = ['streaming', 'parallel']
        summary['performance'] = {
            'max_workers': config.performance.max_workers,
            'batch_size': config.performance.batch_size,
        }

        for feature in performance_features:
            feature_details = self.get_feature_details(feature, config)
            summary['performance'][f'{feature}_enabled'] = feature_details.get('enabled', False)

        # USE get_feature_details for security features
        security_features = ['data_masking', 'encryption', 'audit_logging']
        summary['security'] = {}

        for feature in security_features:
            feature_details = self.get_feature_details(feature, config)
            summary['security'][f'{feature.replace("_", "")}_enabled'] = feature_details.get('enabled', False)

        # USE get_feature_details for validation features
        validation_features = ['quality_analysis', 'business_rules', 'anomaly_detection']
        summary['validation'] = {
            'strict_mode': config.validation.strict_mode,
        }

        for feature in validation_features:
            feature_details = self.get_feature_details(feature, config)
            summary['validation'][feature] = feature_details.get('enabled', False)

        # USE EXISTING feature_summary data
        summary['features'] = {
            'total': feature_summary['total_features'],
            'enabled': len(feature_summary['enabled']),
            'enabled_features': feature_summary['enabled'],
            'available': len(feature_summary['available']),
            'missing_dependencies': len(feature_summary['missing_dependencies']),
            'by_category': feature_summary['by_category']
        }

        # AI summary using existing methods
        ai_features = ['openai', 'mistral']
        ai_enabled_features = []

        for ai_feature in ai_features:
            ai_details = self.get_feature_details(ai_feature, config)
            if ai_details.get('enabled'):
                ai_enabled_features.append(ai_feature)

        summary['ai'] = {
            'enabled': len(ai_enabled_features) > 0,
            'active_providers': ai_enabled_features,
            'primary_provider': config.ai.primary_provider if config.ai else None,
            'fallback_enabled': config.ai.enable_fallback if config.ai else False
        }

        # Detailed AI config using existing methods
        for ai_feature in ai_features:
            ai_details = self.get_feature_details(ai_feature, config)
            ai_config = getattr(config.ai, ai_feature, None) if config.ai else None

            if ai_config:
                summary['ai'][ai_feature] = {
                    'enabled': ai_details.get('enabled', False),
                    'model': ai_config.model if ai_details.get('enabled') else None,
                    'api_key_available': ai_config.is_available() if hasattr(ai_config, 'is_available') else False
                }

        return summary

    def get_feature_details(self, feature_name: str, config: GenerationConfig) -> Dict[str, Any]:
        """Get detailed information about a specific feature"""
        if feature_name not in self.feature_manager.features:
            return {'error': f'Unknown feature: {feature_name}'}

        result = self.feature_manager.check_feature_dependencies(feature_name, config)
        feature = self.feature_manager.features[feature_name]

        return {
            'name': feature_name,
            'enabled': result.enabled,
            'status': result.status.value,
            'dependencies_met': result.dependencies_met,
            'dependencies_missing': result.dependencies_missing,
            'dependencies': [
                {
                    'name': dep.name,
                    'type': dep.type.value,
                    'required': dep.required,
                    'error_message': dep.error_message,
                    'install_hint': dep.install_hint
                }
                for dep in feature.dependencies
            ],
            'warnings': result.warnings,
            'suggestions': result.suggestions
        }
