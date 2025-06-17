# Detailed Code Flow Analysis - Faker Rules Processing

## Table of Contents
1. [Entry Points and Initialization](#entry-points-and-initialization)
2. [Rule Processing Pipeline](#rule-processing-pipeline)
3. [Value Generation Flow](#value-generation-flow)
4. [Constraint Management](#constraint-management)
5. [Integration Points](#integration-points)
6. [Error Handling and Fallbacks](#error-handling-and-fallbacks)
7. [Performance Optimization](#performance-optimization)
8. [Configuration Management](#configuration-management)

---

## Entry Points and Initialization

### 1. Main Execution Flow (`main.py`)

```python
# Entry point
if __name__ == "__main__":
    # Parse command line arguments
    args = parse_arguments()
    
    # Load configuration
    config_manager = ConfigurationManager()
    config = config_manager.load_configuration(args.config, args.environment)
    
    # Apply command line overrides
    apply_command_line_overrides(config, args)
    
    # Setup logging
    logger = setup_logging(config)
    
    # Run data generation
    generated_data = main(config=config, total_records=config.rows, output_dir=output_directory)
```

### 2. Engine Initialization (`generate_data_parallely.py`)

```python
class OptimizedDataGenerationEngine:
    def __init__(self, config: GenerationConfig, logger: logging.Logger):
        # Initialize core components
        self.constraint_manager = OptimizedConstraintManager(...)
        self.validator = UnifiedValidator(logger=logger)
        
        # Create the main DataGenerator instance
        self.data_generator = DataGenerator(config, config.locale, logger=logger)
        
        # Initialize the enhanced parallel data generator
        self.parallel_generator = ParallelDataGenerator(
            data_generator_instance=self.data_generator,
            max_workers=config.performance.max_workers,
            max_memory_mb=config.performance.max_memory_mb,
            enable_streaming=config.performance.enable_streaming,
            logger=logger
        )
```

---

## Rule Processing Pipeline

### 1. Configuration Loading (`json_reader.py`)

```python
class JSONConfigReader:
    def load_config(self):
        with open(self.file_name) as json_file:
            config = json.load(json_file)
            
            # Process file-based choices
            self._process_file_choices(config)
            
            # Convert legacy formats if needed
            if not self._is_valid_schema_format(config):
                json_converter = JSONTemplateConverter()
                config = json_converter.convert(config)
                
            return config
    
    def _process_file_choices(self, config: Dict[str, Any]):
        """Process choice_from_file rules"""
        for table in config['tables']:
            for column in table['columns']:
                rule = column.get('rule')
                if isinstance(rule, dict) and rule.get('type') == 'choice_from_file':
                    # Load choices from file
                    file_rule = self.choice_loader.load_choices_from_file(
                        file_path=rule['file_path'],
                        column=rule.get('column'),
                        weight_column=rule.get('weight_column')
                    )
                    # Replace the rule with loaded choices
                    column['rule'] = file_rule
```

### 2. Schema Validation (`schema_validator.py`)

```python
class SchemaValidator:
    def _validate_type_rule_consistency(self, column, table_name, col_name, table_index, col_index):
        """Auto-convert string types to appropriate numeric types"""
        col_type = column.get('type', '').lower()
        rule = column.get('rule')
        
        if isinstance(rule, dict):
            rule_type = rule.get('type', '').lower()
            
            # Auto-conversion logic
            if col_type in {'str', 'string', 'text'} and rule_type == 'range':
                min_val, max_val = rule.get('min'), rule.get('max')
                if isinstance(min_val, int) and isinstance(max_val, int):
                    # Convert str to int for numeric range rules
                    self._apply_correction(table_index, col_index, 'type', 'str', 'int', 
                                         f"auto_converted_range_to_int")
```

---

## Value Generation Flow

### 1. Main Generation Entry Point (`data_generator.py`)

```python
class ValueGenerator:
    def generate_by_rule(self, rule: Any, data_type: str, column_name: str = "generated_column") -> Any:
        """Main entry point for value generation"""
        
        # String rule processing
        if isinstance(rule, str):
            result = self._generate_from_string_rule(rule)
            if result is not None:
                return result
                
        # Dictionary rule processing
        elif isinstance(rule, dict):
            # Check for OpenAI generation
            if rule.get("type") == "openai_generated":
                return self._generate_with_openai_rule(rule, data_type, column_name)
            else:
                result = self._generate_from_dict_rule(rule, data_type)
                if result is not None:
                    return result
        
        # Default generation by type
        result = self._generate_default_by_type(data_type)
        if result is not None:
            return result
        
        # Final fallback to OpenAI if enabled
        if self.openai_enabled and self.openai_config.fallback_enabled:
            return self._generate_with_openai_fallback(rule, data_type, column_name)
        
        # Ultimate fallback
        return self._ultimate_fallback(data_type)
```

### 2. String Rule Processing

```python
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
```

### 3. Dictionary Rule Processing

```python
def _generate_from_dict_rule(self, rule: dict, data_type: str) -> Any:
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
        value = self.generate_by_rule(rule_type, data_type) if rule_type else None
    
    # Apply prefix/suffix if specified
    if 'prefix' in rule or 'suffix' in rule:
        value = self._apply_prefix_suffix(value, rule)
    
    # Apply regex validation if specified
    if rule_type == 'regex':
        value = self._apply_regex_validation(value, rule)
        
    return value
```

### 4. Specific Rule Implementations

```python
def _generate_choice(self, rule: dict) -> Any:
    """Generate value from choices with optional probabilities"""
    choices = rule.get("value", [])
    probabilities = rule.get("probabilities", {})
    
    if probabilities:
        weights = [probabilities.get(choice, 1.0) for choice in choices]
        return random.choices(choices, weights=weights, k=1)[0]
    return random.choice(choices)

def _generate_numeric_range(self, rule: dict, data_type: str) -> Any:
    """Generate numeric value within range"""
    min_val = rule.get("min", 0)
    max_val = rule.get("max", 10000)
    
    if data_type in ["int", "str"]:
        return random.randint(int(min_val), int(max_val))
    elif data_type in ["float", "str"]:
        return round(random.uniform(min_val, max_val), 2)
    return min_val

def _generate_date_range(self, rule: dict) -> Any:
    """Generate date within range"""
    start_date = rule.get("start", "1950-01-01")
    end_date = rule.get("end", datetime.now().strftime("%Y-%m-%d"))
    
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    delta = end - start
    return (start + timedelta(days=random.randint(0, delta.days))).date()
```

---

## Constraint Management

### 1. Batch Generation with Constraints (`data_generator.py`)

```python
class BatchGenerator:
    def generate_batch(self, table_metadata: Dict, batch_size: int, 
                      generated_data: Dict[str, pd.DataFrame],
                      foreign_key_data: Dict = None) -> List[Dict]:
        """Generate a batch of records with constraint enforcement"""
        
        # Get current global record count for this table
        table_name = table_metadata["table_name"]
        current_count = self.table_record_counts.get(table_name, 0)
        
        # Prepare FK pools
        fk_pools, fk_distributions = self._prepare_fk_pools(...)
        
        # Generate records with global indexing
        batch_data = []
        for record_idx in range(batch_size):
            global_record_idx = current_count + record_idx
            
            row = self._generate_single_record(
                global_record_idx, table_metadata, fk_pools, fk_distributions
            )
            batch_data.append(row)
        
        # Update global record count
        self.table_record_counts[table_name] = current_count + batch_size
        
        # Apply batch-level constraints
        return self.constraint_manager.apply_length_constraints_to_batch(batch_data, table_metadata)
```

### 2. Unique Value Generation (`optimized_constraint_manager.py`)

```python
class OptimizedConstraintManager:
    def generate_unique_pk_value(self, table_name: str, column_name: str,
                                 value_generator_func, max_attempts: int = 100) -> Any:
        """Generate a unique primary key value with optimized collision detection"""
        constraint_key = f"{table_name}.{column_name}"
        
        # Get existing values for efficient collision detection
        existing_values = self._pk_cache.get(constraint_key, set())
        
        for attempt in range(max_attempts):
            value = value_generator_func()
            if value not in existing_values:
                self.add_pk_value(table_name, column_name, value)
                return value
        
        # Fallback with timestamp for guaranteed uniqueness
        import time
        base_value = str(value_generator_func()) if value_generator_func else "pk"
        unique_value = f"{base_value}_{int(time.time() * 1000000) % 1000000}"
        self.add_pk_value(table_name, column_name, unique_value)
        return unique_value
```

### 3. Foreign Key Management

```python
def handle_one_to_many_relationship(self, fk_config: Dict, available_values: List[Any],
                                   record_count: int) -> List[Any]:
    """Handle one-to-many relationship with optimized distribution"""
    distribution_type = fk_config.get("distribution", "random")
    
    if distribution_type == "even":
        return self._distribute_evenly_optimized(available_values, record_count)
    elif distribution_type == "weighted":
        weights = fk_config.get("weights", {})
        return self._distribute_weighted_optimized(available_values, record_count, weights)
    else:
        return self._distribute_randomly_optimized(available_values, record_count, fk_config)
```

---

## Integration Points

### 1. OpenAI Integration (`data_generator.py`)

```python
def _generate_with_openai_rule(self, rule: Dict[str, Any], data_type: str, column_name: str) -> Any:
    """Generate value using OpenAI with specific rule configuration"""
    if not self.openai_enabled:
        return self._ultimate_fallback(data_type)
    
    # Check cost limit
    if self._check_cost_limit():
        return self._ultimate_fallback(data_type)
    
    # Create cache key based on rule
    cache_key = self._create_cache_key_for_rule(rule, data_type, column_name)
    
    # Check if we have cached data
    if cache_key in self._openai_cache:
        self._openai_cache_hits += 1
        return self._get_cached_value(cache_key)
    
    # Generate new batch of data via OpenAI
    self._generate_and_cache_openai_data_from_rule(rule, data_type, column_name, cache_key)
    
    return self._get_cached_value(cache_key)
```

### 2. Validation Integration (`unified_validation_system.py`)

```python
class UnifiedValidator:
    def validate_value(self, value: Any, rule: Union[str, Dict[str, Any]], 
                      data_type: str = None, **kwargs) -> Tuple[bool, str]:
        """Main validation method - replaces all duplicate validation code"""
        self._validation_stats['total_validations'] += 1
        
        try:
            if value is None:
                return True, "Value is None"
            
            # Handle string rules
            if isinstance(rule, str):
                return self._validate_string_rule(value, rule)
            
            # Handle dictionary rules
            elif isinstance(rule, dict):
                return self._validate_dict_rule(value, rule, **kwargs)
            
            return True, "No validation rule applied"
            
        except Exception as e:
            self._validation_stats['validation_errors'] += 1
            return False, f"Validation error: {str(e)}"
```

### 3. Security Integration (`streaming_data_generator.py`)

```python
def _apply_comprehensive_security_measures(self, data: List[Dict], table_metadata: Dict[str, Any]) -> List[Dict]:
    """Apply comprehensive security measures"""
    if not self.config.security.enable_data_masking:
        return data
    
    # Build sensitivity map from column metadata
    sensitivity_map = {}
    sensitive_columns = []
    
    for column in table_metadata.get('columns', []):
        sensitivity_level = column.get('sensitivity', 'PUBLIC')
        sensitivity_map[column['name']] = sensitivity_level
        
        if sensitivity_level in ['PII', 'SENSITIVE']:
            sensitive_columns.append(column['name'])
    
    # Apply masking
    masked_data = self.security_manager.mask_sensitive_data(data, sensitivity_map)
    
    # Apply encryption if enabled
    if self.security_manager.encryption_key and sensitive_columns:
        encrypted_data = self.security_manager.encrypt_sensitive_fields(masked_data, sensitive_columns)
        self.generation_stats['security_operations'] += len(encrypted_data) * len(sensitive_columns)
    else:
        encrypted_data = masked_data
    
    return encrypted_data
```

---

## Error Handling and Fallbacks

### 1. Generation Fallbacks (`data_generator.py`)

```python
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
        "uuid": lambda: self.faker.uuid4(),
        "email": lambda: self.faker.email(),
        "phone": lambda: self.faker.phone_number(),
    }

    generator = fallbacks.get(data_type.lower(), lambda: f"default_value_{random.randint(1, 9999)}")
    return generator()
```

### 2. Validation Error Recovery (`validator.py`)

```python
def _apply_regex_validation(self, value: Any, rule: dict) -> Any:
    """Apply regex validation with retry logic"""
    rule_regex = rule.get("regex")
    if not rule_regex or value is None:
        return value
        
    validator = DataValidator()
    max_attempts = 10
    attempts = 0

    while not validator.regex_validator(rule_regex, str(value)) and attempts < max_attempts:
        rule_type = rule.get("type")
        if rule_type == "email":
            base_value = self.faker.email()
        elif rule_type == "phone_number":
            base_value = self._generate_phone_matching_regex(rule_regex)
        else:
            base_value = self.generate_by_rule(rule_type, "str")

        # Reapply prefix/suffix after regenerating base value
        value = self._apply_prefix_suffix(base_value, rule)
        attempts += 1

    return value
```

### 3. Constraint Violation Handling (`constraint_manager.py`)

```python
def evaluate_conditions(self, conditions: List[Dict], row_data: Dict[str, Any]) -> bool:
    """Evaluate conditions with error handling"""
    if not conditions:
        return True

    try:
        for condition in conditions:
            if not self._evaluate_single_condition(condition, row_data):
                return False
        return True

    except Exception as e:
        self.logger.warning(f"Error evaluating conditions {conditions}: {e}")
        return False  # Fail gracefully on error
```

---

## Performance Optimization

### 1. Memory Management (`streaming_data_generator.py`)

```python
class MemoryMonitor:
    def __init__(self, max_memory_mb: int = 1000, check_interval: float = 1.0):
        self.max_memory_mb = max_memory_mb
        self.check_interval = check_interval
        self._monitoring = False
        self._callbacks = []

    def _monitor_loop(self):
        """Main monitoring loop"""
        while self._monitoring:
            try:
                process = psutil.Process()
                self.current_memory_mb = process.memory_info().rss / 1024 / 1024

                if self.current_memory_mb > self.max_memory_mb:
                    for callback in self._callbacks:
                        try:
                            callback()
                        except Exception as e:
                            logging.error(f"Error in memory cleanup callback: {e}")

                time.sleep(self.check_interval)
            except Exception as e:
                logging.error(f"Error in memory monitoring: {e}")
                time.sleep(self.check_interval)
```

### 2. Caching Strategies (`optimized_constraint_manager.py`)

```python
class LRUConstraintCache:
    def __init__(self, maxsize: int = 10000):
        self.maxsize = maxsize
        self.cache = {}
        self.access_order = []
        self._lock = threading.RLock()

    def get(self, key: str, default=None):
        """Get value from cache with LRU tracking"""
        with self._lock:
            if key in self.cache:
                # Move to end (most recently used)
                self.access_order.remove(key)
                self.access_order.append(key)
                return self.cache[key]
            return default

    def set(self, key: str, value: Any) -> None:
        """Set value in cache with LRU eviction"""
        with self._lock:
            if key in self.cache:
                # Update existing
                self.cache[key] = value
                self.access_order.remove(key)
                self.access_order.append(key)
            else:
                # Add new
                if len(self.cache) >= self.maxsize:
                    # Evict least recently used
                    oldest = self.access_order.pop(0)
                    del self.cache[oldest]

                self.cache[key] = value
                self.access_order.append(key)
```

### 3. Parallel Processing Optimization (`streaming_data_generator.py`)

```python
def _execute_thread_parallel(self, tasks: List[GenerationTask]) -> List[GenerationResult]:
    """Execute tasks using thread pool with shared DataGenerator"""
    futures = []

    for task in tasks:
        future = self.thread_pool.submit(self._execute_generation_task_with_datagenerator, task)
        futures.append(future)

    results = []
    for future in as_completed(futures):
        try:
            result = future.result(timeout=300)  # 5 minute timeout
            results.append(result)
        except Exception as e:
            self.logger.error(f"Thread task failed: {e}")
            results.append(GenerationResult(
                task_id="unknown", data=[], generation_time=0, error=str(e)
            ))

    return results
```

---

## Configuration Management

### 1. Environment-Specific Configuration (`config_manager.py`)

```python
class ConfigurationManager:
    def _apply_environment_overrides(self, config: GenerationConfig, environment: str) -> GenerationConfig:
        """Apply environment-specific configuration overrides"""
        if environment not in self.environment_configs:
            self.logger.warning(f"Unknown environment: {environment}")
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

        return config
```

### 2. Dynamic Configuration Loading (`json_reader.py`)

```python
class ChoiceFileLoader:
    def load_choices_from_file(self, file_path: str, column: str = None,
                               weight_column: str = None, **kwargs) -> Dict[str, Any]:
        """Load choices from CSV/Excel file with caching"""
        try:
            full_path = Path(file_path)

            # Check cache first
            cache_key = f"{full_path}:{column}:{weight_column}"
            if cache_key in self.loaded_files:
                return self.loaded_files[cache_key]

            # Load file based on extension
            if full_path.suffix.lower() in ['.xlsx', '.xls']:
                df = pd.read_excel(full_path, **kwargs)
            elif full_path.suffix.lower() == '.csv':
                df = pd.read_csv(full_path, **kwargs)
            else:
                raise ValueError(f"Unsupported file format: {full_path.suffix}")

            # Use first column if none specified
            if column is None:
                column = df.columns[0]

            # Get unique values (remove duplicates and nulls)
            choices = df[column].dropna().unique().tolist()

            # Build choice rule
            choice_rule = {
                "type": "choice",
                "value": choices,
                "source_file": str(file_path),
                "source_column": column
            }

            # Add probabilities if weight column specified
            if weight_column and weight_column in df.columns:
                weight_df = df.groupby(column)[weight_column].sum().reset_index()
                total_weight = weight_df[weight_column].sum()

                probabilities = {}
                for _, row in weight_df.iterrows():
                    probabilities[row[column]] = row[weight_column] / total_weight

                choice_rule["probabilities"] = probabilities
                choice_rule["source_weight_column"] = weight_column

            # Cache the result
            self.loaded_files[cache_key] = choice_rule

            return choice_rule

        except Exception as e:
            raise ValueError(f"Error loading choices from {file_path}: {str(e)}")
```

### 3. OpenAI Configuration Management (`config_manager.py`)

```python
def test_openai_connection(self, config: GenerationConfig) -> Dict[str, Any]:
    """Test OpenAI connection and return status"""
    if not config.openai.enabled:
        return {
            'status': 'disabled',
            'message': 'OpenAI integration is disabled in configuration'
        }

    api_key = config.openai.get_api_key()
    if not api_key:
        return {
            'status': 'error',
            'message': 'No API key found'
        }

    try:
        import openai
        
        # Set API key and test connection
        openai.api_key = api_key
        
        response = openai.ChatCompletion.create(
            model=config.openai.model,
            messages=[{"role": "user", "content": "Test connection"}],
            max_tokens=10,
            timeout=config.openai.timeout_seconds
        )

        return {
            'status': 'success',
            'message': 'OpenAI connection successful',
            'model': config.openai.model,
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
```

---

## Data Flow Summary

### Complete Generation Pipeline

```
1. Configuration Loading
   ├── Load JSON configuration file
   ├── Process file-based choice rules
   ├── Convert legacy formats
   ├── Validate schema
   └── Apply environment overrides

2. Rule Processing
   ├── Parse string rules (faker methods)
   ├── Parse dictionary rules (complex rules)
   ├── Handle prefix/suffix rules
   ├── Process OpenAI generation rules
   └── Validate rule consistency

3. Value Generation
   ├── Route to appropriate generator
   ├── Apply constraints (unique, FK, etc.)
   ├── Validate generated values
   ├── Apply prefix/suffix formatting
   └── Cache results for performance

4. Batch Processing
   ├── Generate batches in parallel
   ├── Apply length constraints
   ├── Manage memory usage
   ├── Handle foreign key relationships
   └── Validate batch integrity

5. Output Generation
   ├── Apply security measures (masking/encryption)
   ├── Stream to appropriate format
   ├── Generate performance reports
   ├── Create audit trails
   └── Clean up resources
```

This comprehensive analysis demonstrates how the faker rules are processed throughout the entire system, from initial configuration loading through final output generation, with robust error handling, performance optimization, and security measures at every step.