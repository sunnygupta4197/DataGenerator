# Developer Guide - Enhanced Data Generator Framework

## 🎯 Technical Overview

The Enhanced Data Generator is a sophisticated Python framework designed for enterprise-scale synthetic data generation. Built with modern software engineering principles, it provides thread-safe, memory-efficient, and highly configurable data generation capabilities.

## 🏗️ Architecture Deep Dive

### Core Design Patterns

#### 1. **Factory Pattern** - Writer Creation
```python
# WriterFactory creates appropriate writers based on format
writer = WriterFactory.create_writer('output.csv', format_type='csv')
writer = WriterFactory.create_writer('output.parquet', compression='snappy')
```

#### 2. **Strategy Pattern** - Generation Strategies
```python
# ParallelDataGenerator selects optimal strategy
if estimated_memory_mb <= available_memory_mb * 0.5:
    strategy = "parallel"
elif estimated_memory_mb <= available_memory_mb * 0.8:
    strategy = "streaming_parallel"
else:
    strategy = "pure_streaming"
```

#### 3. **Observer Pattern** - Performance Monitoring
```python
# MemoryMonitor with callback system
memory_monitor.add_cleanup_callback(self._cleanup_memory)
memory_monitor.start_monitoring()
```

#### 4. **Template Method Pattern** - Validation Pipeline
```python
# UnifiedValidator with extensible validation pipeline
def validate_value(self, value, rule, data_type):
    if isinstance(rule, str):
        return self._validate_string_rule(value, rule)
    elif isinstance(rule, dict):
        return self._validate_dict_rule(value, rule)
```

### Component Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Application Layer                        │
├─────────────────────────────────────────────────────────────┤
│  main.py - CLI & Orchestration           │
│  OptimizedDataGenerationOrchestrator                        │
└─────────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────────┐
│                     Service Layer                           │
├─────────────────────────────────────────────────────────────┤
│  ConfigurationManager    │  DataQualityAnalyzer             │
│  SecurityManager         │  PerformanceProfiler             │
└─────────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────────┐
│                      Core Layer                             │
├─────────────────────────────────────────────────────────────┤
│  DataGenerator           │  ParallelDataGenerator           │
│  ValueGenerator          │  StreamingDataGenerator          │
│  OptimizedConstraintMgr  │  UnifiedValidator                │
└─────────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────────┐
│                   Infrastructure Layer                      │
├─────────────────────────────────────────────────────────────┤
│  StreamingWriters        │  MemoryManager                   │
│  LRUConstraintCache      │  ValidationPatterns              │
└─────────────────────────────────────────────────────────────┘
```

## 🔧 Core Components

### 1. Configuration System

#### ConfigurationManager
```python
class ConfigurationManager:
    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.config_cache = {}
        self.environment_configs = {}
        self.template_configs = {}
        self._load_default_configs()
```

**Key Features:**
- Environment-specific configurations (dev/test/prod)
- Configuration templates and wizards
- OpenAI integration testing
- Schema validation and auto-correction
- Environment variable overrides

**Usage Patterns:**
```python
# Environment-aware loading
config = config_manager.load_configuration('config.json', 'production')

# OpenAI testing
status = config_manager.test_openai_connection(config)

# Template generation
template = config_manager.generate_config_template_with_openai(
    table_schemas, "production", enable_openai=True
)
```

### 2. Data Generation Engine

#### DataGenerator Core
```python
class DataGenerator:
    def __init__(self, config, locale=None, logger=None, open_api_key=None):
        self.config = config
        self.faker = Faker(locale) if locale else Faker()
        self.validator = DataValidator(logger=self.logger)
        self.constraint_manager = ConstraintManager(logger=self.logger)
        
        # Component composition
        self.converter = DataTypeConverter(self.logger)
        self.value_generator = ValueGenerator(self.faker, self.logger, open_api_key)
        self.fk_manager = ForeignKeyManager(...)
        self.batch_generator = BatchGenerator(...)
```

**Thread Safety:**
- Uses `threading.RLock()` for constraint tracking
- Thread-safe caching with proper synchronization
- Concurrent foreign key pool management

#### ValueGenerator with OpenAI
```python
class ValueGenerator:
    def _initialize_openai(self, openai_config):
        # API key management with fallback chain
        api_key = openai_config.get_api_key()  # Direct -> File -> Env
        
        # Cost tracking and limits
        self._cost_tracker = 0.0
        
        # LRU caching for generated values
        self._openai_cache = {}
        self._cache_usage_count = {}
```

**AI Integration Features:**
- Intelligent caching with LRU eviction
- Cost tracking and budget limits
- Fallback strategies when AI fails
- Batch generation for efficiency

### 3. Constraint Management

#### OptimizedConstraintManager
```python
class OptimizedConstraintManager:
    def __init__(self, logger=None, max_memory_mb=500, enable_parallel=True):
        self._lock = threading.RLock()
        
        # LRU caches for performance
        self._pk_cache = LRUConstraintCache(maxsize=cache_size_limit)
        self._unique_cache = LRUConstraintCache(maxsize=cache_size_limit)
        
        # Memory management
        self.memory_manager = MemoryManager(max_memory_mb)
        
        # Thread pool for parallel operations
        self._thread_pool = ThreadPoolExecutor(max_workers=4)
```

**Performance Optimizations:**
- LRU caching for constraint lookups
- Memory-bounded constraint pools
- Parallel foreign key refreshing
- Intelligent cleanup strategies

### 4. Validation System

#### UnifiedValidator
```python
class UnifiedValidator:
    def __init__(self, logger=None):
        self.patterns = ValidationPatterns()  # Compiled regex patterns
        self._initialize_validators()
        
        # Performance tracking
        self._validation_stats = {
            'total_validations': 0,
            'cache_hits': 0,
            'validation_errors': 0
        }
```

**Validator Types:**
- `RegexValidator` - Pattern matching with caching
- `EmailValidator` - RFC-compliant email validation
- `PhoneValidator` - Country-specific phone validation
- `DateValidator` - Multi-format date validation
- `RangeValidator` - Numeric range validation
- `LengthValidator` - String length validation
- `ChoiceValidator` - Choice/enum validation
- `CreditCardValidator` - Luhn algorithm validation
- `TypeValidator` - Data type validation
- `PrefixSuffixValidator` - Advanced prefix/suffix validation

### 5. Streaming & Parallel Processing

#### ParallelDataGenerator
```python
class ParallelDataGenerator:
    def __init__(self, data_generator_instance, max_workers=None, 
                 max_memory_mb=1000, enable_streaming=True, logger=None):
        self.data_generator = data_generator_instance
        self.thread_pool = ThreadPoolExecutor(max_workers=self.max_workers)
        self.memory_monitor = MemoryMonitor(max_memory_mb)
        
        # Strategy selection
        self.default_batch_size = 10000
        self.streaming_batch_size = 1000
```

**Generation Strategies:**
```python
def generate_adaptive(self, table_metadata, total_records, **kwargs):
    estimated_memory = self._estimate_memory_requirements(...)
    
    if estimated_memory <= available_memory * 0.5:
        return self.generate_parallel(...)
    elif estimated_memory <= available_memory * 0.8:
        return self.generate_streaming_parallel(...)
    else:
        return self.generate_streaming(...)
```

### 6. I/O System

#### StreamingWriters
```python
class StreamingWriter(ABC):
    def __init__(self, file_path, buffer_size=8192, enable_progress=False):
        self.buffer_size = buffer_size
        self.enable_progress = enable_progress
        self.records_written = 0
        self.bytes_written = 0
        
    @abstractmethod
    def write_batch(self, batch: List[Dict[str, Any]]) -> None:
        pass
```

**Writer Implementations:**
- `StreamingCSVWriter` - RFC 4180 compliant CSV
- `StreamingJSONWriter` - JSONL and JSON array formats
- `StreamingParquetWriter` - Columnar storage with compression
- `StreamingExcelWriter` - Multi-sheet Excel support
- `CompressionWriter` - Transparent compression wrapper

## 🚀 Advanced Programming Patterns

### 1. Memory Management

#### LRU Cache Implementation
```python
class LRUConstraintCache:
    def __init__(self, maxsize: int = 10000):
        self.maxsize = maxsize
        self.cache = {}
        self.access_order = []
        self._lock = threading.RLock()
    
    def get(self, key: str, default=None):
        with self._lock:
            if key in self.cache:
                self.access_order.remove(key)
                self.access_order.append(key)
                return self.cache[key]
            return default
```

#### Memory Monitoring
```python
class MemoryMonitor:
    def _monitor_loop(self):
        while self._monitoring:
            current_memory = psutil.Process().memory_info().rss / 1024 / 1024
            
            if current_memory > self.max_memory_mb:
                for callback in self._callbacks:
                    callback()
            
            time.sleep(self.check_interval)
```

### 2. Async and Concurrent Patterns

#### Thread-Safe Constraint Management
```python
def generate_unique_pk_value(self, table_name, column_name, value_generator_func):
    constraint_key = f"{table_name}.{column_name}"
    
    with self._lock:
        existing_values = self._pk_cache.get(constraint_key, set())
        
        for attempt in range(max_attempts):
            value = value_generator_func()
            if value not in existing_values:
                self._pk_cache.add_to_set(constraint_key, value)
                return value
```

#### Parallel Processing with Error Handling
```python
def _execute_thread_parallel(self, tasks):
    futures = []
    for task in tasks:
        future = self.thread_pool.submit(self._execute_generation_task, task)
        futures.append(future)
    
    results = []
    for future in as_completed(futures):
        try:
            result = future.result(timeout=300)
            results.append(result)
        except Exception as e:
            self.logger.error(f"Task failed: {e}")
            results.append(GenerationResult(error=str(e)))
    
    return results
```

### 3. Performance Optimization Techniques

#### Compiled Regex Patterns
```python
class ValidationPatterns:
    def __init__(self):
        self._patterns = {...}
        self._compiled_patterns = {}
        self._compile_patterns()
    
    def _compile_patterns(self):
        for name, pattern in self._patterns.items():
            try:
                self._compiled_patterns[name] = re.compile(pattern, re.IGNORECASE)
            except re.error as e:
                logging.warning(f"Failed to compile pattern '{name}': {e}")
```

#### Function-Level Caching
```python
@lru_cache(maxsize=1000)
def validate_pattern(self, pattern_name: str, value: str) -> bool:
    compiled_pattern = self.patterns.get_pattern(pattern_name)
    if compiled_pattern:
        return bool(compiled_pattern.match(value))
    return False
```

## 🔒 Security Implementation

### Data Masking Strategies
```python
class SecurityManager:
    def _mask_email(self, email: str) -> str:
        if '@' not in email:
            return email
        
        local, domain = email.split('@', 1)
        if len(local) > 2:
            masked_local = local[0] + '*' * (len(local) - 2) + local[-1]
        else:
            masked_local = '*' * len(local)
        
        return f"{masked_local}@{domain}"
    
    def _encrypt_value(self, value: Any) -> Optional[str]:
        # AES encryption implementation
        key_hash = hashlib.sha256(self.encryption_key).digest()[:16]
        encrypted_bytes = bytes([ord(c) ^ key_hash[i % len(key_hash)] 
                               for i, c in enumerate(str(value))])
        return f"ENC:{base64.b64encode(encrypted_bytes).decode()}"
```

### Audit Trail Implementation
```python
def audit_data_generation(self, generation_params, records_count, sensitive_columns):
    audit_record = {
        'audit_id': str(uuid.uuid4()),
        'timestamp': datetime.now().isoformat(),
        'event_type': 'data_generation',
        'generation_params': generation_params,
        'records_generated': records_count,
        'sensitive_columns': sensitive_columns,
        'compliance_level': self._assess_compliance_level(generation_params)
    }
    
    self.audit_trail.append(audit_record)
    return audit_record
```

## 🧪 Testing Strategies

### Unit Testing Patterns
```python
class TestDataGenerator(unittest.TestCase):
    def setUp(self):
        self.config = self._create_test_config()
        self.generator = DataGenerator(self.config)
    
    def test_generate_batch_optimized(self):
        table_metadata = self._create_test_table_metadata()
        batch = self.generator.generate_batch_optimized(table_metadata, 100)
        
        self.assertEqual(len(batch), 100)
        self.assertTrue(all('id' in record for record in batch))
        
    def test_constraint_validation(self):
        # Test constraint enforcement
        pass
```

### Integration Testing
```python
class TestFullWorkflow(unittest.TestCase):
    def test_end_to_end_generation(self):
        # Test complete workflow from config to output
        config = load_test_config()
        orchestrator = OptimizedDataGenerationOrchestrator(config, self.logger)
        
        result = orchestrator.run_data_generation(1000, './test_output')
        
        self.assertGreater(len(result), 0)
        self.assertTrue(os.path.exists('./test_output'))
```

### Performance Testing
```python
class TestPerformance(unittest.TestCase):
    def test_memory_usage(self):
        initial_memory = psutil.Process().memory_info().rss
        
        # Generate large dataset
        data = self.generator.generate_large_dataset(100000)
        
        final_memory = psutil.Process().memory_info().rss
        memory_increase = (final_memory - initial_memory) / 1024 / 1024
        
        self.assertLess(memory_increase, 500)  # Less than 500MB increase
```

## 🔧 Extension Points

### Custom Validators
```python
class CustomValidator(BaseValidator):
    def validate(self, value: Any, **kwargs) -> Tuple[bool, str]:
        # Your custom validation logic
        if self._is_valid(value):
            return True, "Custom validation passed"
        return False, "Custom validation failed"

# Register custom validator
unified_validator.validators['custom'] = CustomValidator()
```

### Custom Writers
```python
class CustomStreamingWriter(StreamingWriter):
    def write_batch(self, batch: List[Dict[str, Any]]) -> None:
        # Your custom output format implementation
        for record in batch:
            self._write_record(record)
    
    def _write_record(self, record):
        # Custom serialization logic
        pass
```

### Custom Value Generators
```python
class CustomValueGenerator:
    def generate_by_rule(self, rule, data_type, column_name):
        if isinstance(rule, dict) and rule.get('type') == 'custom_type':
            return self._generate_custom_value(rule)
        return None
    
    def _generate_custom_value(self, rule):
        # Your custom generation logic
        pass
```

### Choice From File Feature

#### ChoiceFileLoader Implementation
```python
class ChoiceFileLoader:
    def __init__(self):
        self.loaded_files = {}  # Cache for loaded files

    def load_choices_from_file(self, file_path: str, column: str = None,
                               weight_column: str = None, **kwargs) -> Dict[str, Any]:
        """Load choices from CSV/Excel file with optional weighting"""
        
        # Load file based on extension
        if full_path.suffix.lower() in ['.xlsx', '.xls']:
            df = pd.read_excel(full_path, **kwargs)
        elif full_path.suffix.lower() == '.csv':
            df = pd.read_csv(full_path, **kwargs)
        
        # Use first column if none specified
        if column is None:
            column = df.columns[0]
        
        # Get unique values
        choices = df[column].dropna().unique().tolist()
        
        # Build choice rule with optional weights
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
        
        return choice_rule
```

#### Configuration Usage
```json
{
  "name": "product_category",
  "type": "str",
  "rule": {
    "type": "choice_from_file",
    "file_path": "data/product_categories.csv",
    "column": "category_name",
    "weight_column": "popularity_weight"
  }
}
```

#### Advanced File-Based Choices
```python
# Multiple file formats supported
choice_configs = [
    {
        "type": "choice_from_file",
        "file_path": "data/cities.csv",
        "column": "city_name",
        "weight_column": "population"
    },
    {
        "type": "choice_from_file", 
        "file_path": "data/products.xlsx",
        "column": "product_name",
        "weight_column": "sales_volume"
    }
]

# Caching mechanism for performance
choice_loader = ChoiceFileLoader()
for config in choice_configs:
    choices = choice_loader.load_choices_from_file(**config)
    # File is cached for subsequent use
```

#### Error Handling and Validation
```python
def validate_file_exists(self, file_path: str) -> bool:
    """Check if the choice file exists"""
    return Path(file_path).exists()

def get_file_info(self, file_path: str) -> Dict[str, Any]:
    """Get information about a choice file"""
    if not Path(file_path).exists():
        return {"exists": False}
    
    try:
        df = pd.read_excel(file_path, nrows=5) if file_path.endswith('.xlsx') else pd.read_csv(file_path, nrows=5)
        return {
            "exists": True,
            "columns": df.columns.tolist(),
            "sample_data": df.head().to_dict('records')
        }
    except Exception as e:
        return {"exists": True, "error": str(e)}
```

## 📊 Performance Tuning

### Memory Optimization
```python
# Configure memory limits
config.performance.max_memory_mb = 2000
config.performance.batch_size = 5000

# Enable streaming for large datasets
if total_records > 100000:
    config.performance.enable_streaming = True
```

### Parallel Processing Optimization
```python
# CPU-bound tasks - use processes
if cpu_intensive_generation:
    parallel_gen.generate_parallel(use_processes=True)

# I/O-bound tasks - use threads
if io_intensive_operations:
    parallel_gen.generate_parallel(use_processes=False)
```

### Constraint Cache Tuning
```python
# Adjust cache sizes based on available memory
constraint_manager = OptimizedConstraintManager(
    max_memory_mb=1000,
    cache_size_limit=50000  # Adjust based on dataset size
)
```

## 🚀 Deployment Patterns

### Configuration Management
```python
# Environment-specific configurations
configs = {
    'development': {
        'performance': {'max_workers': 2, 'max_memory_mb': 500},
        'openai': {'cost_limit_usd': 5.0},
        'security': {'enable_masking': False}
    },
    'production': {
        'performance': {'max_workers': 8, 'max_memory_mb': 4000},
        'openai': {'cost_limit_usd': 100.0},
        'security': {'enable_masking': True, 'audit_enabled': True}
    }
}
```

### Docker Deployment
```dockerfile
FROM python:3.9-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

ENV PYTHONPATH=/app
ENV DG_MAX_WORKERS=4
ENV DG_MAX_MEMORY_MB=2000

CMD ["python", "main.py", "--config", "config.json"]
```

### Monitoring and Observability
```python
# Performance monitoring
profiler = PerformanceProfiler()
with profiler.profile("data_generation"):
    generate_data()

# Memory monitoring
memory_monitor = MemoryMonitor(max_memory_mb=2000)
memory_monitor.add_cleanup_callback(cleanup_function)
memory_monitor.start_monitoring()

# Statistics collection
stats = generator.get_comprehensive_statistics()
logger.info(f"Generation rate: {stats['records_per_second']:.1f} records/sec")
```

## 🔍 Debugging and Troubleshooting

### Logging Configuration
```python
# Structured logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('data_generation.log')
    ]
)

# Component-specific logging
logging.getLogger('faker').setLevel(logging.WARNING)
logging.getLogger('openai').setLevel(logging.DEBUG)
```

### Error Handling Patterns
```python
try:
    batch_data = generator.generate_batch_optimized(table_metadata, batch_size)
except MemoryError:
    logger.error("Memory limit exceeded, switching to streaming mode")
    batch_data = generator.generate_streaming(table_metadata, batch_size)
except ValidationError as e:
    logger.error(f"Validation failed: {e}")
    # Apply data corrections or fallback generation
except Exception as e:
    logger.error(f"Unexpected error: {e}", exc_info=True)
    # Implement appropriate error recovery
```

### Performance Debugging
```python
# Profile generation performance
with PerformanceProfiler() as profiler:
    with profiler.profile("constraint_checking"):
        constraint_manager.validate_constraints(data)
    
    with profiler.profile("value_generation"):
        value_generator.generate_batch(rules)

# Analyze performance bottlenecks
report = profiler.get_profile_report()
for operation, metrics in report['operations'].items():
    if metrics['average_duration_seconds'] > threshold:
        logger.warning(f"Slow operation: {operation}")
```

This developer guide provides the technical depth needed for developers to understand, extend, and maintain the Enhanced Data Generator framework effectively.