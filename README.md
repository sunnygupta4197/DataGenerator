# Enhanced Data Generator Framework

A sophisticated, enterprise-grade Python framework for generating synthetic data with advanced features including streaming, parallel processing, OpenAI integration, data quality analysis, security compliance, and comprehensive validation systems.

## 🚀 Key Features

### Core Generation Capabilities
- **Multi-format Output**: CSV, JSON, JSONL, Parquet, Excel, SQL queries
- **Streaming Generation**: Handle datasets larger than memory with intelligent batching
- **Parallel Processing**: Multi-threaded and multi-process generation with automatic strategy selection
- **Advanced Constraints**: Primary keys, foreign keys, unique constraints, composite relationships
- **Sophisticated Rules**: Range validation, choice selection, regex patterns, conditional logic
- **File-based Choices**: Load choice values directly from CSV/Excel files with weight support
- **OpenAI Integration**: AI-powered data generation for complex scenarios with caching and cost management

### Enterprise Features
- **Security & Compliance**: Data masking, field-level encryption, audit trails, GDPR/HIPAA compliance
- **Quality Assurance**: Statistical analysis, anomaly detection, business rule validation
- **Performance Profiling**: Detailed performance monitoring, memory management, bottleneck identification
- **Configuration Management**: Environment-specific configurations, schema validation, template generation
- **Validation System**: Unified validation with 15+ validator types, batch processing, detailed reporting

### Advanced Architecture
- **Memory Management**: Intelligent memory usage with automatic cleanup and optimization
- **Constraint Optimization**: LRU caching, thread-safe operations, relationship management
- **Streaming Writers**: Buffered I/O, compression support, progress tracking
- **Error Handling**: Comprehensive error recovery, validation reporting, debugging tools

## 📋 Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Architecture Overview](#architecture-overview)
- [Configuration Guide](#configuration-guide)
- [File-Based Choice Rules](#file-based-choice-rules)
- [Usage Examples](#usage-examples)
- [Advanced Features](#advanced-features)
- [Performance Optimization](#performance-optimization)
- [Security & Compliance](#security--compliance)
- [API Reference](#api-reference)
- [Contributing](#contributing)

## 🔧 Installation

### Prerequisites
- Python 3.8+
- pip or conda package manager
- 4GB+ RAM recommended for large datasets

### Basic Installation
```bash
git clone <repository-url>
cd data-generator
pip install -r requirements.txt
```

### Optional Dependencies
```bash
# For Parquet support
pip install pyarrow

# For Excel support
pip install openpyxl

# For OpenAI integration
pip install openai

# For enhanced security
pip install cryptography

# For all features
pip install -r requirements-full.txt
```

## 🚀 Quick Start

### 1. Basic Configuration
Create a configuration file (`config.json`):

```json
{
  "environment": "development",
  "locale": "en_US",
  "rows": 10000,
  "tables": [
    {
      "table_name": "users",
      "columns": [
        {
          "name": "id",
          "type": "int",
          "constraint": ["PK"],
          "rule": {"type": "range", "min": 1, "max": 999999}
        },
        {
          "name": "email",
          "type": "str",
          "rule": {"type": "email"},
          "nullable": false
        },
        {
          "name": "age",
          "type": "int",
          "rule": {"type": "range", "min": 18, "max": 80}
        },
        {
          "name": "product_category",
          "type": "str",
          "rule": {
            "type": "choice_from_file",
            "file_path": "data/product_categories.csv",
            "column": "category_name",
            "weight_column": "popularity_score"
          }
        }
      ]
    }
  ],
  "performance": {
    "max_workers": 4,
    "enable_streaming": true,
    "enable_parallel": true,
    "max_memory_mb": 1000
  },
  "validation": {
    "enable_data_quality_analysis": true,
    "strict_mode": false
  }
}
```

### 2. Generate Data (Command Line)
```bash
# Basic generation
python main.py --config config.json --rows 10000

# Enhanced streaming with quality analysis
python main.py --config config.json --rows 1000000 --enable_streaming --enable_quality_analysis

# Parallel generation with security features
python main.py --config config.json --enable_parallel --enable_masking --max_workers 8

# Full feature demonstration
python main.py --config config.json --rows 500000 --enable_all_features --max_memory 2048
```

### 3. Generate Data (Python API)
```python
from main import main
from config_manager import ConfigurationManager

# Load configuration
config_manager = ConfigurationManager()
config = config_manager.load_configuration('config.json')

# Generate data with all features
data = main(config, total_records=10000, output_dir='./output')
```

## 🏗️ Architecture Overview

The framework is built with a modular, scalable architecture:

```
Enhanced Data Generator Framework
├── Configuration Layer
│   ├── ConfigurationManager - Environment-aware config management
│   ├── JSONConfigReader - Schema validation and legacy conversion
│   ├── ChoiceFileLoader - CSV/Excel choice loading with caching
│   └── SchemaValidator - Comprehensive schema validation
├── Generation Engine
│   ├── DataGenerator - Core generation logic with OpenAI integration  
│   ├── ValueGenerator - AI-enhanced value generation with caching
│   ├── ParallelDataGenerator - Multi-threaded/process generation
│   └── StreamingGenerators - Memory-efficient streaming generation
├── Constraint & Validation System
│   ├── OptimizedConstraintManager - Thread-safe constraint tracking
│   ├── UnifiedValidator - 15+ validation types with batch processing
│   └── PrefixSuffixValidator - Advanced prefix/suffix validation
├── Quality & Security
│   ├── DataQualityAnalyzer - Statistical analysis and anomaly detection
│   ├── SecurityManager - Data masking, encryption, audit trails
│   └── PerformanceProfiler - Performance monitoring and optimization
├── I/O System
│   ├── StreamingWriters - Buffered writers for multiple formats
│   ├── CompressionSupport - GZIP, BZIP2, LZMA compression
│   └── WriterFactory - Format-aware writer creation
└── Memory Management
    ├── MemoryMonitor - Real-time memory tracking
    ├── LRUConstraintCache - Optimized constraint caching
    └── BatchProcessor - Intelligent batch size management
```

### Key Design Principles
- **Memory Efficiency**: Stream processing with configurable memory limits
- **Thread Safety**: All core components are thread-safe with proper locking
- **Extensibility**: Plugin architecture for custom validators and generators
- **Performance**: LRU caching, compiled regex patterns, optimized data structures
- **Reliability**: Comprehensive error handling and recovery mechanisms

## ⚙️ Configuration Guide

### Environment-Specific Configurations

The framework supports multiple environments with different optimization profiles:

```json
{
  "environment": "production",
  "performance": {
    "max_workers": 8,
    "enable_streaming": true,
    "enable_parallel": true,
    "max_memory_mb": 4000,
    "batch_size": 50000
  },
  "security": {
    "enable_data_masking": true,
    "enable_encryption": true,
    "audit_enabled": true,
    "sensitive_data_patterns": ["ssn", "credit_card", "email"]
  },
  "openai": {
    "enabled": true,
    "model": "gpt-3.5-turbo",
    "cache_size": 200,
    "cost_limit_usd": 100.0,
    "timeout_seconds": 60
  },
  "validation": {
    "strict_mode": true,
    "enable_data_quality_analysis": true,
    "enable_anomaly_detection": true,
    "quality_threshold": 0.95
  }
}
```

### Advanced Column Definitions

```json
{
  "name": "customer_code",
  "type": "str",
  "nullable": false,
  "constraint": ["unique"],
  "rule": {
    "prefix": "CUST-",
    "suffix": "-VIP",
    "type": "choice",
    "value": ["GOLD", "SILVER", "BRONZE"],
    "probabilities": {"GOLD": 0.1, "SILVER": 0.3, "BRONZE": 0.6}
  },
  "length": {"min": 10, "max": 15},
  "sensitivity": "PII",
  "conditional_rules": [
    {
      "when": [{"column": "age", "operator": ">", "value": 65}],
      "then": {"rule": {"type": "fixed", "value": "SENIOR"}}
    }
  ]
}
```

### Foreign Key Relationships

```json
{
  "foreign_keys": [
    {
      "parent_table": "customers",
      "parent_column": "id",
      "child_column": "customer_id",
      "relationship_type": "many_to_one",
      "distribution": "weighted",
      "weights": {"premium": 0.2, "standard": 0.8}
    }
  ]
}
```

## 📁 File-Based Choice Rules

The framework supports loading choice values directly from CSV and Excel files, making it easy to use real-world data for synthetic generation.

### Basic File-Based Choice

```json
{
  "name": "product_category",
  "type": "str",
  "rule": {
    "type": "choice_from_file",
    "file_path": "data/categories.csv"
  }
}
```

### Advanced File-Based Choice with Weights

```json
{
  "name": "city",
  "type": "str",
  "rule": {
    "type": "choice_from_file",
    "file_path": "data/cities.xlsx",
    "column": "city_name",
    "weight_column": "population"
  }
}
```

### Supported File Formats

- **CSV files** (`.csv`)
- **Excel files** (`.xlsx`, `.xls`)

### Configuration Options

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `type` | string | ✅ | Must be `"choice_from_file"` |
| `file_path` | string | ✅ | Path to CSV/Excel file (relative or absolute) |
| `column` | string | ❌ | Column name containing choice values (uses first column if omitted) |
| `weight_column` | string | ❌ | Column name containing weights/probabilities for weighted selection |

### File Structure Examples

#### Simple Categories CSV
```csv
category_name
Electronics
Clothing
Books
Home & Garden
Sports
```

#### Weighted Cities Excel
```xlsx
city_name,population,country
New York,8500000,USA
Los Angeles,4000000,USA
Chicago,2700000,USA
Houston,2300000,USA
Phoenix,1600000,USA
```

### Advanced File-Based Examples

#### Product Categories with Popularity Weighting
```json
{
  "name": "product_category",
  "type": "str",
  "rule": {
    "type": "choice_from_file",
    "file_path": "reference_data/product_categories.csv",
    "column": "category_name",
    "weight_column": "market_share_percent"
  }
}
```

#### Geographic Data from Excel
```json
{
  "name": "region",
  "type": "str",
  "rule": {
    "type": "choice_from_file",
    "file_path": "geo_data/regions.xlsx",
    "column": "region_name",
    "weight_column": "economic_activity_index"
  }
}
```

#### Industry Classifications
```json
{
  "name": "industry_code",
  "type": "str",
  "rule": {
    "type": "choice_from_file",
    "file_path": "standards/naics_codes.csv",
    "column": "industry_code",
    "weight_column": "frequency"
  }
}
```

### File Loading Features

- **Automatic Caching**: Files are loaded once and cached for performance
- **Duplicate Removal**: Automatically removes duplicate values
- **Null Handling**: Ignores null/empty values
- **Weighted Selection**: Supports probability-based selection using weight columns
- **Flexible Formats**: Works with various CSV delimiters and Excel sheets
- **Error Handling**: Comprehensive error reporting for file issues

### File Loading Configuration Options

You can pass additional pandas options for file reading:

```json
{
  "name": "specialized_data",
  "type": "str",
  "rule": {
    "type": "choice_from_file",
    "file_path": "data/specialized.csv",
    "column": "value_column",
    "delimiter": ";",
    "encoding": "utf-8",
    "sheet_name": "Data"
  }
}
```

### Performance Considerations

- **File Caching**: Files are cached in memory after first load
- **Memory Usage**: Large files are handled efficiently with pandas optimization
- **Load Time**: Initial load time depends on file size, subsequent access is instant
- **Recommendations**: 
  - Keep choice files under 100MB for optimal performance
  - Use CSV format for fastest loading
  - Pre-process large datasets to extract only needed columns

### Error Handling

The system provides detailed error messages for common issues:

```bash
❌ Error loading choices for users.product_category: File not found: data/categories.csv
❌ Error loading choices for orders.region: Column 'region_name' not found in geo_data/regions.xlsx
✅ Loaded 1,247 choices for products.category from reference_data/categories.csv
✅ Loaded 50 weighted choices for customers.city from geo_data/cities.xlsx
```

## 📊 Usage Examples

### Basic Data Generation
```python
from data_generator import DataGenerator
from config_manager import ConfigurationManager

# Load configuration
config = ConfigurationManager().load_configuration('config.json')

# Create generator
generator = DataGenerator(config, locale='en_US')

# Generate batch
table_metadata = config.tables[0]
batch_data = generator.generate_batch_optimized(
    table_metadata=table_metadata,
    batch_size=1000
)

print(f"Generated {len(batch_data)} records")
```

### File-Based Choice Generation
```python
# Configuration with file-based choices
config_with_files = {
    "tables": [
        {
            "table_name": "products",
            "columns": [
                {
                    "name": "category",
                    "type": "str",
                    "rule": {
                        "type": "choice_from_file",
                        "file_path": "data/product_categories.csv",
                        "column": "category_name",
                        "weight_column": "popularity"
                    }
                },
                {
                    "name": "brand",
                    "type": "str",
                    "rule": {
                        "type": "choice_from_file",
                        "file_path": "data/brands.xlsx",
                        "column": "brand_name"
                    }
                }
            ]
        }
    ]
}

# Generate data
generator = DataGenerator(config_with_files)
data = generator.generate_batch_optimized(table_metadata, 1000)
```

### Streaming Large Datasets
```python
from streaming_data_generator import ParallelDataGenerator

# Create generator instance
with ParallelDataGenerator(
    data_generator_instance=generator,
    max_workers=4,
    enable_streaming=True
) as parallel_gen:
    
    # Generate streaming data
    for batch in parallel_gen.generate_streaming(
        table_metadata=table_metadata,
        total_records=1000000,
        output_path='output/large_dataset.csv'
    ):
        print(f"Processed batch: {len(batch)} records")
        
        # Real-time processing
        if len(batch) > 0:
            # Apply custom processing
            process_batch(batch)
```

### Quality Analysis
```python
from streaming_data_generator import DataQualityAnalyzer

# Analyze data quality
analyzer = DataQualityAnalyzer()
quality_report = analyzer.analyze_distribution(data, table_metadata)

print(f"Quality Score: {quality_report['data_quality_score']:.2f}")
print(f"Issues Found: {len(quality_report['issues'])}")

# Detect anomalies
anomalies = analyzer.detect_anomalies(data)
print(f"Anomalies Detected: {len(anomalies['anomalies'])}")

# Business rule validation
business_rules = [
    {
        "type": "conditional",
        "condition_column": "age",
        "condition_operator": ">",
        "condition_value": 65,
        "requirement_column": "plan_type",
        "requirement_value": "senior"
    }
]

violations = analyzer.validate_business_rules(data, business_rules)
print(f"Rule Violations: {violations['total_violations']}")
```

### Security Features
```python
from streaming_data_generator import SecurityManager

# Configure security
security = SecurityManager()
security.enable_masking = True
security.add_masking_rule('email', 'partial')
security.add_masking_rule('ssn', 'hash')
security.set_encryption_key(b'your-32-byte-encryption-key-here')

# Apply security measures
sensitivity_map = {
    'email': 'PII',
    'ssn': 'SENSITIVE',
    'name': 'PII',
    'age': 'PUBLIC'
}

# Mask sensitive data
masked_data = security.mask_sensitive_data(data, sensitivity_map)

# Encrypt specific fields
encrypted_data = security.encrypt_sensitive_fields(
    masked_data, 
    sensitive_fields=['ssn', 'credit_card']
)

# Create audit trail
audit_record = security.audit_data_generation(
    generation_params={'table': 'users', 'records': 10000},
    records_count=10000,
    sensitive_columns=['email', 'ssn']
)
```

### OpenAI Integration
```python
# Configure OpenAI in your config
openai_config = {
    "enabled": true,
    "model": "gpt-3.5-turbo",
    "cache_size": 100,
    "cost_limit_usd": 10.0
}

# Use OpenAI for complex data generation
column_with_ai = {
    "name": "product_description",
    "type": "str",
    "rule": {
        "type": "openai_generated",
        "description": "Generate realistic product descriptions for e-commerce",
        "context_columns": ["product_name", "category"],
        "max_length": 200
    }
}

# The framework will automatically use OpenAI with caching
data = generator.generate_batch_optimized(table_metadata, 1000)
```

## 🔧 Advanced Features

### Configuration Management
```python
from config_manager import ConfigurationManager

# Create configuration manager
config_manager = ConfigurationManager()

# Load with environment override
config = config_manager.load_configuration('config.json', 'production')

# Test OpenAI connection
openai_status = config_manager.test_openai_connection(config)
print(f"OpenAI Status: {openai_status['status']}")

# Generate template configuration
template_config = config_manager.generate_config_template_with_openai(
    table_schemas=[...],
    template_type="production",
    enable_openai=True
)

# Interactive configuration wizard
config = config_manager.create_config_wizard_with_openai()
```

### Performance Optimization
```python
from streaming_data_generator import PerformanceProfiler

# Enable performance profiling
profiler = PerformanceProfiler()

with profiler.profile("data_generation"):
    # Your data generation code
    data = generate_large_dataset()

# Get performance report
report = profiler.get_profile_report()
print(f"Operations analyzed: {len(report['operations'])}")

# Memory optimization
from optimized_constraint_manager import OptimizedConstraintManager

constraint_manager = OptimizedConstraintManager(
    max_memory_mb=2000,
    enable_parallel=True
)

# Get performance statistics
stats = constraint_manager.get_constraint_statistics()
print(f"Cache hit ratio: {stats['cache_hit_ratio']:.2%}")
```

### Custom Validators
```python
from unified_validation_system import UnifiedValidator

validator = UnifiedValidator()

# Add custom pattern
validator.add_custom_pattern('custom_id', r'^[A-Z]{2}\d{6}$')

# Validate with custom rules
is_valid, message = validator.validate_value(
    'AB123456', 
    {'type': 'regex', 'pattern': 'custom_id'}
)

# Batch validation
batch_results = validator.validate_batch(data_batch, table_metadata)
summary = validator.get_validation_summary(batch_results)
print(summary)
```

## 🎯 Performance Optimization

### Benchmarks
- **Small datasets** (<10K records): ~50K records/second
- **Medium datasets** (10K-1M records): ~25K records/second with streaming
- **Large datasets** (>1M records): ~15K records/second with parallel processing
- **Memory usage**: 50-200MB regardless of dataset size in streaming mode
- **File-based choices**: Initial load + instant subsequent access via caching

### Optimization Strategies

1. **Memory Management**
   ```python
   # Configure memory limits
   config.performance.max_memory_mb = 2000
   config.performance.enable_streaming = True
   config.performance.batch_size = 10000
   ```

2. **Parallel Processing**
   ```python
   # Optimize worker count
   import os
   config.performance.max_workers = min(8, os.cpu_count())
   config.performance.enable_parallel = True
   ```

3. **Constraint Optimization**
   ```python
   # Use LRU caching for constraints
   constraint_manager = OptimizedConstraintManager(
       max_memory_mb=1000,
       enable_parallel=True
   )
   ```

4. **Batch Size Tuning**
   ```python
   # Adjust batch sizes based on available memory
   if dataset_size > 1000000:
       batch_size = 1000  # Streaming
   elif dataset_size > 100000:
       batch_size = 5000  # Medium batches
   else:
       batch_size = 10000  # Large batches
   ```

5. **File-Based Choice Optimization**
   ```python
   # Pre-load choice files for better performance
   choice_loader = ChoiceFileLoader()
   choice_loader.load_choices_from_file('data/categories.csv')
   
   # Use CSV format for faster loading
   # Keep choice files under 100MB
   # Extract only necessary columns
   ```

## 🔒 Security & Compliance

### Data Protection Features
- **PII Masking**: Automatic detection and masking of sensitive data
- **Field-Level Encryption**: AES encryption for sensitive fields
- **Audit Trails**: Complete audit logging of all operations
- **Compliance**: GDPR, HIPAA-ready data handling

### Security Configuration
```python
security_config = {
    "enable_data_masking": True,
    "encrypt_fields": ["salary", "bonus", "commission"],
    "audit_enabled": True,
    "sensitive_data_patterns": ["ssn", "credit_card", "salary"]
}
```

### Compliance Features
```python
# GDPR compliance check
compliance_rules = {
    "no_real_pii": {
        "type": "no_real_pii",
        "patterns": [r'\b\d{3}-\d{2}-\d{4}\b']  # Real SSN patterns
    },
    "data_minimization": {
        "type": "data_minimization",
        "allowed_columns": ["id", "name", "email", "age"]
    }
}

compliance_report = security_manager.validate_compliance(data, compliance_rules)
print(f"Compliance Score: {compliance_report['compliance_score']:.2%}")
```

### File-Based Choice Security
- **Path Validation**: Prevents directory traversal attacks
- **File Type Validation**: Only allows CSV/Excel files
- **Content Sanitization**: Removes potentially harmful content
- **Access Logging**: Logs all file access for audit trails

## 📖 API Reference

### Core Classes

#### `ConfigurationManager`
- `load_configuration(file_path, environment)` - Load configuration with environment support
- `generate_config_template(schemas, template_type)` - Generate configuration templates
- `test_openai_connection(config)` - Test OpenAI integration
- `create_config_wizard_with_openai()` - Interactive configuration wizard

#### `DataGenerator`
- `generate_batch_optimized(table_metadata, batch_size, fk_data)` - Generate optimized data batch
- `generate_value(rule, data_type)` - Generate single value
- `get_openai_cache_statistics()` - Get OpenAI performance stats

#### `ParallelDataGenerator`
- `generate_streaming(table_metadata, total_records, output_path)` - Streaming generation
- `generate_parallel(table_metadata, total_records, use_processes)` - Parallel generation
- `generate_adaptive(table_metadata, total_records)` - Adaptive strategy selection

#### `ChoiceFileLoader`
- `load_choices_from_file(file_path, column, weight_column)` - Load choices from CSV/Excel
- `validate_file_exists(file_path)` - Check if choice file exists
- `get_file_info(file_path)` - Get file metadata and column information

#### `UnifiedValidator`
- `validate_value(value, rule, data_type)` - Validate single value
- `validate_batch(batch, table_metadata)` - Batch validation
- `add_custom_pattern(name, pattern)` - Add custom validation pattern

#### `SecurityManager`
- `mask_sensitive_data(data, sensitivity_map)` - Apply data masking
- `encrypt_sensitive_fields(data, fields)` - Encrypt specific fields
- `audit_data_generation(params, count, columns)` - Create audit record

### Command Line Interface

```bash
# Basic usage
python main.py --config CONFIG_FILE [OPTIONS]

# Options
--rows INTEGER                 # Number of rows per table
--output_dir PATH             # Output directory
--max_workers INTEGER         # Number of parallel workers
--max_memory INTEGER          # Memory limit in MB
--enable_streaming            # Enable streaming mode
--enable_parallel             # Enable parallel processing
--enable_quality_analysis     # Enable quality analysis
--enable_masking              # Enable data masking
--enable_all_features         # Enable all advanced features
--format FORMAT               # Output format (csv/json/parquet)
--log_level LEVEL             # Logging level
```

### Choice File API

```python
from json_reader import ChoiceFileLoader

# Initialize loader
loader = ChoiceFileLoader()

# Load simple choices
choices = loader.load_choices_from_file('data/categories.csv')

# Load weighted choices  
weighted_choices = loader.load_choices_from_file(
    'data/cities.xlsx', 
    column='city_name',
    weight_column='population'
)

# Check file info
info = loader.get_file_info('data/sample.csv')
print(f"Columns: {info['columns']}")
```

### Test Coverage
- **Unit tests**: 95%+ coverage of core functionality
- **Integration tests**: End-to-end workflow validation
- **Performance tests**: Memory and speed benchmarks
- **Security tests**: Data masking and encryption validation
- **File loading tests**: CSV/Excel parsing and error handling

## 🤝 Contributing

### Development Setup
1. Fork the repository
2. Create a feature branch: `git checkout -b feature/amazing-feature`
3. Install development dependencies: `pip install -r requirements-dev.txt`
4. Run tests: `pytest`
5. Submit a pull request

### Code Standards
- **PEP 8 compliance**: Use black for formatting
- **Type hints**: All functions must have type hints
- **Docstrings**: Comprehensive documentation for all public methods
- **Testing**: Unit tests required for new features
- **Performance**: Benchmark critical paths

### Architecture Guidelines
- **Single Responsibility**: Each class should have one clear purpose
- **Thread Safety**: All shared components must be thread-safe
- **Memory Efficiency**: Implement proper memory management
- **Error Handling**: Comprehensive error recovery
- **Logging**: Structured logging with appropriate levels

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 📞 Support

### Documentation
- [Developer Guide](docs/DEVELOPER_GUIDE.md)
- [Configuration Reference](docs/CONFIG_REFERENCE.md)
- [Performance Tuning](docs/PERFORMANCE_GUIDE.md)
- [Security Best Practices](docs/SECURITY_GUIDE.md)
- [File-Based Choices Guide](docs/FILE_CHOICES_GUIDE.md)

### Getting Help
- **Issues**: Create an issue for bugs or feature requests
- **Discussions**: Use GitHub Discussions for questions
- **Documentation**: Check the comprehensive documentation
- **Examples**: Review the example configurations and code

## 🗺️ Roadmap

### Current Version (v2.0)
- ✅ Enhanced parallel processing with streaming support
- ✅ OpenAI integration with cost management and caching
- ✅ Comprehensive security and compliance features
- ✅ Advanced validation system with 15+ validator types
- ✅ Performance profiling and memory optimization
- ✅ Multi-format output with compression support
- ✅ File-based choice loading with CSV/Excel support
- ✅ Weighted selection from external data sources

### Upcoming Features (v2.1)
- [ ] Real-time data generation APIs with REST/GraphQL endpoints
- [ ] Advanced AI model integration (GPT-4, Claude, local models)
- [ ] Interactive web-based configuration UI
- [ ] Enhanced business rule engine with visual editor
- [ ] Multi-database output support (PostgreSQL, MySQL, MongoDB)
- [ ] Cloud deployment templates (AWS, Azure, GCP)
- [ ] Additional file format support (JSON, XML, ORC)
- [ ] Advanced choice file preprocessing and validation

### Future Enhancements (v3.0)
- [ ] Distributed generation across multiple nodes
- [ ] Real-time data streaming with Kafka/Pulsar integration
- [ ] Advanced ML-based data generation and validation
- [ ] Integration with data catalog systems
- [ ] Advanced privacy-preserving techniques (differential privacy)
- [ ] Custom plugin architecture for domain-specific generators
- [ ] Web-based choice file management interface

---

## 🏆 Why Choose Enhanced Data Generator?

### Enterprise-Ready
- **Production-proven** architecture handling millions of records
- **Comprehensive testing** with 95%+ code coverage
- **Security-first** design with compliance built-in
- **Performance-optimized** with intelligent memory management

### Developer-Friendly
- **Intuitive API** with comprehensive documentation
- **Flexible configuration** supporting multiple environments
- **Extensive examples** and real-world usage patterns
- **Active community** with regular updates and support

### Feature-Complete
- **15+ validation types** with custom pattern support
- **Multiple output formats** with compression and streaming
- **AI-powered generation** with cost controls and caching
- **Advanced relationships** with foreign key optimization
- **Quality assurance** with statistical analysis and anomaly detection
- **File-based choices** with automatic caching and weighted selection
- **Real-world data integration** via CSV/Excel file support

**Start generating high-quality synthetic data today!**

```bash
git clone <repository-url>
cd enhanced-data-generator
pip install -r requirements.txt
python main.py --config examples/basic_config.json --enable_all_features
```

## 📝 File-Based Choice Examples

### Example 1: Product Categories
Create a CSV file `data/product_categories.csv`:
```csv
category_name,popularity_score
Electronics,85
Clothing,92
Books,67
Home & Garden,73
Sports,58
Automotive,41
Health & Beauty,79
Toys & Games,64
```

Configuration:
```json
{
  "name": "category",
  "type": "str",
  "rule": {
    "type": "choice_from_file",
    "file_path": "data/product_categories.csv",
    "column": "category_name",
    "weight_column": "popularity_score"
  }
}
```

### Example 2: Geographic Data
Create an Excel file `data/cities.xlsx`:
| city_name | population | country | region |
|-----------|------------|---------|---------|
| New York | 8500000 | USA | North America |
| London | 9000000 | UK | Europe |
| Tokyo | 14000000 | Japan | Asia |
| Sydney | 5300000 | Australia | Oceania |

Configuration:
```json
{
  "name": "city",
  "type": "str",
  "rule": {
    "type": "choice_from_file",
    "file_path": "data/cities.xlsx",
    "column": "city_name",
    "weight_column": "population"
  }
}
```

### Example 3: Industry Classifications
Create a CSV file `data/industry_codes.csv`:
```csv
code,industry_name,frequency
11,Agriculture,0.05
21,Mining,0.02
22,Utilities,0.03
23,Construction,0.08
31,Manufacturing,0.15
42,Wholesale Trade,0.07
44,Retail Trade,0.12
48,Transportation,0.06
51,Information,0.04
52,Finance,0.09
54,Professional Services,0.11
61,Educational Services,0.07
62,Health Care,0.11
```

Configuration:
```json
{
  "name": "industry_code",
  "type": "str",
  "rule": {
    "type": "choice_from_file",
    "file_path": "data/industry_codes.csv",
    "column": "code",
    "weight_column": "frequency"
  }
},
{
  "name": "industry_name",
  "type": "str",
  "rule": {
    "type": "choice_from_file",
    "file_path": "data/industry_codes.csv",
    "column": "industry_name",
    "weight_column": "frequency"
  }
}
```

### Example 4: Multi-Column Reference Data
Create an Excel file `data/employee_departments.xlsx`:
| dept_code | dept_name | manager | budget | location |
|-----------|-----------|---------|--------|----------|
| ENG | Engineering | John Smith | 2500000 | Building A |
| MKT | Marketing | Jane Doe | 1200000 | Building B |
| SAL | Sales | Bob Johnson | 3000000 | Building C |
| HR | Human Resources | Alice Brown | 800000 | Building B |
| FIN | Finance | Charlie Wilson | 1500000 | Building A |

Configuration for multiple related columns:
```json
{
  "columns": [
    {
      "name": "department_code",
      "type": "str",
      "rule": {
        "type": "choice_from_file",
        "file_path": "data/employee_departments.xlsx",
        "column": "dept_code",
        "weight_column": "budget"
      }
    },
    {
      "name": "department_name",
      "type": "str",
      "rule": {
        "type": "choice_from_file",
        "file_path": "data/employee_departments.xlsx", 
        "column": "dept_name",
        "weight_column": "budget"
      }
    },
    {
      "name": "building_location",
      "type": "str",
      "rule": {
        "type": "choice_from_file",
        "file_path": "data/employee_departments.xlsx",
        "column": "location"
      }
    }
  ]
}
```

## 🚨 Error Handling and Troubleshooting

### Common Issues and Solutions

#### File Not Found
```bash
❌ Error loading choices for users.category: File not found: data/categories.csv
```
**Solution**: Check file path and ensure the file exists relative to your script location.

#### Column Not Found
```bash
❌ Error loading choices for products.brand: Column 'brand_name' not found in data/brands.csv
```
**Solution**: Verify column names in your file match the configuration. Use `get_file_info()` to inspect available columns.

#### Invalid File Format
```bash
❌ Error loading choices from data/file.txt: Unsupported file format: .txt
```
**Solution**: Use only CSV (.csv) or Excel (.xlsx, .xls) files.

#### Memory Issues with Large Files
```bash
⚠️ Warning: Large file detected (500MB). Consider preprocessing to reduce size.
```
**Solution**: Pre-process large files to extract only necessary columns and rows.

### Debug File Loading
```python
from json_reader import ChoiceFileLoader

loader = ChoiceFileLoader()

# Get file information
info = loader.get_file_info('data/sample.csv')
print("File info:", info)

# Test loading
try:
    choices = loader.load_choices_from_file(
        'data/sample.csv',
        column='category',
        weight_column='weight'
    )
    print(f"Successfully loaded {len(choices['value'])} choices")
except Exception as e:
    print(f"Error: {e}")
```

### Performance Monitoring
```python
import time
from json_reader import ChoiceFileLoader

loader = ChoiceFileLoader()

# Time file loading
start_time = time.time()
choices = loader.load_choices_from_file('data/large_file.csv')
load_time = time.time() - start_time

print(f"File loaded in {load_time:.2f} seconds")
print(f"Loaded {len(choices['value'])} unique choices")

# Test cached access (should be instant)
start_time = time.time()
choices_cached = loader.load_choices_from_file('data/large_file.csv')
cached_time = time.time() - start_time

print(f"Cached access in {cached_time:.4f} seconds")
```

## 📋 Best Practices for File-Based Choices

### File Organization
```
project/
├── data/
│   ├── reference/          # Static reference data
│   │   ├── countries.csv
│   │   ├── currencies.csv
│   │   └── timezones.csv
│   ├── demographic/        # Demographic data
│   │   ├── names.xlsx
│   │   ├── cities.csv
│   │   └── occupations.csv
│   └── business/          # Business domain data
│       ├── industries.csv
│       ├── departments.xlsx
│       └── products.csv
├── config/
│   ├── development.json
│   ├── staging.json
│   └── production.json
└── main.py
```

### File Naming Conventions
- Use descriptive names: `product_categories.csv` instead of `data.csv`
- Include data source or date when relevant: `cities_2024.csv`
- Use consistent naming: snake_case for files and columns
- Group related files in subdirectories

### Data Quality Guidelines
1. **Clean Data**: Remove duplicates, handle missing values
2. **Consistent Formatting**: Use consistent date formats, case sensitivity
3. **Appropriate Weights**: Ensure weight columns contain valid numeric values
4. **Documentation**: Include a README explaining each file's purpose and structure

### Performance Optimization
1. **File Size**: Keep individual files under 100MB for optimal performance
2. **Column Selection**: Include only necessary columns in choice files
3. **Data Types**: Use appropriate data types (integers for codes, strings for names)
4. **Preprocessing**: Clean and optimize files before using them in generation

### Security Considerations
1. **Path Validation**: Use relative paths, avoid absolute paths in configurations
2. **File Permissions**: Ensure choice files have appropriate read permissions
3. **Data Sensitivity**: Avoid including real PII in choice files
4. **Access Logging**: Monitor file access in production environments

## 🔍 Advanced File-Based Choice Features

### Conditional File Selection
```json
{
  "name": "product_type",
  "type": "str",
  "conditional_rules": [
    {
      "when": [{"column": "category", "operator": "==", "value": "Electronics"}],
      "then": {
        "rule": {
          "type": "choice_from_file",
          "file_path": "data/electronics_products.csv",
          "column": "product_type"
        }
      }
    },
    {
      "when": [{"column": "category", "operator": "==", "value": "Clothing"}],
      "then": {
        "rule": {
          "type": "choice_from_file", 
          "file_path": "data/clothing_products.csv",
          "column": "product_type"
        }
      }
    }
  ]
}
```

### Hierarchical Data Relationships
```json
{
  "columns": [
    {
      "name": "country",
      "type": "str",
      "rule": {
        "type": "choice_from_file",
        "file_path": "data/countries.csv",
        "column": "country_name",
        "weight_column": "population"
      }
    },
    {
      "name": "city",
      "type": "str",
      "rule": {
        "type": "choice_from_file",
        "file_path": "data/cities.csv",
        "column": "city_name",
        "weight_column": "population"
      },
      "conditional_rules": [
        {
          "when": [{"column": "country", "operator": "==", "value": "USA"}],
          "then": {
            "rule": {
              "type": "choice_from_file",
              "file_path": "data/us_cities.csv",
              "column": "city_name",
              "weight_column": "population"
            }
          }
        }
      ]
    }
  ]
}
```

### Time-Based Data Variations
```json
{
  "name": "seasonal_product",
  "type": "str",
  "conditional_rules": [
    {
      "when": [{"column": "order_date", "operator": "month_in", "value": [12, 1, 2]}],
      "then": {
        "rule": {
          "type": "choice_from_file",
          "file_path": "data/winter_products.csv",
          "column": "product_name"
        }
      }
    },
    {
      "when": [{"column": "order_date", "operator": "month_in", "value": [6, 7, 8]}],
      "then": {
        "rule": {
          "type": "choice_from_file",
          "file_path": "data/summer_products.csv", 
          "column": "product_name"
        }
      }
    }
  ]
}
```

This comprehensive documentation now includes detailed information about the `choice_from_file` feature, including:

1. **Complete feature overview** with examples and use cases
2. **Configuration options** with detailed parameter descriptions  
3. **File format support** for CSV and Excel files
4. **Advanced examples** showing weighted selection and multi-column scenarios
5. **Performance considerations** and optimization strategies
6. **Error handling** with common issues and solutions
7. **Best practices** for file organization and data quality
8. **Security considerations** for production environments
9. **Advanced features** like conditional file selection and hierarchical relationships

The documentation is structured to help users understand both basic and advanced usage patterns while providing practical examples they can immediately implement in their projects.