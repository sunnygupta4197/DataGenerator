# Data Generator Complete Reference Manual
## Version 2.0 - Comprehensive Documentation

---

## Table of Contents

1. [Quick Start Guide](#quick-start-guide)
2. [Output Formats](#output-formats)
3. [Date and Time Formats](#date-and-time-formats)
4. [Rule Types](#rule-types)
5. [Data Types](#data-types)
6. [Constraints and Relationships](#constraints-and-relationships)
7. [Faker Providers](#faker-providers)
8. [AI Features](#ai-features)
9. [Security Features](#security-features)
10. [Performance Features](#performance-features)
11. [Compression Support](#compression-support)
12. [Advanced Features](#advanced-features)
13. [Configuration Examples](#configuration-examples)
14. [Environment Variables](#environment-variables)
15. [Troubleshooting](#troubleshooting)
16. [Feature Compatibility Matrix](#feature-compatibility-matrix)

---

## Quick Start Guide

### Installation
```bash
pip install pandas faker
```

### Basic Configuration
```json
{
  "tables": [{
    "table_name": "users",
    "columns": [
      {"name": "id", "type": "int", "constraints": ["PK"]},
      {"name": "name", "type": "str", "rule": "name"},
      {"name": "email", "type": "str", "rule": "email"}
    ]
  }],
  "rows": 100,
  "output": {"format": "csv"}
}
```

### Basic Usage
```bash
python main.py --config config.json
```

---

## Output Formats

### Supported Formats

| Format | Aliases | Description | Use Case | File Extension |
|--------|---------|-------------|----------|----------------|
| **CSV** | csv | Comma-separated values | Universal compatibility | .csv |
| **TSV** | tsv | Tab-separated values | Data with commas | .tsv |
| **JSON** | json | JavaScript Object Notation | APIs, web apps | .json |
| **JSONL** | jsonl | JSON Lines | Streaming data, logs | .jsonl |
| **Fixed Width** | fixed, fwf, fixed_width | Fixed-width columns | Legacy systems, mainframes | .txt |
| **SQL Query** | sql, sql_query | SQL INSERT statements | Database loading | .sql |
| **Excel** | excel, xlsx, xls | Microsoft Excel format | Business reports | .xlsx |
| **Parquet** | parquet | Columnar storage | Big data, analytics | .parquet |

### Universal Fixed-Width Support

**ANY format can use fixed-width formatting:**

```json
{
  "format": "csv",
  "enable_fixed_width": true,
  "column_widths": {"name": 20, "age": 3, "city": 15}
}
```

**Output:**
```
John Smith          ,25 ,New York       
Jane Doe            ,30 ,Los Angeles    
```

### Universal Compression Support

**ANY format can be compressed:**

```json
{
  "format": "csv",
  "compression": "gzip"
}
```

**Supported Compression:**
- **GZIP**: Fast, ~70% reduction
- **BZ2**: Better compression, ~80% reduction  
- **LZMA**: Maximum compression, ~85% reduction

---

## Date and Time Formats

### Human-Readable Patterns

| Pattern | Description | Example Output | Auto-Converts To |
|---------|-------------|----------------|-------------------|
| DD/MM/YYYY | European date | 25/12/2023 | %d/%m/%Y |
| MM/DD/YYYY | US date | 12/25/2023 | %m/%d/%Y |
| YYYY-MM-DD | ISO date | 2023-12-25 | %Y-%m-%d |
| HH:MM:SS | 24-hour time | 14:30:45 | %H:%M:%S |
| hh:MM:SS A | 12-hour time | 02:30:45 PM | %I:%M:%S %p |
| YYYY-MM-DD HH:MM:SS | ISO timestamp | 2023-12-25 14:30:45 | %Y-%m-%d %H:%M:%S |

### Auto-Correction Rules

| Input | Corrected To | Reason |
|-------|--------------|--------|
| dd/mm/yyyy | DD/MM/YYYY | Case correction |
| min | mm | Minute format |
| sec | ss | Second format |
| am/pm | A | AM/PM format |

### Date Range Rules

```json
{
  "type": "date_range",
  "start": "2020-01-01",
  "end": "2024-12-31",
  "format": "DD/MM/YYYY"
}
```

---

## Rule Types

### Basic Rules

| Rule Type | Purpose | Required Fields | Example | Compatible Types |
|-----------|---------|----------------|---------|------------------|
| **choice** | Pick from list | value (array) | `{"type": "choice", "value": ["A", "B", "C"]}` | any |
| **range** | Numeric range | min, max | `{"type": "range", "min": 18, "max": 65}` | int, float |
| **sequence** | Sequential numbers | none | `{"type": "sequence", "start": 1000}` | int |
| **fixed** | Static value | value | `{"type": "fixed", "value": "ACTIVE"}` | any |

### Communication Rules

| Rule Type | Purpose | Example | Use Case |
|-----------|---------|---------|----------|
| **email** | Email addresses | `{"type": "email"}` | User accounts |
| **phone_number** | Phone numbers | `{"type": "phone_number"}` | Contact info |
| **uuid** | Unique identifiers | `{"type": "uuid"}` | Primary keys |

### Pattern Rules

| Rule Type | Purpose | Example | Use Case |
|-----------|---------|---------|----------|
| **regex** | Pattern matching | `{"type": "regex", "regex": "^[A-Z]{3}-\\d{4}$"}` | Product codes |

### Temporal Rules

| Rule Type | Purpose | Required Fields | Example |
|-----------|---------|----------------|---------|
| **date_range** | Date within range | start, end | `{"type": "date_range", "start": "2020-01-01", "end": "2023-12-31"}` |
| **time_range** | Time within range | start, end | `{"type": "time_range", "start": "09:00:00", "end": "17:00:00"}` |
| **timestamp_range** | Timestamp range | start, end | `{"type": "timestamp_range", "start": "2020-01-01 00:00:00"}` |

### Advanced Rules

#### Weighted Choices
```json
{
  "type": "choice",
  "value": ["Gold", "Silver", "Bronze"],
  "probabilities": {"Gold": 0.1, "Silver": 0.3, "Bronze": 0.6}
}
```

#### Conditional Rules
```json
{
  "type": "conditional",
  "when": [
    {"column": "age", "operator": ">", "value": 65}
  ],
  "then": {"type": "fixed", "value": "Senior"},
  "else": {"type": "fixed", "value": "Regular"}
}
```

#### AI-Generated Rules
```json
{
  "type": "ai_generated",
  "description": "Generate realistic product names for electronics",
  "max_length": 50
}
```

---

## Data Types

### Core Data Types

| Type | Aliases | Description | Compatible Rules | Example Values |
|------|---------|-------------|------------------|----------------|
| **int** | integer | Whole numbers | range, sequence, choice | 1, 42, -15 |
| **float** | double, decimal | Decimal numbers | range, choice | 3.14, -2.5 |
| **str** | string, text | Text data | choice, email, phone, regex | "Hello", "John Doe" |
| **bool** | boolean | True/false | choice, fixed | true, false |
| **date** | - | Calendar dates | date, date_range | 2023-12-25 |
| **datetime** | timestamp | Date and time | timestamp, timestamp_range | 2023-12-25 14:30:45 |
| **uuid** | - | Unique identifiers | uuid, choice | 123e4567-e89b-12d3... |

### Extended Numeric Types

| Type | Size | Range | Capacity | Use Case |
|------|------|-------|----------|----------|
| **tinyint** | 1 byte | -128 to 127 | 256 values | Small numbers, flags |
| **smallint** | 2 bytes | -32,768 to 32,767 | 65,536 values | Medium range |
| **bigint** | 8 bytes | Very large range | 2^63 values | Large IDs |

### Auto-Detected Types

The system can automatically detect and handle:
- **Email patterns** in field names
- **Phone patterns** in field names  
- **SSN patterns** in field names
- **Credit card patterns** in field names

---

## Constraints and Relationships

### Primary Constraints

| Constraint | Purpose | Configuration | Compatible Types |
|------------|---------|---------------|------------------|
| **PK** | Primary Key | `"constraints": ["PK"]` | int, str, uuid |
| **unique** | Unique values | `"constraints": ["unique"]` | any |
| **nullable** | Allow nulls | `"nullable": true` | any |

### Length Constraints

#### Simple Length
```json
{"name": "code", "type": "str", "length": 10}
```

#### Range Length
```json
{"name": "description", "type": "str", "length": {"min": 5, "max": 20}}
```

#### Auto-Conversion for Integers
```json
{"name": "id", "type": "int", "length": 5}
```
**Automatically converts to:**
```json
{"name": "id", "type": "int", "rule": {"type": "range", "min": 10000, "max": 99999}}
```

### Foreign Key Relationships

```json
{
  "foreign_keys": [{
    "parent_table": "users",
    "parent_column": "id", 
    "child_column": "user_id",
    "relationship_type": "one_to_many"
  }]
}
```

**Relationship Types:**
- **one_to_many**: One parent, multiple children
- **one_to_one**: One parent, one child
- **many_to_many**: Complex relationships

---

## Faker Providers

### Personal Data

| Provider | Output Example | Use Case |
|----------|----------------|----------|
| name | John Smith | Full names |
| first_name | John | Given names |
| last_name | Smith | Surnames |
| email | john@example.com | User accounts |
| phone_number | +1-555-123-4567 | Contact info |
| address | 123 Main St, City | Mailing addresses |

### Company Data

| Provider | Output Example | Use Case |
|----------|----------------|----------|
| company | Acme Corporation | Business names |
| job | Software Engineer | Job titles |
| department | Engineering | Org structure |
| catch_phrase | Innovative solutions | Slogans |

### Location Data

| Provider | Output Example | Use Case |
|----------|----------------|----------|
| city | New York | Location data |
| country | United States | Geographic info |
| zipcode | 12345 | Postal codes |
| latitude | 40.7128 | Coordinates |
| longitude | -74.0060 | Coordinates |

### Financial Data

| Provider | Output Example | Use Case |
|----------|----------------|----------|
| credit_card_number | 4532-1234-5678-9012 | Payment testing |
| iban | GB82 WEST 1234... | Banking |
| currency_code | USD | Financial data |

### Internet Data

| Provider | Output Example | Use Case |
|----------|----------------|----------|
| url | https://example.com | Web data |
| domain_name | example.com | Domains |
| ipv4 | 192.168.1.1 | Network data |
| mac_address | 02:00:00:00:00:00 | Hardware IDs |

### Automotive Data

| Provider | Output Example | Use Case |
|----------|----------------|----------|
| license_plate | ABC-1234 | Vehicle data |
| vehicle_make | Toyota | Car brands |
| vehicle_model | Camry | Car models |
| vin | 1HGBH41JXMN109186 | Vehicle IDs |

---

## AI Features

### Supported Providers

#### OpenAI Integration
```json
{
  "ai": {
    "openai": {
      "enabled": true,
      "api_key": "sk-...",
      "model": "gpt-3.5-turbo",
      "temperature": 0.7,
      "max_tokens": 2000,
      "cost_limit_usd": 10.0
    }
  }
}
```

#### Mistral AI Integration
```json
{
  "ai": {
    "mistral": {
      "enabled": true,
      "api_key": "...",
      "model": "mistral-small",
      "temperature": 0.7,
      "max_tokens": 2000,
      "cost_limit_usd": 8.0
    }
  }
}
```

### Multi-Provider Setup
```json
{
  "ai": {
    "primary_provider": "openai",
    "enable_fallback": true,
    "openai": {...},
    "mistral": {...}
  }
}
```

### AI Rule Configuration
```json
{
  "type": "ai_generated",
  "description": "Generate realistic tech startup names",
  "context_columns": ["industry", "target_market"],
  "max_length": 50
}
```

### Cost Management
- **Cost Limits**: Prevent overspending
- **Caching**: Reduce repeated API calls
- **Fallback**: Use secondary provider if primary fails
- **Monitoring**: Track usage and costs

---

## Security Features

### Data Masking

#### Automatic PII Detection
```json
{
  "security": {
    "enable_data_masking": true,
    "auto_detect_pii": true
  }
}
```

#### Custom Masking Rules
```json
{
  "security": {
    "masking_rules": {
      "email": "partial",
      "ssn": "full", 
      "salary": "hash"
    }
  }
}
```

#### Masking Types
- **partial**: Show some characters (j***@example.com)
- **full**: Replace all (***-**-****)
- **hash**: Irreversible hash (a7b2c9d1)
- **format_preserving**: Keep format, change values

### Sensitivity Levels
- **PUBLIC**: No protection needed
- **INTERNAL**: Company confidential
- **PII**: Personally identifiable information
- **SENSITIVE**: Highly confidential

### Compliance Profiles

#### GDPR Compliance
```json
{
  "security": {
    "compliance_profile": "GDPR",
    "strict_mode": true,
    "audit_enabled": true
  }
}
```

#### HIPAA Compliance
```json
{
  "security": {
    "compliance_profile": "HIPAA",
    "enable_encryption": true,
    "min_masking_percentage": 99.0
  }
}
```

#### Other Profiles
- **PCI_DSS**: Payment card data
- **SOX**: Financial data (7-year retention)

### Encryption
```json
{
  "security": {
    "enable_encryption": true,
    "encryption_algorithm": "AES-256-GCM",
    "encrypt_fields": ["ssn", "credit_card"]
  }
}
```

### Audit Trail
```json
{
  "security": {
    "audit_enabled": true,
    "audit_log_level": "detailed",
    "retention_days": 90
  }
}
```

---

## Performance Features

### Streaming Mode
**For large datasets (>100K records):**
```json
{
  "performance": {
    "enable_streaming": true,
    "streaming_batch_size": 1000
  }
}
```

**Benefits:**
- Constant memory usage
- Handle datasets larger than RAM
- Real-time file writing

### Parallel Processing
**For medium datasets (10K-100K records):**
```json
{
  "performance": {
    "enable_parallel": true,
    "max_workers": 4
  }
}
```

**Options:**
- **Thread-based**: Good for I/O operations
- **Process-based**: Better for CPU-intensive tasks

### Memory Management
```json
{
  "performance": {
    "max_memory_mb": 1000,
    "cache_size_limit": 50000
  }
}
```

**Features:**
- Automatic memory monitoring
- Cleanup callbacks
- Memory threshold alerts

### Adaptive Strategy
```json
{
  "performance": {
    "enable_streaming": true,
    "enable_parallel": true
  }
}
```

**System automatically chooses:**
- **Small datasets**: Standard generation
- **Medium datasets**: Parallel processing
- **Large datasets**: Streaming mode

---

## Advanced Features

### External File Integration

#### CSV Choice Files
```json
{
  "type": "choice_file",
  "file_path": "products.csv",
  "column": "product_name",
  "weight_column": "popularity"
}
```

#### Excel Choice Files
```json
{
  "type": "choice_file", 
  "file_path": "data.xlsx",
  "sheet_name": "Products",
  "column": "category"
}
```

### Advanced Validation

#### Business Rules
```json
{
  "validation": {
    "business_rules": [{
      "type": "conditional",
      "condition_column": "age",
      "condition_operator": ">",
      "condition_value": 65,
      "requirement_column": "plan_type",
      "requirement_value": "Senior"
    }]
  }
}
```

#### Quality Analysis
```json
{
  "validation": {
    "enable_data_quality_analysis": true,
    "quality_threshold": 0.8,
    "enable_anomaly_detection": true
  }
}
```

### Multi-Format Export
```json
{
  "output": {
    "formats": ["csv", "json", "excel"]
  }
}
```

---

## Configuration Examples

### E-commerce System
```json
{
  "tables": [{
    "table_name": "products",
    "columns": [
      {"name": "id", "type": "int", "constraints": ["PK"]},
      {"name": "name", "type": "str", "rule": {"type": "ai_generated", "description": "Product names"}},
      {"name": "price", "type": "float", "rule": {"type": "range", "min": 9.99, "max": 999.99}},
      {"name": "category", "type": "str", "rule": {"type": "choice", "value": ["Electronics", "Clothing"]}}
    ]
  }],
  "rows": 10000,
  "output": {"format": "csv"},
  "ai": {"openai": {"enabled": true}}
}
```

### HR System
```json
{
  "tables": [{
    "table_name": "employees", 
    "columns": [
      {"name": "employee_id", "type": "str", "rule": {"type": "sequence", "prefix": "EMP", "start": 1000}},
      {"name": "first_name", "type": "str", "rule": "first_name"},
      {"name": "salary", "type": "int", "rule": {"type": "range", "min": 40000, "max": 150000}, "sensitivity": "SENSITIVE"}
    ]
  }],
  "security": {"enable_data_masking": true}
}
```

### Financial System
```json
{
  "tables": [{
    "table_name": "accounts",
    "columns": [
      {"name": "account_number", "type": "str", "rule": {"type": "regex", "regex": "^ACC-\\d{8}$"}},
      {"name": "ssn", "type": "str", "rule": {"type": "regex", "regex": "^\\d{3}-\\d{2}-\\d{4}$"}, "sensitivity": "SENSITIVE"}
    ]
  }],
  "security": {"compliance_profile": "PCI_DSS"}
}
```

---

## Environment Variables

### Basic Configuration
| Variable | Purpose | Example | Target |
|----------|---------|---------|--------|
| DG_ROWS | Number of rows | 10000 | config.rows |
| DG_OUTPUT_FORMAT | Output format | parquet | config.output.format |
| DG_OUTPUT_DIR | Output directory | ./prod_output | config.output.directory |
| DG_MAX_WORKERS | Worker threads | 8 | config.performance.max_workers |

### AI Configuration
| Variable | Purpose | Example | Target |
|----------|---------|---------|--------|
| OPENAI_API_KEY | OpenAI key | sk-... | config.ai.openai.api_key |
| DG_OPENAI_MODEL | OpenAI model | gpt-4 | config.ai.openai.model |
| MISTRAL_API_KEY | Mistral key | ... | config.ai.mistral.api_key |
| DG_AI_PRIMARY_PROVIDER | Primary AI | mistral | config.ai.primary_provider |

### Security Configuration
| Variable | Purpose | Example | Target |
|----------|---------|---------|--------|
| DG_ENABLE_MASKING | Data masking | true | config.security.enable_data_masking |
| DG_COMPLIANCE_PROFILE | Compliance | GDPR | config.security.compliance_profile |
| DATA_ENCRYPTION_KEY | Encryption key | 32-byte-key | config.security.encryption_key |

### Performance Configuration
| Variable | Purpose | Example | Target |
|----------|---------|---------|--------|
| DG_ENABLE_STREAMING | Streaming mode | true | config.performance.enable_streaming |
| DG_MAX_MEMORY_MB | Memory limit | 2048 | config.performance.max_memory_mb |
| DG_BATCH_SIZE | Batch size | 50000 | config.performance.batch_size |

---

## Troubleshooting

### Common Issues

#### Schema Validation Errors
**Problem**: Critical errors prevent generation
**Solution**: Fix schema validation errors before running
**Prevention**: Use schema validation before generation

#### AI API Issues
**Problem**: AI generation fails
**Causes**: Missing API key, rate limiting, cost limits
**Solutions**: 
- Set API key in environment
- Increase cost limits
- Enable fallback provider

#### Memory Issues  
**Problem**: Out of memory crashes
**Solution**: Enable streaming mode or reduce batch size
**Prevention**: Monitor memory usage, use streaming for large datasets

#### Performance Issues
**Problem**: Slow generation
**Solutions**:
- Enable parallel processing
- Optimize rules and constraints
- Use appropriate batch sizes

#### Output Format Issues
**Problem**: Cannot write files
**Causes**: Permission issues, invalid format
**Solutions**:
- Check file permissions
- Verify format spelling
- Install required libraries

### Error Categories

| Category | Symptoms | Common Causes | Solutions |
|----------|----------|---------------|-----------|
| **Schema** | Generation fails to start | Invalid schema structure | Fix validation errors |
| **Memory** | System crashes | Dataset too large | Enable streaming |
| **Performance** | Very slow generation | Poor configuration | Enable parallel processing |
| **AI** | AI requests fail | API issues | Check API key, limits |
| **Output** | Cannot create files | Permission/format issues | Check permissions, formats |

---

## Feature Compatibility Matrix

### Format Support Matrix

| Feature | CSV | JSON | Excel | Parquet | Fixed | SQL |
|---------|-----|------|-------|---------|-------|-----|
| **Basic Output** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Fixed-Width** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Compression** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Streaming** | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ |
| **Large Datasets** | ✅ | ✅ | ⚠️ | ✅ | ✅ | ✅ |
| **Analytics** | ⚠️ | ⚠️ | ⚠️ | ✅ | ❌ | ❌ |

### Rule Compatibility Matrix

| Rule Type | int | str | bool | date | float |
|-----------|-----|-----|------|------|-------|
| **choice** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **range** | ✅ | ❌ | ❌ | ❌ | ✅ |
| **sequence** | ✅ | ❌ | ❌ | ❌ | ❌ |
| **email** | ❌ | ✅ | ❌ | ❌ | ❌ |
| **regex** | ❌ | ✅ | ❌ | ❌ | ❌ |
| **date_range** | ❌ | ✅ | ❌ | ✅ | ❌ |

---

## Appendix

### Version Information
- **Version**: 2.0
- **Last Updated**: December 2024
- **Compatibility**: Python 3.7+

### Dependencies
- **Required**: pandas, faker
- **Optional**: openpyxl (Excel), pyarrow (Parquet), openai, mistralai

### Support Resources
- **Documentation**: This manual
- **Examples**: Configuration examples section
- **Troubleshooting**: Troubleshooting section

### License
See project license file for terms and conditions.

---

*This manual provides comprehensive coverage of all data generator features. For the most up-to-date information, refer to the project repository.*