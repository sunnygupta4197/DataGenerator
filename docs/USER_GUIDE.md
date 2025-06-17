# User Guide - Enhanced Data Generator

## 🎯 What is the Enhanced Data Generator?

The Enhanced Data Generator is a powerful tool that creates realistic, synthetic data for your business needs. Whether you're testing applications, training machine learning models, or need sample data for demos, this tool generates high-quality, customizable datasets that look and behave like real data without compromising privacy or security.

## 💡 Why Use Synthetic Data?

### **Privacy Protection** 🔒
- No real customer data at risk
- GDPR and HIPAA compliant
- Safe for development and testing environments

### **Cost Effective** 💰
- No need to purchase expensive datasets
- Generate unlimited amounts of data
- Reduce development time and costs

### **Customizable** ⚙️
- Generate exactly the data you need
- Control data relationships and patterns
- Scale from small samples to millions of records

### **Always Available** ⚡
- Generate fresh data anytime
- No dependency on production systems
- Perfect for continuous integration testing

## 🚀 Getting Started

### What You'll Need
- A computer with Python installed (we'll help you with this)
- Basic understanding of your data requirements
- About 15 minutes to get started

### Quick Start (No Programming Required!)

#### Option 1: Simple Command Line
1. **Download** the data generator to your computer
2. **Open** your command prompt or terminal
3. **Run** this simple command:
```bash
python main.py --config examples/basic_config.json --rows 1000
```
4. **Find** your generated data in the `output` folder!

#### Option 2: Interactive Setup Wizard
1. **Run** the setup wizard:
```bash
python setup_wizard.py
```
2. **Answer** simple questions about your data needs
3. **Let** the wizard create your configuration
4. **Generate** your data automatically!

## 📊 Understanding Your Data Needs

### Basic Data Types

| Data Type | What It Is | Examples |
|-----------|------------|----------|
| **Names** | People's names | John Smith, Sarah Johnson |
| **Emails** | Email addresses | john@company.com, sarah.j@email.org |
| **Phones** | Phone numbers | +1-555-123-4567, (555) 987-6543 |
| **Addresses** | Street addresses | 123 Main St, Anytown, NY 12345 |
| **Dates** | Calendar dates | 2024-01-15, March 3, 2023 |
| **Numbers** | Quantities, IDs, prices | 42, 1001, $29.99 |
| **Categories** | Fixed choices | Gold/Silver/Bronze, Active/Inactive |

### Common Business Scenarios

#### **Customer Database**
Generate customer information including:
- Customer IDs and names
- Contact information (email, phone)
- Demographics (age, location)
- Preferences and categories

#### **Product Catalog**
Create product data with:
- Product codes and names
- Prices and categories
- Descriptions and specifications
- Inventory levels

#### **Sales Records**
Generate transaction data:
- Order numbers and dates
- Customer and product links
- Quantities and amounts
- Sales regions and representatives

#### **Employee Data**
Create HR datasets with:
- Employee IDs and personal info
- Job titles and departments
- Salary and performance data
- Start dates and status

## ⚙️ Configuring Your Data

### The Simple Way: Using Examples

We provide ready-to-use examples for common scenarios:

```bash
# Generate customer data
python main.py --config examples/customers.json --rows 5000

# Generate product catalog
python main.py --config examples/products.json --rows 1000

# Generate sales transactions
python main.py --config examples/sales.json --rows 10000
```

### The Custom Way: Configuration Files

Configuration files tell the generator what kind of data to create. Here's a simple example:

```json
{
  "tables": [
    {
      "table_name": "customers",
      "columns": [
        {
          "name": "customer_id",
          "type": "number",
          "rule": "unique_sequence_starting_from_1000"
        },
        {
          "name": "first_name",
          "type": "text",
          "rule": "realistic_first_names"
        },
        {
          "name": "email",
          "type": "text",
          "rule": "valid_email_addresses"
        },
        {
          "name": "age",
          "type": "number",
          "rule": "age_between_18_and_80"
        }
      ]
    }
  ],
  "rows": 1000
}
```

**What This Does:**
- Creates a customer table with 1,000 records
- Each customer gets a unique ID starting from 1000
- Names are realistic (John, Sarah, Michael, etc.)
- Emails are properly formatted (john.smith@email.com)
- Ages are realistic (between 18 and 80)

## 🎛️ Customization Options

### Data Size Options

| Option | Records | Best For | Time |
|--------|---------|----------|------|
| **Small** | 1-1,000 | Testing, demos | Seconds |
| **Medium** | 1,000-100,000 | Development, training | Minutes |
| **Large** | 100,000-1M+ | Production testing | Hours |

### Output Formats

#### **Excel Spreadsheets** (.xlsx)
- Perfect for business users
- Easy to open and analyze
- Good for presentations and reports

#### **CSV Files** (.csv)
- Universal format
- Works with any database or tool
- Lightweight and fast

#### **JSON Data** (.json)
- Modern web applications
- API testing and development
- Structured data exchange

### Quality Levels

#### **Basic Quality**
- Fast generation
- Simple data validation
- Good for quick testing

#### **High Quality**
- Realistic data patterns
- Advanced validation
- Business rule compliance
- Better for demos and training

#### **Production Quality**
- Maximum realism
- Full security compliance
- Comprehensive validation
- Audit trails and reporting

## 🔒 Security and Privacy Features

### Data Masking
Automatically protects sensitive information:
- **Email masking**: user@example.com → u***@example.com
- **Phone masking**: +1-555-123-4567 → +1-555-***-***7
- **Name masking**: John Smith → J*** S****

### Compliance Features
- **GDPR Ready**: No real personal data used
- **HIPAA Compliant**: Healthcare data protection
- **Audit Trails**: Track all data generation activities
- **Encryption**: Protect sensitive generated data

### Safe by Design
- No real data ever used as source
- All generated data is completely synthetic
- Safe for sharing with third parties
- No privacy risks or data breaches

## 🎯 Common Use Cases

### Application Testing
**Challenge**: Need test data without using real customer information
**Solution**: Generate realistic customer, order, and product data
```bash
python main.py --config examples/app_testing.json --rows 5000
```

### Demo Presentations
**Challenge**: Need impressive, realistic data for sales demos
**Solution**: High-quality data with consistent relationships
```bash
python main.py --config examples/demo_data.json --enable_quality_analysis --rows 1000
```

### Machine Learning Training
**Challenge**: Need large datasets for training AI models
**Solution**: Generate millions of records with controlled patterns
```bash
python main.py --config examples/ml_training.json --rows 1000000 --enable_streaming
```

### Database Performance Testing
**Challenge**: Test database performance with realistic data volumes
**Solution**: Generate large-scale data with proper relationships
```bash
python main.py --config examples/performance_test.json --rows 5000000 --enable_parallel
```

### Compliance Testing
**Challenge**: Test data privacy features without real data
**Solution**: Generate data with built-in masking and encryption
```bash
python main.py --config examples/compliance.json --enable_masking --enable_encryption
```

## 🛠️ Available Tools and Features

### Command Line Options

#### Basic Options
```bash
--config FILE          # Your configuration file
--rows NUMBER          # How many records to generate
--output_dir FOLDER    # Where to save the files
--format FORMAT        # Output format (csv, excel, json)
```

#### Quality Options
```bash
--enable_quality_analysis    # Check data quality
--enable_anomaly_detection   # Find unusual patterns
--strict_validation         # Extra thorough checking
```

#### Performance Options
```bash
--enable_streaming          # Handle very large datasets
--enable_parallel          # Use multiple processors
--max_workers NUMBER       # How many processors to use
--max_memory NUMBER        # Memory limit in MB
```

#### Security Options
```bash
--enable_masking           # Protect sensitive data
--enable_encryption        # Encrypt sensitive fields
--audit_enabled           # Track all activities
```

### Interactive Features

#### Configuration Wizard
Creates configurations through simple questions:
```bash
python config_wizard.py
```
**Asks questions like:**
- What type of business data do you need?
- How many records do you want?
- What format should the output be?
- Do you need any special security features?

#### Data Preview
See a sample of your data before generating the full dataset:
```bash
python preview_data.py --config your_config.json --sample 10
```

#### Quality Reports
Get detailed reports about your generated data:
```bash
python analyze_quality.py --input your_data.csv
```

## 📈 Performance and Optimization

### Small Datasets (Under 10,000 records)
- **Generation Time**: Seconds
- **Memory Used**: Minimal
- **Recommended Settings**: Default settings work perfectly

### Medium Datasets (10,000 - 100,000 records)
- **Generation Time**: Minutes
- **Memory Used**: Moderate
- **Recommended Settings**: 
```bash
--enable_parallel --max_workers 4
```

### Large Datasets (100,000+ records)
- **Generation Time**: Hours
- **Memory Used**: Controlled
- **Recommended Settings**:
```bash
--enable_streaming --enable_parallel --max_memory 2000
```

### Very Large Datasets (1 Million+ records)
- **Generation Time**: Multiple hours
- **Memory Used**: Minimal (streaming mode)
- **Recommended Settings**:
```bash
--enable_streaming --max_memory 1000 --format parquet
```

## 📋 Step-by-Step Tutorials

### Tutorial 1: Your First Dataset

**Goal**: Create a simple customer list with 100 customers

#### Step 1: Create Configuration
Save this as `my_first_config.json`:
```json
{
  "tables": [
    {
      "table_name": "customers",
      "columns": [
        {
          "name": "id",
          "type": "int",
          "rule": {"type": "range", "min": 1, "max": 99999}
        },
        {
          "name": "name",
          "type": "str",
          "rule": "name"
        },
        {
          "name": "email",
          "type": "str",
          "rule": {"type": "email"}
        }
      ]
    }
  ],
  "rows": 100
}
```

#### Step 2: Generate Data
```bash
python main.py --config my_first_config.json
```

#### Step 3: Check Results
Look in the `output` folder for `customers.csv`

**What You'll See:**
```
id,name,email
1,John Smith,john.smith@email.com
2,Sarah Johnson,sarah.johnson@gmail.com
3,Michael Brown,michael.brown@company.org
...
```

### Tutorial 2: E-commerce Product Catalog

**Goal**: Create a product catalog with categories and prices

#### Step 1: Configuration File
Save as `products_config.json`:
```json
{
  "tables": [
    {
      "table_name": "products",
      "columns": [
        {
          "name": "product_id",
          "type": "str",
          "rule": {
            "prefix": "PROD-",
            "type": "range",
            "min": 1000,
            "max": 9999
          }
        },
        {
          "name": "product_name",
          "type": "str",
          "rule": {
            "type": "choice",
            "value": [
              "Wireless Headphones", "Bluetooth Speaker", "Smart Watch",
              "Laptop Stand", "USB Cable", "Phone Case", "Tablet Cover",
              "Wireless Mouse", "Keyboard", "Monitor"
            ]
          }
        },
        {
          "name": "category",
          "type": "str",
          "rule": {
            "type": "choice",
            "value": ["Electronics", "Accessories", "Computing"]
          }
        },
        {
          "name": "price",
          "type": "float",
          "rule": {"type": "range", "min": 9.99, "max": 299.99}
        },
        {
          "name": "in_stock",
          "type": "bool",
          "rule": {
            "type": "choice",
            "value": [true, false],
            "probabilities": {"true": 0.8, "false": 0.2}
          }
        }
      ]
    }
  ],
  "rows": 500
}
```

#### Step 2: Generate with Quality Analysis
```bash
python main.py --config products_config.json --enable_quality_analysis
```

#### Step 3: Results
You'll get a detailed product catalog with:
- Unique product IDs (PROD-1234)
- Realistic product names
- Proper categories
- Reasonable prices
- Stock status (80% in stock, 20% out of stock)

### Tutorial 3: Customer and Orders (Related Data)

**Goal**: Create customers and their orders with proper relationships

#### Step 1: Advanced Configuration
Save as `customer_orders_config.json`:
```json
{
  "tables": [
    {
      "table_name": "customers",
      "columns": [
        {
          "name": "customer_id",
          "type": "int",
          "constraint": ["PK"],
          "rule": {"type": "range", "min": 1000, "max": 9999}
        },
        {
          "name": "first_name",
          "type": "str",
          "rule": "first_name"
        },
        {
          "name": "last_name",
          "type": "str",
          "rule": "last_name"
        },
        {
          "name": "email",
          "type": "str",
          "rule": {"type": "email"}
        },
        {
          "name": "registration_date",
          "type": "date",
          "rule": {
            "type": "date_range",
            "start": "2020-01-01",
            "end": "2024-12-31"
          }
        }
      ]
    },
    {
      "table_name": "orders",
      "columns": [
        {
          "name": "order_id",
          "type": "int",
          "constraint": ["PK"],
          "rule": {"type": "range", "min": 10000, "max": 99999}
        },
        {
          "name": "customer_id",
          "type": "int",
          "nullable": false
        },
        {
          "name": "order_date",
          "type": "date",
          "rule": {
            "type": "date_range",
            "start": "2024-01-01",
            "end": "2024-12-31"
          }
        },
        {
          "name": "total_amount",
          "type": "float",
          "rule": {"type": "range", "min": 25.00, "max": 500.00}
        },
        {
          "name": "status",
          "type": "str",
          "rule": {
            "type": "choice",
            "value": ["Pending", "Shipped", "Delivered", "Cancelled"],
            "probabilities": {
              "Pending": 0.1,
              "Shipped": 0.2,
              "Delivered": 0.6,
              "Cancelled": 0.1
            }
          }
        }
      ],
      "foreign_keys": [
        {
          "parent_table": "customers",
          "parent_column": "customer_id",
          "child_column": "customer_id",
          "relationship_type": "many_to_one"
        }
      ]
    }
  ],
  "rows": 1000
}
```

#### Step 2: Generate Related Data
```bash
python main.py --config customer_orders_config.json --enable_parallel
```

#### Step 3: Verify Relationships
The generator ensures that:
- Every order belongs to an existing customer
- Customer IDs match between tables
- Dates are logical (orders after registration)
- Realistic distribution of order statuses

## 🎨 Customization Examples

### Realistic Business Rules

#### Age-Dependent Pricing
```json
{
  "name": "insurance_premium",
  "type": "float",
  "conditional_rules": [
    {
      "when": [{"column": "age", "operator": "<", "value": 25}],
      "then": {"rule": {"type": "range", "min": 150.00, "max": 300.00}}
    },
    {
      "when": [{"column": "age", "operator": ">", "value": 65}],
      "then": {"rule": {"type": "range", "min": 200.00, "max": 400.00}}
    }
  ]
}
```

#### Geographic Data Consistency
```json
{
  "name": "zip_code",
  "type": "str",
  "conditional_rules": [
    {
      "when": [{"column": "state", "operator": "equals", "value": "CA"}],
      "then": {"rule": {"type": "range", "min": 90000, "max": 96199}}
    },
    {
      "when": [{"column": "state", "operator": "equals", "value": "NY"}],
      "then": {"rule": {"type": "range", "min": 10000, "max": 14999}}
    }
  ]
}
```

### Loading Choices from Files

One of the most powerful features is the ability to load realistic choices from your own CSV or Excel files.

#### Why Use Files for Choices?
- **Use Your Own Data**: Load real category names, product lists, or location data
- **Maintain Accuracy**: Keep choices up-to-date with your business
- **Weight Distribution**: Control how often certain choices appear
- **Large Lists**: Handle hundreds or thousands of choices efficiently

#### Basic File Usage
Create a CSV file called `cities.csv`:
```csv
city_name,population
New York,8400000
Los Angeles,3900000
Chicago,2700000
Houston,2300000
Phoenix,1680000
```

Use in your configuration:
```json
{
  "name": "customer_city",
  "type": "str",
  "rule": {
    "type": "choice_from_file",
    "file_path": "data/cities.csv",
    "column": "city_name"
  }
}
```

#### Weighted Choices from Files
To make larger cities appear more frequently:
```json
{
  "name": "customer_city", 
  "type": "str",
  "rule": {
    "type": "choice_from_file",
    "file_path": "data/cities.csv",
    "column": "city_name",
    "weight_column": "population"
  }
}
```

This will make New York appear much more often than Phoenix, proportional to population.

#### Practical Examples

**Product Categories** (`product_categories.csv`):
```csv
category,sales_frequency
Electronics,45
Clothing,30
Home & Garden,15
Sports,10
```

**Job Titles** (`job_titles.xlsx`):
```csv
title,department,salary_range
Software Engineer,Engineering,High
Marketing Manager,Marketing,Medium
Sales Associate,Sales,Low
Data Analyst,Analytics,Medium
```

**Configuration Usage**:
```json
{
  "columns": [
    {
      "name": "product_category",
      "type": "str", 
      "rule": {
        "type": "choice_from_file",
        "file_path": "data/product_categories.csv",
        "column": "category",
        "weight_column": "sales_frequency"
      }
    },
    {
      "name": "job_title",
      "type": "str",
      "rule": {
        "type": "choice_from_file",
        "file_path": "data/job_titles.xlsx", 
        "column": "title"
      }
    }
  ]
}
```

#### Supported File Formats
- **CSV files** (`.csv`) - Most common and reliable
- **Excel files** (`.xlsx`, `.xls`) - Good for business users
- **Tab-separated** files - Specify delimiter in advanced options

#### File Structure Tips
- **First row should be headers** (column names)
- **Use clear column names** for easy reference
- **Remove empty rows** for best results
- **Keep files in a `data/` folder** for organization

#### Advanced File Options
```json
{
  "rule": {
    "type": "choice_from_file",
    "file_path": "data/international_cities.csv",
    "column": "city_name",
    "weight_column": "population",
    "encoding": "utf-8",
    "delimiter": ",",
    "skip_rows": 1
  }
}
```

This feature is perfect for:
- **Company-specific data**: Use your actual product catalogs, customer segments, etc.
- **Geographic data**: Real city, state, country lists with proper distributions
- **Industry-specific terms**: Medical codes, financial instruments, legal terminology
- **Seasonal data**: Adjust weights based on time of year or business cycles

### Industry-Specific Examples

#### Healthcare Data
```json
{
  "tables": [
    {
      "table_name": "patients",
      "columns": [
        {
          "name": "patient_id",
          "type": "str",
          "rule": {"prefix": "PT-", "type": "range", "min": 100000, "max": 999999}
        },
        {
          "name": "age",
          "type": "int",
          "rule": {"type": "range", "min": 0, "max": 100}
        },
        {
          "name": "diagnosis_code",
          "type": "str",
          "rule": {
            "type": "choice_from_file",
            "file_path": "data/icd10_codes.csv",
            "column": "code",
            "weight_column": "frequency"
          }
        }
      ]
    }
  ]
}
```

#### Financial Services
```json
{
  "tables": [
    {
      "table_name": "accounts",
      "columns": [
        {
          "name": "account_number",
          "type": "str",
          "rule": {"type": "range", "min": 1000000000, "max": 9999999999}
        },
        {
          "name": "account_type",
          "type": "str",
          "rule": {
            "type": "choice",
            "value": ["Checking", "Savings", "Credit", "Investment"]
          }
        },
        {
          "name": "branch_location",
          "type": "str", 
          "rule": {
            "type": "choice_from_file",
            "file_path": "data/bank_branches.csv",
            "column": "branch_name",
            "weight_column": "customer_count"
          }
        },
        {
          "name": "balance",
          "type": "float",
          "rule": {"type": "range", "min": 0.00, "max": 100000.00}
        }
      ]
    }
  ]
}
```

## 🔍 Quality Control

### Automatic Quality Checks
When you use `--enable_quality_analysis`, the system automatically checks:

#### **Data Consistency**
- All required fields are filled
- Data types are correct
- Relationships are maintained

#### **Realistic Patterns**
- Names look like real names
- Emails have proper format
- Phone numbers follow regional patterns
- Dates are in logical ranges

#### **Business Logic**
- Prices are reasonable
- Ages make sense
- Geographic data is consistent
- Time sequences are logical

### Quality Reports
After generation, you'll see reports like:

```
📊 Data Quality Report
===================
Overall Score: 0.95/1.00 (Excellent)

✅ Email Format: 100% valid
✅ Phone Numbers: 98% valid (US format)
✅ Date Ranges: 100% within specified bounds
⚠️  Price Distribution: Some outliers detected
✅ Foreign Keys: 100% valid relationships

Recommendations:
- Consider adjusting price range limits
- Review price generation rules for more realistic distribution
```

### Common Quality Issues and Solutions

#### Issue: Unrealistic Names
**Problem**: Getting names like "Xxx Yyy"
**Solution**: Use built-in name generators:
```json
{"name": "first_name", "type": "str", "rule": "first_name"}
```

#### Issue: Invalid Email Formats
**Problem**: Emails don't look realistic
**Solution**: Use email rule with domain options:
```json
{
  "name": "email",
  "type": "str",
  "rule": {"type": "email", "domains": ["gmail.com", "yahoo.com", "company.com"]}
}
```

#### Issue: Inconsistent Relationships
**Problem**: Orders reference non-existent customers
**Solution**: Define proper foreign keys:
```json
{
  "foreign_keys": [
    {
      "parent_table": "customers",
      "parent_column": "customer_id",
      "child_column": "customer_id"
    }
  ]
}
```

## 🚨 Troubleshooting

### Common Problems and Solutions

#### Problem: "Memory Error"
**Symptoms**: Program crashes with memory error
**Solutions**:
1. Reduce batch size: `--max_memory 500`
2. Enable streaming: `--enable_streaming`
3. Generate smaller datasets first

#### Problem: "Generation Too Slow"
**Symptoms**: Taking too long to generate data
**Solutions**:
1. Enable parallel processing: `--enable_parallel`
2. Increase worker count: `--max_workers 6`
3. Disable quality analysis for faster generation

#### Problem: "Invalid Configuration"
**Symptoms**: Error messages about configuration format
**Solutions**:
1. Check JSON syntax (commas, brackets, quotes)
2. Use the configuration wizard: `python config_wizard.py`
3. Start with example configurations

#### Problem: "Unrealistic Data"
**Symptoms**: Generated data doesn't look realistic
**Solutions**:
1. Enable quality analysis: `--enable_quality_analysis`
2. Use built-in realistic rules (name, email, phone)
3. Add conditional rules for business logic

#### Problem: "Missing Foreign Key Data"
**Symptoms**: Relationships between tables are broken
**Solutions**:
1. Generate parent tables first
2. Check foreign key configuration
3. Ensure parent table has primary keys

### Getting Help

#### Check the Examples
Look in the `examples/` folder for:
- `basic_customer.json` - Simple customer data
- `ecommerce.json` - Product catalog and orders
- `healthcare.json` - Medical records (anonymized)
- `financial.json` - Banking and transaction data

#### Enable Debug Mode
For detailed information about what's happening:
```bash
python main.py --config your_config.json --log_level DEBUG
```

#### Generate Sample Data First
Test your configuration with a small sample:
```bash
python main.py --config your_config.json --rows 10
```

## 📞 Support and Resources

### Documentation
- **Configuration Reference**: Details on all available options
- **API Documentation**: For programmatic usage
- **Best Practices Guide**: Tips for optimal results
- **Security Guide**: Privacy and compliance information

### Community Resources
- **Example Library**: Ready-to-use configurations for common scenarios
- **Video Tutorials**: Step-by-step visual guides
- **FAQ Database**: Answers to common questions
- **User Forum**: Community support and discussions

### Professional Support
- **Consultation Services**: Help designing your data generation strategy
- **Custom Development**: Specialized generators for unique requirements
- **Training Programs**: Learn to use advanced features effectively
- **Enterprise Support**: Priority support for business-critical usage

## 🎯 Best Practices

### Planning Your Data Generation

#### 1. Start Small
- Begin with 100-1,000 records
- Test your configuration
- Verify data quality
- Scale up gradually

#### 2. Define Relationships First
- Identify which tables depend on others
- Plan your foreign key relationships
- Consider the order of generation

#### 3. Consider Your Use Case
- **Testing**: Focus on edge cases and boundary conditions
- **Demos**: Emphasize realistic, impressive data
- **Training**: Ensure statistical accuracy and completeness
- **Development**: Prioritize speed and consistency

#### 4. Plan for Performance
- Large datasets: Use streaming mode
- Complex relationships: Enable parallel processing
- Memory constraints: Adjust batch sizes
- Time constraints: Disable quality analysis

### Security Best Practices

#### 1. Never Use Real Data
- Always generate synthetic data
- Don't base generation on real datasets
- Use completely artificial patterns and rules

#### 2. Enable Security Features
- Use data masking for sensitive fields
- Enable encryption for highly sensitive data
- Maintain audit trails for compliance

#### 3. Test Security Measures
- Verify that no real data patterns appear
- Check that masking is working correctly
- Validate compliance with your requirements

### Performance Best Practices

#### 1. Choose the Right Strategy
- **Small datasets** (< 10K): Default settings
- **Medium datasets** (10K-100K): Parallel processing
- **Large datasets** (100K-1M): Streaming + parallel
- **Very large datasets** (1M+): Pure streaming

#### 2. Optimize Configuration
- Use simple rules when possible
- Avoid complex conditional logic if not needed
- Choose appropriate data types

#### 3. Monitor Resource Usage
- Watch memory consumption
- Check generation speed
- Adjust settings based on performance

This comprehensive user guide provides everything non-technical users need to successfully generate synthetic data for their business needs, with clear explanations, practical examples, and step-by-step instructions.