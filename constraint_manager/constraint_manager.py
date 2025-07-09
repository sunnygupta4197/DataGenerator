import random
import logging
import threading
from typing import Dict, List, Any, Set, Optional, Tuple
from collections import defaultdict
import pandas as pd
import itertools
from functools import lru_cache
import weakref
import gc
from concurrent.futures import ThreadPoolExecutor
import time
import re
from datetime import datetime, timedelta


class MemoryManager:
    """Manages memory usage and cleanup for constraint tracking"""

    def __init__(self, max_memory_mb: int = 500):
        self.max_memory_mb = max_memory_mb
        self.cache_size_limit = self._calculate_cache_size(max_memory_mb)
        self._last_cleanup = time.time()
        self._cleanup_interval = 300  # 5 minutes

    def _calculate_cache_size(self, memory_mb: int) -> int:
        """Calculate optimal cache size based on available memory"""
        # Estimate: each cache entry uses ~100 bytes on average
        bytes_available = memory_mb * 1024 * 1024
        estimated_entry_size = 100
        return min(bytes_available // estimated_entry_size, 100000)

    def should_cleanup(self) -> bool:
        """Check if cleanup is needed"""
        return time.time() - self._last_cleanup > self._cleanup_interval

    def cleanup_if_needed(self, caches: List[Dict]) -> None:
        """Perform cleanup if needed"""
        if self.should_cleanup():
            self._cleanup_caches(caches)
            self._last_cleanup = time.time()
            gc.collect()

    def _cleanup_caches(self, caches: List[Dict]) -> None:
        """Clean up least recently used cache entries"""
        for cache in caches:
            if len(cache) > self.cache_size_limit:
                # Keep only the most recent entries
                items = list(cache.items())
                items.sort(key=lambda x: len(x[1]) if isinstance(x[1], (set, list)) else 0)
                cache.clear()
                # Keep top 80% of cache size
                keep_size = int(self.cache_size_limit * 0.8)
                for key, value in items[-keep_size:]:
                    cache[key] = value


class LRUConstraintCache:
    """LRU cache implementation for constraint tracking"""

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

    def add_to_set(self, key: str, value: Any) -> None:
        """Add value to a set in cache"""
        with self._lock:
            current_set = self.get(key, set())
            current_set.add(value)
            self.set(key, current_set)

    def contains_in_set(self, key: str, value: Any) -> bool:
        """Check if value exists in set"""
        with self._lock:
            current_set = self.get(key, set())
            return value in current_set

    def clear(self) -> None:
        """Clear all cache entries"""
        with self._lock:
            self.cache.clear()
            self.access_order.clear()

    def size(self) -> int:
        """Get current cache size"""
        with self._lock:
            return len(self.cache)


class ConstraintManager:
    """
    Enhanced constraint manager with all methods required by DataGenerator and BatchGenerator
    """

    def __init__(self, logger=None, max_memory_mb: int = 500, enable_parallel: bool = True):
        self.logger = logger or logging.getLogger(__name__)
        self.enable_parallel = enable_parallel
        self._lock = threading.RLock()

        # Memory management
        self.memory_manager = MemoryManager(max_memory_mb)

        # Optimized constraint tracking with LRU caches
        self._pk_cache = LRUConstraintCache(maxsize=self.memory_manager.cache_size_limit)
        self._unique_cache = LRUConstraintCache(maxsize=self.memory_manager.cache_size_limit)

        # Foreign key pools with bounded memory
        self._fk_pools = {}
        self._fk_pools_lock = threading.RLock()

        # Relationship constraint tracking
        self._one_to_one_used = defaultdict(set)
        self._one_to_many_distribution = defaultdict(dict)

        # Performance settings
        self._batch_fk_refresh_threshold = 10000
        self._parallel_batch_size = 1000

        # Thread pool for parallel operations
        self._thread_pool = ThreadPoolExecutor(max_workers=4) if enable_parallel else None

        # Statistics tracking
        self._stats = {
            'cache_hits': 0,
            'cache_misses': 0,
            'memory_cleanups': 0,
            'parallel_operations': 0,
            'condition_evaluations': 0,
            'type_conversions': 0,
            'constraint_checks': 0
        }

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()

    def cleanup(self):
        """Clean up resources"""
        if self._thread_pool:
            self._thread_pool.shutdown(wait=True)
        self._pk_cache.clear()
        self._unique_cache.clear()
        self._fk_pools.clear()
        gc.collect()

    # ===================== MEMORY MANAGEMENT =====================

    def _perform_memory_cleanup(self):
        """Perform memory cleanup if needed"""
        if self.memory_manager.should_cleanup():
            caches = [self._fk_pools, self._one_to_one_used, self._one_to_many_distribution]
            self.memory_manager.cleanup_if_needed(caches)
            self._stats['memory_cleanups'] += 1

    def get_memory_usage(self) -> Dict[str, int]:
        """Get current memory usage statistics"""
        return {
            'pk_cache_size': self._pk_cache.size(),
            'unique_cache_size': self._unique_cache.size(),
            'fk_pools_count': len(self._fk_pools),
            'one_to_one_relationships': len(self._one_to_one_used),
            'one_to_many_relationships': len(self._one_to_many_distribution),
            'total_fk_values': sum(len(pool) for pool in self._fk_pools.values())
        }

    def cleanup_memory(self):
        """Public method to trigger memory cleanup"""
        self._perform_memory_cleanup()

    # ===================== CONDITION EVALUATION (from original ConstraintManager) =====================

    def evaluate_conditions(self, conditions: List[Dict], row_data: Dict[str, Any]) -> bool:
        """
        Evaluate conditions with proper data type handling

        Args:
            conditions: List of condition dictionaries
            row_data: Row data to evaluate against

        Returns:
            bool: True if all conditions are met, False otherwise
        """
        if not conditions:
            return True

        with self._lock:
            self._stats['condition_evaluations'] += 1

        try:
            for condition in conditions:
                if not self._evaluate_single_condition(condition, row_data):
                    return False
            return True

        except Exception as e:
            self.logger.warning(f"Error evaluating conditions {conditions}: {e}")
            return False

    def _evaluate_single_condition(self, condition: Dict, row_data: Dict[str, Any]) -> bool:
        """
        Evaluate a single condition with proper type conversion

        Args:
            condition: Condition dictionary with column, operator, value
            row_data: Row data to evaluate against

        Returns:
            bool: True if condition is met, False otherwise
        """
        try:
            # Extract condition components
            column = condition.get('column')
            operator = condition.get('operator')
            expected_value = condition.get('value')

            if not column or not operator:
                self.logger.warning(f"Invalid condition format: {condition}")
                return True  # Default to True for invalid conditions

            # Get actual value from row data
            actual_value = row_data.get(column)

            # Handle null values
            if actual_value is None:
                if operator in ['is_null', 'isnull']:
                    return True
                elif operator in ['is_not_null', 'not_null']:
                    return False
                else:
                    return True  # Default behavior for null values

            # Convert values to compatible types
            actual_value, expected_value = self._convert_values_for_comparison(
                actual_value, expected_value, condition
            )

            # Evaluate based on operator
            return self._apply_operator(actual_value, expected_value, operator)

        except Exception as e:
            self.logger.warning(f"Error evaluating condition {condition}: {e}")
            return True  # Default to True for error cases

    def _convert_values_for_comparison(self, actual_value: Any, expected_value: Any,
                                       condition: Dict) -> Tuple[Any, Any]:
        """
        Convert values to compatible types for comparison
        """
        with self._lock:
            self._stats['type_conversions'] += 1

        try:
            # Handle string to numeric conversions
            if isinstance(actual_value, str) and isinstance(expected_value, (int, float)):
                try:
                    if isinstance(expected_value, int):
                        converted_actual = int(actual_value)
                    else:
                        converted_actual = float(actual_value)
                    return converted_actual, expected_value
                except (ValueError, TypeError):
                    self.logger.debug(f"Failed to convert '{actual_value}' to {type(expected_value).__name__}")

            # Handle numeric to string conversions
            elif isinstance(actual_value, (int, float)) and isinstance(expected_value, str):
                try:
                    converted_expected = type(actual_value)(expected_value)
                    return actual_value, converted_expected
                except (ValueError, TypeError):
                    # If conversion fails, convert actual to string
                    return str(actual_value), expected_value

            # Handle date/datetime conversions
            elif self._is_date_like(actual_value) or self._is_date_like(expected_value):
                return self._convert_date_values(actual_value, expected_value)

            # Handle boolean conversions
            elif isinstance(actual_value, bool) or isinstance(expected_value, bool):
                return self._convert_boolean_values(actual_value, expected_value)

            # No conversion needed - types are compatible
            return actual_value, expected_value

        except Exception as e:
            self.logger.debug(f"Type conversion error: {e}")
            # Return original values if conversion fails
            return actual_value, expected_value

    def _is_date_like(self, value: Any) -> bool:
        """Check if value is date-like"""
        if isinstance(value, (datetime, str)):
            if isinstance(value, str):
                # Simple check for date-like strings
                return bool(any(char in value for char in ['-', '/', ':']))
        return False

    def _convert_date_values(self, actual_value: Any, expected_value: Any) -> Tuple[Any, Any]:
        """Convert date values for comparison"""
        try:
            # Convert string dates to datetime objects
            if isinstance(actual_value, str):
                try:
                    actual_value = datetime.fromisoformat(actual_value.replace('/', '-'))
                except:
                    pass

            if isinstance(expected_value, str):
                try:
                    expected_value = datetime.fromisoformat(expected_value.replace('/', '-'))
                except:
                    pass

        except Exception:
            pass

        return actual_value, expected_value

    def _convert_boolean_values(self, actual_value: Any, expected_value: Any) -> Tuple[Any, Any]:
        """Convert boolean values for comparison"""
        try:
            # Convert string representations of booleans
            if isinstance(actual_value, str):
                if actual_value.lower() in ('true', 't', '1', 'yes', 'y'):
                    actual_value = True
                elif actual_value.lower() in ('false', 'f', '0', 'no', 'n'):
                    actual_value = False

            if isinstance(expected_value, str):
                if expected_value.lower() in ('true', 't', '1', 'yes', 'y'):
                    expected_value = True
                elif expected_value.lower() in ('false', 'f', '0', 'no', 'n'):
                    expected_value = False

        except Exception:
            pass

        return actual_value, expected_value

    def _apply_operator(self, actual_value: Any, expected_value: Any, operator: str) -> bool:
        """
        Apply comparison operator with proper handling including range operations
        """
        try:
            # Normalize operator names
            operator = operator.lower().strip()

            # Map operator variations
            operator_map = {
                '==': 'equals',
                '=': 'equals',
                'eq': 'equals',
                'equal': 'equals',
                '!=': 'not_equals',
                '<>': 'not_equals',
                'ne': 'not_equals',
                'not_equal': 'not_equals',
                '>': 'greater_than',
                'gt': 'greater_than',
                '>=': 'greater_than_or_equal',
                'gte': 'greater_than_or_equal',
                '<': 'less_than',
                'lt': 'less_than',
                '<=': 'less_than_or_equal',
                'lte': 'less_than_or_equal',
                'in': 'in',
                'not_in': 'not_in',
                'contains': 'contains',
                'starts_with': 'starts_with',
                'ends_with': 'ends_with',
                'like': 'like',
                'range': 'range',
                'between': 'range',
                'within': 'range',
                'in_range': 'range'
            }

            normalized_operator = operator_map.get(operator, operator)

            # Apply the operator
            if normalized_operator == 'equals':
                return actual_value == expected_value
            elif normalized_operator == 'not_equals':
                return actual_value != expected_value
            elif normalized_operator == 'greater_than':
                return actual_value > expected_value
            elif normalized_operator == 'greater_than_or_equal':
                return actual_value >= expected_value
            elif normalized_operator == 'less_than':
                return actual_value < expected_value
            elif normalized_operator == 'less_than_or_equal':
                return actual_value <= expected_value
            elif normalized_operator == 'range':
                return self._apply_range_operator(actual_value, expected_value)
            elif normalized_operator == 'in':
                if isinstance(expected_value, (list, tuple, set)):
                    return actual_value in expected_value
                else:
                    return actual_value == expected_value
            elif normalized_operator == 'not_in':
                if isinstance(expected_value, (list, tuple, set)):
                    return actual_value not in expected_value
                else:
                    return actual_value != expected_value
            elif normalized_operator == 'contains':
                return str(expected_value).lower() in str(actual_value).lower()
            elif normalized_operator == 'starts_with':
                return str(actual_value).lower().startswith(str(expected_value).lower())
            elif normalized_operator == 'ends_with':
                return str(actual_value).lower().endswith(str(expected_value).lower())
            elif normalized_operator == 'like':
                # Simple LIKE implementation
                pattern = str(expected_value).replace('%', '.*').replace('_', '.')
                return bool(re.match(pattern, str(actual_value), re.IGNORECASE))
            else:
                self.logger.warning(f"Unsupported operator: {operator}")
                return True

        except Exception as e:
            self.logger.warning(f"Error applying operator '{operator}' to '{actual_value}' and '{expected_value}': {e}")
            return True  # Default to True for error cases

    def _apply_range_operator(self, actual_value: Any, expected_value: Any) -> bool:
        """
        Apply range operator with support for various range formats
        """
        try:
            # Handle different range formats
            if isinstance(expected_value, dict):
                # Format: {'min': 10, 'max': 50} or {'start': 10, 'end': 50}
                min_val = expected_value.get('min', expected_value.get('start'))
                max_val = expected_value.get('max', expected_value.get('end'))

                if min_val is not None and max_val is not None:
                    # Convert values for comparison
                    actual_value, min_val = self._convert_values_for_comparison(actual_value, min_val, {})
                    actual_value, max_val = self._convert_values_for_comparison(actual_value, max_val, {})
                    return min_val <= actual_value <= max_val
                elif min_val is not None:
                    actual_value, min_val = self._convert_values_for_comparison(actual_value, min_val, {})
                    return actual_value >= min_val
                elif max_val is not None:
                    actual_value, max_val = self._convert_values_for_comparison(actual_value, max_val, {})
                    return actual_value <= max_val

            elif isinstance(expected_value, (list, tuple)) and len(expected_value) == 2:
                # Format: [10, 50] or (10, 50)
                min_val, max_val = expected_value[0], expected_value[1]
                actual_value, min_val = self._convert_values_for_comparison(actual_value, min_val, {})
                actual_value, max_val = self._convert_values_for_comparison(actual_value, max_val, {})
                return min_val <= actual_value <= max_val

            elif isinstance(expected_value, str):
                # Format: "10-50" or "10..50" or "10,50"
                range_separators = ['-', '..', ',', ' to ', ' TO ']
                for separator in range_separators:
                    if separator in expected_value:
                        parts = expected_value.split(separator, 1)
                        if len(parts) == 2:
                            try:
                                min_val = float(parts[0].strip()) if '.' in parts[0] else int(parts[0].strip())
                                max_val = float(parts[1].strip()) if '.' in parts[1] else int(parts[1].strip())
                                actual_value, min_val = self._convert_values_for_comparison(actual_value, min_val, {})
                                actual_value, max_val = self._convert_values_for_comparison(actual_value, max_val, {})
                                return min_val <= actual_value <= max_val
                            except (ValueError, TypeError):
                                continue

            else:
                # Single value - treat as equality
                actual_value, expected_value = self._convert_values_for_comparison(actual_value, expected_value, {})
                return actual_value == expected_value

            # Fallback to True if no valid range format found
            self.logger.debug(f"Could not parse range format: {expected_value}")
            return True

        except Exception as e:
            self.logger.warning(f"Error in range operation: {e}")
            return True

    # ===================== NULL VALUE GENERATION =====================

    def should_generate_null(self, column_def: Dict[str, Any]) -> bool:
        """
        Determine if a null value should be generated based on column definition

        Args:
            column_def: Column definition dictionary

        Returns:
            bool: True if null should be generated
        """
        try:
            # Check if column is nullable
            nullable = column_def.get('nullable', True)
            if not nullable:
                return False

            # Get null percentage
            null_percentage = column_def.get('null_percentage', 0)
            if null_percentage <= 0:
                return False

            # Generate random decision
            return random.random() * 100 < null_percentage

        except Exception as e:
            self.logger.debug(f"Error in should_generate_null: {e}")
            return False

    # ===================== OPTIMIZED PRIMARY KEY CONSTRAINTS =====================

    def is_pk_value_used(self, table_name: str, column_name: str, value: Any) -> bool:
        """Check if primary key value is already used with LRU caching"""
        constraint_key = f"{table_name}.{column_name}"

        if self._pk_cache.contains_in_set(constraint_key, value):
            self._stats['cache_hits'] += 1
            return True

        self._stats['cache_misses'] += 1
        return False

    def add_pk_value(self, table_name: str, column_name: str, value: Any):
        """Add primary key value to tracking with thread safety"""
        constraint_key = f"{table_name}.{column_name}"
        self._pk_cache.add_to_set(constraint_key, value)

        # Periodic cleanup
        self._perform_memory_cleanup()

    def generate_unique_pk_value(self, table_name: str, column_name: str,
                                 value_generator_func, max_attempts: int = 100) -> Any:
        """Generate a unique primary key value with optimized collision detection"""
        constraint_key = f"{table_name}.{column_name}"

        # Get existing values for more efficient collision detection
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

    # ===================== OPTIMIZED UNIQUE CONSTRAINTS =====================

    def is_unique_value_used(self, table_name: str, column_name: str, value: Any) -> bool:
        """Check if unique constraint value is already used"""
        constraint_key = f"{table_name}.{column_name}"
        return self._unique_cache.contains_in_set(constraint_key, value)

    def add_unique_value(self, table_name: str, column_name: str, value: Any):
        """Add value to unique constraint tracking"""
        constraint_key = f"{table_name}.{column_name}"
        self._unique_cache.add_to_set(constraint_key, value)

    def generate_unique_value(self, table_name: str, column_name: str,
                              value_generator_func, max_attempts: int = 100) -> Any:
        """Generate unique value for columns with unique constraints"""
        constraint_key = f"{table_name}.{column_name}"
        existing_values = self._unique_cache.get(constraint_key, set())

        for attempt in range(max_attempts):
            value = value_generator_func()
            if value not in existing_values:
                self.add_unique_value(table_name, column_name, value)
                return value

        # Fallback with timestamp
        import time
        base_value = str(value_generator_func()) if value_generator_func else "unique"
        unique_value = f"{base_value}_{int(time.time() * 1000000) % 1000000}"
        self.add_unique_value(table_name, column_name, unique_value)
        return unique_value

    # ===================== OPTIMIZED FOREIGN KEY CONSTRAINTS =====================

    def update_fk_pool(self, table_name: str, column_name: str, values: List[Any]):
        """Update foreign key pool with thread safety and memory management"""
        fk_key = f"{table_name}.{column_name}"

        with self._fk_pools_lock:
            # Limit pool size to prevent memory issues
            max_pool_size = min(len(values), 50000)
            if len(values) > max_pool_size:
                # Sample randomly to maintain distribution
                values = random.sample(values, max_pool_size)

            self._fk_pools[fk_key] = values

        self.logger.debug(f"Updated FK pool {fk_key} with {len(values)} values")

    def get_fk_values(self, parent_table: str, parent_column: str,
                      sample_size: Optional[int] = None) -> List[Any]:
        """Get available foreign key values with optimized sampling"""
        fk_key = f"{parent_table}.{parent_column}"

        with self._fk_pools_lock:
            available_values = self._fk_pools.get(fk_key, [])

        # Optimized sampling
        if sample_size and len(available_values) > sample_size:
            return random.sample(available_values, sample_size)

        return available_values.copy()  # Return copy to prevent external modification

    def get_fk_values_with_relationship(self, fk_config: Dict, available_values: List[Any],
                                        record_count: int) -> List[Any]:
        """
        Get FK values considering relationship type

        Args:
            fk_config: Foreign key configuration
            available_values: Available FK values
            record_count: Number of records to generate

        Returns:
            List of FK values considering relationship constraints
        """
        if not available_values:
            return []

        relationship_type = fk_config.get('relationship_type', 'many_to_one')

        try:
            if relationship_type == 'one_to_one':
                # Each FK value can only be used once
                return available_values[:record_count]

            elif relationship_type == 'one_to_many':
                # Each FK value can be used multiple times, but distribute evenly
                if len(available_values) >= record_count:
                    return random.sample(available_values, record_count)
                else:
                    # Repeat values to fill record count
                    result = []
                    for i in range(record_count):
                        result.append(available_values[i % len(available_values)])
                    return result

            else:  # many_to_one (default)
                # Multiple child records can reference the same parent
                return [random.choice(available_values) for _ in range(record_count)]

        except Exception as e:
            self.logger.warning(f"Error in get_fk_values_with_relationship: {e}")
            return [random.choice(available_values) for _ in range(min(record_count, len(available_values)))]

    def get_relationship_type(self, fk_config: Dict) -> str:
        """
        Get the relationship type from FK configuration

        Args:
            fk_config: Foreign key configuration

        Returns:
            Relationship type string
        """
        return fk_config.get('relationship_type', 'many_to_one').lower()

    def refresh_fk_pools_from_dataframe(self, df_map: Dict[str, pd.DataFrame],
                                        table_metadata_map: Dict[str, Dict]):
        """Efficiently refresh foreign key pools with parallel processing"""
        if self.enable_parallel and len(df_map) > 2:
            self._refresh_fk_pools_parallel(df_map, table_metadata_map)
        else:
            self._refresh_fk_pools_sequential(df_map, table_metadata_map)

    def _refresh_fk_pools_sequential(self, df_map: Dict[str, pd.DataFrame],
                                     table_metadata_map: Dict[str, Dict]):
        """Sequential FK pool refresh"""
        for table_name, df in df_map.items():
            if df.empty:
                continue

            table_metadata = table_metadata_map.get(table_name, {})
            pk_columns = self._get_primary_key_columns(table_metadata)

            for pk_col in pk_columns:
                if pk_col in df.columns:
                    # Optimized unique value extraction
                    unique_values = df[pk_col].dropna().unique().tolist()
                    self.update_fk_pool(table_name, pk_col, unique_values)

    def _refresh_fk_pools_parallel(self, df_map: Dict[str, pd.DataFrame],
                                   table_metadata_map: Dict[str, Dict]):
        """Parallel FK pool refresh for better performance"""
        self._stats['parallel_operations'] += 1

        def process_table(table_data):
            table_name, df, table_metadata = table_data
            if df.empty:
                return []

            pk_columns = self._get_primary_key_columns(table_metadata)
            results = []

            for pk_col in pk_columns:
                if pk_col in df.columns:
                    unique_values = df[pk_col].dropna().unique().tolist()
                    results.append((table_name, pk_col, unique_values))

            return results

        # Prepare data for parallel processing
        table_data = [
            (table_name, df, table_metadata_map.get(table_name, {}))
            for table_name, df in df_map.items()
            if not df.empty
        ]

        # Process in parallel
        if self._thread_pool and table_data:
            future_to_table = {
                self._thread_pool.submit(process_table, data): data[0]
                for data in table_data
            }

            for future in future_to_table:
                try:
                    results = future.result(timeout=30)  # 30 second timeout
                    for table_name, pk_col, unique_values in results:
                        self.update_fk_pool(table_name, pk_col, unique_values)
                except Exception as e:
                    table_name = future_to_table[future]
                    self.logger.error(f"Error processing table {table_name}: {e}")

    # ===================== OPTIMIZED RELATIONSHIP CONSTRAINTS =====================

    def handle_one_to_one_relationship(self, fk_config: Dict, available_values: List[Any]) -> Optional[Any]:
        """Handle one-to-one relationship constraints with optimization"""
        parent_table = fk_config["parent_table"]
        parent_column = fk_config["parent_column"]
        constraint_key = f"{parent_table}.{parent_column}"
        with self._lock:
            used_values = self._one_to_one_used[constraint_key]

            # Optimized filtering using set operations
            available_set = set(available_values)
            unused_set = available_set - used_values

            if not unused_set:
                self.logger.warning(f"No unused values available for one-to-one relationship {constraint_key}")
                return None

            # Select and mark as used
            selected_value = random.choice(list(unused_set))
            used_values.add(selected_value)

        self.logger.debug(f"Assigned one-to-one value {selected_value} for {constraint_key}")
        return selected_value

    def handle_one_to_many_relationship(self, fk_config: Dict, available_values: List[Any],
                                        record_count: int) -> List[Any]:
        """Handle one-to-many relationship with optimized distribution"""
        distribution_type = fk_config.get("distribution", "random")

        # Use optimized distribution methods
        if distribution_type == "even":
            return self._distribute_evenly_optimized(available_values, record_count)
        elif distribution_type == "weighted":
            weights = fk_config.get("weights", {})
            return self._distribute_weighted_optimized(available_values, record_count, weights)
        else:
            return self._distribute_randomly_optimized(available_values, record_count, fk_config)

    def _distribute_evenly_optimized(self, available_values: List[Any], record_count: int) -> List[Any]:
        """Optimized even distribution using cycling"""
        if not available_values:
            return []

        # Use modulo for efficient cycling
        return [available_values[i % len(available_values)] for i in range(record_count)]

    def _distribute_randomly_optimized(self, available_values: List[Any], record_count: int,
                                       fk_config: Dict) -> List[Any]:
        """Optimized random distribution with constraints"""
        if not available_values:
            return []

        min_children = fk_config.get("min_children", 1)
        max_children = fk_config.get("max_children", 10)

        # Pre-allocate result list for better performance
        result = [None] * record_count
        parent_counts = {val: 0 for val in available_values}

        for i in range(record_count):
            # Filter available parents efficiently
            available_parents = [
                val for val, count in parent_counts.items()
                if count < max_children
            ]

            if not available_parents:
                available_parents = available_values

            selected_parent = random.choice(available_parents)
            result[i] = selected_parent
            parent_counts[selected_parent] += 1

        return result

    def _distribute_weighted_optimized(self, available_values: List[Any], record_count: int,
                                       weights: Dict[str, float]) -> List[Any]:
        """Optimized weighted distribution"""
        if not available_values:
            return []

        # Pre-compute weights for efficiency
        value_weights = [weights.get(str(value), 1.0) for value in available_values]

        # Use numpy-style weighted choice if available, else use random.choices
        try:
            import numpy as np
            probabilities = np.array(value_weights) / sum(value_weights)
            indices = np.random.choice(len(available_values), size=record_count, p=probabilities)
            return [available_values[i] for i in indices]
        except ImportError:
            return random.choices(available_values, weights=value_weights, k=record_count)

    # ===================== BATCH OPERATIONS =====================

    def apply_length_constraints_to_batch(self, batch_data: List[Dict],
                                          table_metadata: Dict) -> List[Dict]:
        """Apply length constraints to a batch with parallel processing"""
        if not batch_data:
            return batch_data

        if self.enable_parallel and len(batch_data) > self._parallel_batch_size:
            return self._apply_constraints_parallel(batch_data, table_metadata)
        else:
            return self._apply_constraints_sequential(batch_data, table_metadata)

    def _apply_constraints_sequential(self, batch_data: List[Dict], table_metadata: Dict) -> List[Dict]:
        """Sequential constraint application"""
        columns = table_metadata.get("columns", [])

        for i, row in enumerate(batch_data):
            for column in columns:
                column_name = column["name"]
                length_constraint = column.get("length")

                if length_constraint and column_name in row:
                    current_value = row[column_name]
                    if isinstance(current_value, str):
                        rule = column.get("rule", {})
                        data_type = column.get("type", "str")
                        adjusted_value = self.adjust_value_for_length(
                            current_value, length_constraint, rule, data_type
                        )
                        if adjusted_value != current_value:
                            batch_data[i][column_name] = adjusted_value

        return batch_data

    def _apply_constraints_parallel(self, batch_data: List[Dict], table_metadata: Dict) -> List[Dict]:
        """Parallel constraint application for large batches"""
        if not self._thread_pool:
            return self._apply_constraints_sequential(batch_data, table_metadata)

        self._stats['parallel_operations'] += 1

        # Split batch into chunks for parallel processing
        chunk_size = max(1, len(batch_data) // 4)  # 4 threads
        chunks = [batch_data[i:i + chunk_size] for i in range(0, len(batch_data), chunk_size)]

        def process_chunk(chunk):
            return self._apply_constraints_sequential(chunk, table_metadata)

        # Process chunks in parallel
        futures = [self._thread_pool.submit(process_chunk, chunk) for chunk in chunks]

        # Collect results
        processed_chunks = []
        for future in futures:
            try:
                processed_chunks.append(future.result(timeout=30))
            except Exception as e:
                self.logger.error(f"Error processing batch chunk: {e}")
                # Fallback to sequential processing for this chunk
                processed_chunks.append(chunks[len(processed_chunks)])

        # Flatten results
        result = []
        for chunk in processed_chunks:
            result.extend(chunk)

        return result

    # ===================== UTILITY METHODS =====================

    def adjust_value_for_length(self, value: Any, length_constraint: Any,
                                rule: Dict = None, data_type: str = "str") -> str:
        """Optimized value adjustment for length constraints"""
        if not isinstance(value, str):
            return str(value)

        current_length = len(value)

        if isinstance(length_constraint, int):
            target_length = length_constraint

            if current_length > target_length:
                return self._truncate_intelligently(value, target_length, rule)
            elif current_length < target_length:
                return self._pad_intelligently(value, target_length, data_type)

        elif isinstance(length_constraint, dict):
            min_length = length_constraint.get('min', 0)
            max_length = length_constraint.get('max', float('inf'))

            if current_length < min_length:
                return self._pad_intelligently(value, min_length, data_type)
            elif current_length > max_length and max_length != float('inf'):
                return self._truncate_intelligently(value, int(max_length), rule)

        return value

    def _truncate_intelligently(self, value: str, target_length: int, rule: Dict = None) -> str:
        """Intelligent truncation preserving structure"""
        if isinstance(rule, dict) and (rule.get("prefix") or rule.get("suffix")):
            prefix = rule.get("prefix", "")
            suffix = rule.get("suffix", "")
            available_length = target_length - len(prefix) - len(suffix)

            if available_length > 0:
                if prefix and suffix:
                    core_value = value[len(prefix):len(value) - len(suffix)]
                elif prefix:
                    core_value = value[len(prefix):]
                elif suffix:
                    core_value = value[:len(value) - len(suffix)]
                else:
                    core_value = value

                truncated_core = core_value[:available_length]
                return f"{prefix}{truncated_core}{suffix}"

        return value[:target_length]

    def _pad_intelligently(self, value: str, target_length: int, data_type: str) -> str:
        """Intelligent padding based on data type"""
        if data_type in ['int', 'integer'] and value.isdigit():
            return value.zfill(target_length)
        elif data_type in ['float', 'floating']:
            return self._pad_float_value(value, target_length)
        else:
            return value.ljust(target_length, 'X')

    def _pad_float_value(self, value: str, target_length: int) -> str:
        value = value.strip()
        try:
            float_value = float(value)
        except ValueError:
            return value.ljust(target_length, 'X')

        is_negative = value.startswith('-')
        if is_negative:
            value = value[1:]
        if '.' in value:
            integer_part, decimal_part = value.split('.')
            decimal_space = len(decimal_part) + 1
            available_for_integer = target_length - decimal_space
            if is_negative:
                available_for_integer = target_length - decimal_space
            if available_for_integer > 0:
                padded_integer = integer_part.zfill(available_for_integer)
                result = f"{padded_integer}{decimal_part}"
            else:
                result = value[:target_length - (1 if is_negative else 0)]
        else:
            available_length = target_length - (1 if is_negative else 0)
            result = value.zfill(available_length)

        if is_negative:
            result = f'-{result}'
        return result[:target_length]

    def _get_primary_key_columns(self, table_metadata: Dict) -> List[str]:
        """Get primary key columns from table metadata with caching"""
        # Check for composite primary key first
        composite_pk = table_metadata.get("composite_primary_key", [])
        if composite_pk:
            return composite_pk

        # Look for individual primary key columns
        pk_columns = []
        for column in table_metadata.get("columns", []):
            constraints = column.get("constraints", [])
            constraint = column.get("constraint", [])

            if "PK" in constraints or "PK" in constraint:
                pk_columns.append(column["name"])

        return pk_columns

    # ===================== RESET AND CLEANUP METHODS =====================

    def reset_table_constraints(self, table_name: str):
        """
        Reset constraints for a specific table

        Args:
            table_name: Name of the table to reset
        """
        try:
            with self._lock:
                # Find and remove PK constraints for this table
                pk_keys_to_remove = [
                    key for key in self._pk_cache.cache.keys()
                    if key.startswith(f"{table_name}.")
                ]
                for key in pk_keys_to_remove:
                    # Remove from both cache and access order
                    if key in self._pk_cache.cache:
                        del self._pk_cache.cache[key]
                    if key in self._pk_cache.access_order:
                        self._pk_cache.access_order.remove(key)

                # Find and remove unique constraints for this table
                unique_keys_to_remove = [
                    key for key in self._unique_cache.cache.keys()
                    if key.startswith(f"{table_name}.")
                ]
                for key in unique_keys_to_remove:
                    if key in self._unique_cache.cache:
                        del self._unique_cache.cache[key]
                    if key in self._unique_cache.access_order:
                        self._unique_cache.access_order.remove(key)

                # Find and remove FK pools for this table
                with self._fk_pools_lock:
                    fk_keys_to_remove = [
                        key for key in self._fk_pools.keys()
                        if key.startswith(f"{table_name}.")
                    ]
                    for key in fk_keys_to_remove:
                        del self._fk_pools[key]

                # Find and remove relationship constraints for this table
                relationship_keys_to_remove = [
                    key for key in self._one_to_one_used.keys()
                    if key.startswith(f"{table_name}.")
                ]
                for key in relationship_keys_to_remove:
                    del self._one_to_one_used[key]

                distribution_keys_to_remove = [
                    key for key in self._one_to_many_distribution.keys()
                    if key.startswith(f"{table_name}.")
                ]
                for key in distribution_keys_to_remove:
                    del self._one_to_many_distribution[key]

                self.logger.debug(f"Reset constraints for table: {table_name}")

        except Exception as e:
            self.logger.warning(f"Error resetting constraints for table {table_name}: {e}")

    def reset_all_constraints(self):
        """Reset all constraint tracking with proper cleanup"""
        with self._lock:
            self._pk_cache.clear()
            self._unique_cache.clear()

            with self._fk_pools_lock:
                self._fk_pools.clear()

            self._one_to_one_used.clear()
            self._one_to_many_distribution.clear()

            # Reset statistics
            self._stats = {
                'cache_hits': 0,
                'cache_misses': 0,
                'memory_cleanups': 0,
                'parallel_operations': 0,
                'condition_evaluations': 0,
                'type_conversions': 0,
                'constraint_checks': 0
            }

        gc.collect()
        self.logger.info("All constraint tracking caches reset")

    # ===================== STATISTICS AND MONITORING =====================

    def get_constraint_statistics(self) -> Dict[str, Any]:
        """Get comprehensive statistics about constraint usage"""
        memory_stats = self.get_memory_usage()

        return {
            **memory_stats,
            'performance_stats': self._stats.copy(),
            'cache_hit_ratio': (
                self._stats['cache_hits'] /
                (self._stats['cache_hits'] + self._stats['cache_misses'])
                if (self._stats['cache_hits'] + self._stats['cache_misses']) > 0 else 0
            )
        }

    # ===================== ADDITIONAL METHODS FROM ORIGINAL CONSTRAINT MANAGER =====================

    def set_length_constraint_exemptions(self, exemption_patterns: List[str]):
        """
        Set patterns for columns that should be exempt from length constraints

        Args:
            exemption_patterns: List of column name patterns to exempt
        """
        if not hasattr(self, '_length_exemption_patterns'):
            self._length_exemption_patterns = []

        self._length_exemption_patterns.extend(exemption_patterns)
        self.logger.info(f"Added length constraint exemptions: {exemption_patterns}")

    def disable_length_constraints_for_column(self, table_name: str, column_name: str):
        """
        Disable length constraints for a specific column

        Args:
            table_name: Name of the table
            column_name: Name of the column
        """
        if not hasattr(self, '_disabled_length_constraints'):
            self._disabled_length_constraints = set()

        constraint_key = f"{table_name}.{column_name}"
        self._disabled_length_constraints.add(constraint_key)
        self.logger.info(f"Disabled length constraints for: {constraint_key}")

    def is_length_constraint_disabled(self, table_name: str, column_name: str) -> bool:
        """
        Check if length constraints are disabled for a specific column

        Args:
            table_name: Name of the table
            column_name: Name of the column

        Returns:
            bool: True if length constraints are disabled
        """
        if not hasattr(self, '_disabled_length_constraints'):
            return False

        constraint_key = f"{table_name}.{column_name}"
        return constraint_key in self._disabled_length_constraints

    def _should_skip_length_constraint(self, column_name: str, data_type: str, rule: Dict) -> bool:
        """
        Determine if length constraints should be skipped for a column

        Args:
            column_name: Name of the column
            data_type: Data type of the column
            rule: Generation rule for the column

        Returns:
            bool: True if length constraints should be skipped
        """
        # Column names that typically contain categorical data
        categorical_column_patterns = [
            'gender', 'sex', 'status', 'type', 'category', 'level', 'priority',
            'role', 'department', 'division', 'grade', 'class', 'tier',
            'state', 'country', 'region', 'zone', 'area', 'territory',
            'color', 'size', 'style', 'format', 'mode', 'method',
            'active', 'enabled', 'visible', 'public', 'private'
        ]

        column_lower = column_name.lower()

        # Check if column name suggests categorical data
        for pattern in categorical_column_patterns:
            if pattern in column_lower:
                return True

        # Check if rule suggests categorical data
        if isinstance(rule, dict):
            rule_type = rule.get('type')

            # Skip for choice/enum fields
            if rule_type in ['choice', 'enum', 'select']:
                return True

            # Skip for boolean fields
            if rule_type == 'bool' or data_type in ['bool', 'boolean']:
                return True

            # Skip for fields with predefined values
            if 'value' in rule and isinstance(rule['value'], list):
                return True

        # Skip for certain data types
        if data_type in ['bool', 'boolean', 'uuid']:
            return True

        return False

    def _should_preserve_value_intact(self, str_value: str, rule: Dict, data_type: str) -> bool:
        """
        Determine if a value should be preserved intact without length adjustments

        Args:
            str_value: String representation of the value
            rule: Generation rule for context
            data_type: Data type of the column

        Returns:
            bool: True if value should be preserved intact
        """
        # Preserve common categorical values
        categorical_values = {
            # Gender values
            'male', 'female', 'other', 'non-binary', 'transgender', 'prefer not to say',
            'm', 'f', 'o', 'nb', 'trans',

            # Status values
            'active', 'inactive', 'pending', 'completed', 'cancelled', 'approved', 'rejected',
            'enabled', 'disabled', 'suspended', 'archived',

            # Boolean-like values
            'true', 'false', 'yes', 'no', 'on', 'off',

            # Common short codes
            'usa', 'uk', 'ca', 'au', 'de', 'fr', 'jp', 'cn',

            # Priority/severity levels
            'low', 'medium', 'high', 'critical', 'urgent',
            'minor', 'major', 'blocker',

            # Common types
            'admin', 'user', 'guest', 'moderator', 'owner',
            'basic', 'premium', 'pro', 'enterprise',
        }

        if str_value.lower() in categorical_values:
            return True

        # Preserve if this looks like a choice value from the rule
        if isinstance(rule, dict):
            rule_type = rule.get('type')
            if rule_type == 'choice':
                choices = rule.get('value', [])
                if str_value in [str(choice) for choice in choices]:
                    return True

        # Preserve UUID-like values
        if self._looks_like_uuid(str_value):
            return True

        # Preserve email addresses
        if '@' in str_value and '.' in str_value:
            return True

        # Preserve phone numbers
        if self._looks_like_phone_number(str_value):
            return True

        # Preserve dates
        if self._looks_like_date(str_value):
            return True

        return False

    def _looks_like_uuid(self, str_value: str) -> bool:
        """Check if string looks like a UUID"""
        # UUID pattern: 8-4-4-4-12 hexadecimal digits
        uuid_pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
        return bool(re.match(uuid_pattern, str_value.lower()))

    def _looks_like_phone_number(self, str_value: str) -> bool:
        """Check if string looks like a phone number"""
        # Remove common phone number separators
        cleaned = re.sub(r'[\s\-\(\)\+\.]', '', str_value)

        # Check if it's mostly digits and has reasonable length
        if not cleaned.replace('+', '').isdigit():
            return False

        # Phone numbers are typically 7-15 digits
        digit_count = len(re.sub(r'[^\d]', '', cleaned))
        return 7 <= digit_count <= 15

    def _looks_like_date(self, str_value: str) -> bool:
        """Check if string looks like a date"""
        # Common date patterns
        date_patterns = [
            r'\d{4}-\d{1,2}-\d{1,2}',  # YYYY-MM-DD
            r'\d{1,2}/\d{1,2}/\d{4}',  # MM/DD/YYYY or DD/MM/YYYY
            r'\d{1,2}-\d{1,2}-\d{4}',  # MM-DD-YYYY or DD-MM-YYYY
            r'\d{4}/\d{1,2}/\d{1,2}',  # YYYY/MM/DD
            r'\d{1,2}\.\d{1,2}\.\d{4}',  # MM.DD.YYYY or DD.MM.YYYY
            r'\d{4}\.\d{1,2}\.\d{1,2}',  # YYYY.MM.DD
            r'\d{1,2} \w{3} \d{4}',  # DD MMM YYYY
            r'\w{3} \d{1,2}, \d{4}',  # MMM DD, YYYY
            r'\d{4}-\d{1,2}-\d{1,2}T\d{1,2}:\d{1,2}:\d{1,2}',  # ISO datetime
        ]

        for pattern in date_patterns:
            if re.match(pattern, str_value):
                return True

        # Additional check for datetime strings with time components
        if any(char in str_value for char in [':', 'T']) and any(char in str_value for char in ['-', '/']):
            return True

        return False

    def _pad_value_intelligently(self, str_value: str, target_length: int,
                                 data_type: str, rule: Dict) -> str:
        """
        Pad value intelligently based on data type and context

        Args:
            str_value: String value to pad
            target_length: Target length
            data_type: Data type of the column
            rule: Generation rule for context

        Returns:
            Padded string value
        """
        if len(str_value) >= target_length:
            return str_value

        # For numeric types, pad with zeros on the left
        if data_type in ['int', 'integer', 'float', 'double', 'decimal']:
            return str_value.zfill(target_length)

        # For categorical/choice values, don't pad (preserve original)
        if isinstance(rule, dict) and rule.get('type') == 'choice':
            return str_value

        # For codes (uppercase), pad with zeros
        if str_value.isupper() and str_value.isalnum():
            padding_needed = target_length - len(str_value)
            return str_value + '0' * padding_needed

        # For names/text, pad with spaces on the right
        if str_value.isalpha():
            return str_value.ljust(target_length, ' ')

        # Default: pad with appropriate character
        if str_value.isdigit():
            return str_value.zfill(target_length)
        else:
            return str_value.ljust(target_length, ' ')

    def __str__(self):
        stats = self.get_constraint_statistics()
        return f"OptimizedConstraintManager: {stats['pk_cache_size']} PK, {stats['unique_cache_size']} unique, {stats['fk_pools_count']} FK pools"

    def __repr__(self):
        return self.__str__()