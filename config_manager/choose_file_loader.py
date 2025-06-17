import pandas as pd
from typing import Dict, Any
from pathlib import Path


class ChoiceFileLoader:
    """Handles loading choice values from CSV/Excel files for data generation"""

    def __init__(self):
        self.loaded_files = {}

    def load_choices_from_file(self, file_path: str, column: str = None,
                               weight_column: str = None, **kwargs) -> Dict[str, Any]:
        """
        Load choices from CSV/Excel file

        Args:
            file_path: Path to the CSV/Excel file
            column: Column name containing the choice values (if None, uses first column)
            weight_column: Column name containing weights/probabilities
            **kwargs: Additional pandas read options

        Returns:
            Dict containing choice rule configuration
        """
        try:
            full_path = Path(file_path)

            if not self.validate_file_exists(full_path):
                raise FileNotFoundError(f'File not Found: {full_path}')

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

            if column not in df.columns:
                raise ValueError(f"Column '{column}' not found in {file_path}")

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
                # Group by choice value and sum weights
                weight_df = df.groupby(column)[weight_column].sum().reset_index()
                total_weight = weight_df[weight_column].sum()

                # Convert to probabilities
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

    def validate_file_exists(self, file_path: Path) -> bool:
        """Check if the choice file exists"""
        full_path = Path(file_path)
        return full_path.exists()

    def get_file_info(self, file_path: str) -> Dict[str, Any]:
        """Get information about a choice file"""
        full_path = Path(file_path)

        if not full_path.exists():
            return {"exists": False}

        try:
            # Load file to get column info
            if full_path.suffix.lower() in ['.xlsx', '.xls']:
                df = pd.read_excel(full_path, nrows=5)  # Just peek at first few rows
            else:
                df = pd.read_csv(full_path, nrows=5)

            return {
                "exists": True,
                "columns": df.columns.tolist(),
                "row_count": len(df),
                "sample_data": df.head().to_dict('records')
            }
        except Exception as e:
            return {"exists": True, "error": str(e)}
