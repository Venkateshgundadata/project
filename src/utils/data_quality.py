"""
Data Quality Check Utilities
Provides data validation and quality check functions
"""

import pandas as pd
from datetime import datetime

class DataQualityChecker:
    """Performs data quality checks"""
    
    def __init__(self):
        self.checks_passed = []
        self.checks_failed = []
    
    def check_null_values(self, df, columns, table_name):
        """Check for null values in specified columns"""
        for col in columns:
            null_count = df[col].isnull().sum()
            if null_count > 0:
                self.checks_failed.append({
                    'table': table_name,
                    'check': 'null_check',
                    'column': col,
                    'message': f"Found {null_count} null values in {col}"
                })
            else:
                self.checks_passed.append({
                    'table': table_name,
                    'check': 'null_check',
                    'column': col,
                    'message': f"No null values in {col}"
                })
    
    def check_duplicates(self, df, columns, table_name):
        """Check for duplicate values in specified columns"""
        dup_count = df.duplicated(subset=columns).sum()
        if dup_count > 0:
            self.checks_failed.append({
                'table': table_name,
                'check': 'duplicate_check',
                'columns': columns,
                'message': f"Found {dup_count} duplicate rows"
            })
        else:
            self.checks_passed.append({
                'table': table_name,
                'check': 'duplicate_check',
                'columns': columns,
                'message': f"No duplicate rows found"
            })
    
    def check_data_types(self, df, expected_types, table_name):
        """Check if columns have expected data types"""
        for col, expected_type in expected_types.items():
            actual_type = str(df[col].dtype)
            if expected_type not in actual_type:
                self.checks_failed.append({
                    'table': table_name,
                    'check': 'datatype_check',
                    'column': col,
                    'message': f"Expected {expected_type}, got {actual_type}"
                })
            else:
                self.checks_passed.append({
                    'table': table_name,
                    'check': 'datatype_check',
                    'column': col,
                    'message': f"Correct data type: {actual_type}"
                })
    
    def check_value_range(self, df, column, min_val, max_val, table_name):
        """Check if values are within expected range"""
        out_of_range = df[(df[column] < min_val) | (df[column] > max_val)]
        if len(out_of_range) > 0:
            self.checks_failed.append({
                'table': table_name,
                'check': 'range_check',
                'column': column,
                'message': f"{len(out_of_range)} values outside range [{min_val}, {max_val}]"
            })
        else:
            self.checks_passed.append({
                'table': table_name,
                'check': 'range_check',
                'column': column,
                'message': f"All values within range [{min_val}, {max_val}]"
            })
    
    def check_row_count(self, df, expected_min, table_name):
        """Check if row count meets minimum threshold"""
        row_count = len(df)
        if row_count < expected_min:
            self.checks_failed.append({
                'table': table_name,
                'check': 'row_count',
                'message': f"Row count {row_count} below minimum {expected_min}"
            })
        else:
            self.checks_passed.append({
                'table': table_name,
                'check': 'row_count',
                'message': f"Row count {row_count} meets minimum {expected_min}"
            })
    
    def check_referential_integrity(self, df, ref_df, fk_column, pk_column, table_name):
        """Check foreign key referential integrity"""
        orphaned = df[~df[fk_column].isin(ref_df[pk_column])]
        if len(orphaned) > 0:
            self.checks_failed.append({
                'table': table_name,
                'check': 'referential_integrity',
                'column': fk_column,
                'message': f"Found {len(orphaned)} orphaned records"
            })
        else:
            self.checks_passed.append({
                'table': table_name,
                'check': 'referential_integrity',
                'column': fk_column,
                'message': f"All foreign keys valid"
            })
    
    def get_summary(self):
        """Get summary of all checks"""
        total_checks = len(self.checks_passed) + len(self.checks_failed)
        return {
            'timestamp': datetime.now().isoformat(),
            'total_checks': total_checks,
            'passed': len(self.checks_passed),
            'failed': len(self.checks_failed),
            'pass_rate': len(self.checks_passed) / total_checks if total_checks > 0 else 0,
            'passed_checks': self.checks_passed,
            'failed_checks': self.checks_failed
        }
    
    def print_summary(self):
        """Print summary of checks"""
        summary = self.get_summary()
        print("\n" + "=" * 80)
        print("DATA QUALITY CHECK SUMMARY")
        print("=" * 80)
        print(f"Total Checks: {summary['total_checks']}")
        print(f"Passed: {summary['passed']} ({summary['pass_rate']:.1%})")
        print(f"Failed: {summary['failed']}")
        
        if summary['failed'] > 0:
            print("\nFailed Checks:")
            for check in summary['failed_checks']:
                print(f"  ❌ {check['table']}.{check.get('column', 'N/A')}: {check['message']}")
        
        print("=" * 80)
        return summary['failed'] == 0
