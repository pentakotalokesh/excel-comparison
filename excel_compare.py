"""
Excel File Comparison Tool - Summary Only Version
Compares multiple sheets from two Excel files with automatic key detection
and generates validation summary report with counts only.

Requirements:
pip install pandas openpyxl xlsxwriter numpy
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime
import logging
from dataclasses import dataclass, field
import warnings

warnings.filterwarnings('ignore', category=UserWarning)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('excel_comparison.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class ComparisonResult:
    """Stores comparison results for a single sheet"""
    sheet_name: str
    key_column: str
    row_count_file1: int = 0
    row_count_file2: int = 0
    col_count_file1: int = 0
    col_count_file2: int = 0
    new_records_count: int = 0
    deleted_records_count: int = 0
    modified_records_count: int = 0
    duplicates_file1_count: int = 0
    duplicates_file2_count: int = 0
    column_changes: Dict[str, int] = field(default_factory=dict)


class ExcelComparator:
    """Production-ready Excel file comparator with automatic key detection"""
    
    def __init__(self, file1_path: str, file2_path: str, 
                 sheets: Optional[List[str]] = None,
                 header_rows: Optional[Dict[str, int]] = None,
                 file1_label: str = "Report Version - 3",
                 file2_label: str = "Report Version - 4",
                 chunk_size: int = 10000,
                 key_columns: Optional[Dict[str, List[str]]] = None):
        """
        Initialize the comparator
        
        Args:
            file1_path: Path to first Excel file (base/old file)
            file2_path: Path to second Excel file (comparison/new file)
            sheets: List of sheet names to compare (None = all sheets)
            header_rows: Dict mapping sheet names to header row numbers (0-indexed)
            file1_label: Label for first file in report
            file2_label: Label for second file in report
            chunk_size: Number of rows to process at once for large files
            key_columns: Dict mapping sheet names to list of column names to use as composite key
        """
        self.file1_path = Path(file1_path)
        self.file2_path = Path(file2_path)
        self.sheets = sheets
        self.header_rows = header_rows or {}
        self.key_columns = key_columns or {}
        self.file1_label = file1_label
        self.file2_label = file2_label
        self.chunk_size = chunk_size
        self.results: List[ComparisonResult] = []
        
        # Validate files exist
        if not self.file1_path.exists():
            raise FileNotFoundError(f"File not found: {file1_path}")
        if not self.file2_path.exists():
            raise FileNotFoundError(f"File not found: {file2_path}")
    
    def _normalize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize column names to lowercase with underscores"""
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
        return df
    
    def _harmonize_datatypes(self, df1: pd.DataFrame, df2: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Align data types between two dataframes"""
        df1 = df1.copy()
        df2 = df2.copy()
        
        common_cols = df1.columns.intersection(df2.columns)
        
        for col in common_cols:
            if df1[col].dtype != df2[col].dtype:
                df1[col] = df1[col].astype(str)
                df2[col] = df2[col].astype(str)
            
            df1[col] = df1[col].fillna('')
            df2[col] = df2[col].fillna('')
            
            if df1[col].dtype == 'object':
                df1[col] = df1[col].astype(str).str.strip()
                df2[col] = df2[col].astype(str).str.strip()
        
        return df1, df2
    
    def _detect_key_column(self, df: pd.DataFrame, sheet_name: str):
        """Automatically detect the best key column or composite key"""
        if df.empty:
            return df.columns[0] if len(df.columns) > 0 else 'index'
        
        if sheet_name in self.key_columns:
            specified_keys = self.key_columns[sheet_name]
            logger.info(f"Sheet '{sheet_name}': Using user-specified key columns: {specified_keys}")
            return specified_keys
        
        for col in df.columns:
            if df[col].notna().sum() > 0:
                unique_ratio = df[col].nunique() / len(df)
                if unique_ratio > 0.95:
                    logger.info(f"Sheet '{sheet_name}': Auto-detected key column: {col}")
                    return col
        
        for num_cols in [2, 3]:
            if len(df.columns) >= num_cols:
                test_cols = df.columns[:num_cols].tolist()
                composite = df[test_cols].astype(str).agg('_'.join, axis=1)
                unique_ratio = composite.nunique() / len(df)
                if unique_ratio > 0.95:
                    logger.info(f"Sheet '{sheet_name}': Using composite key: {test_cols}")
                    return test_cols
        
        logger.warning(f"Sheet '{sheet_name}': No unique key found, using full-row comparison")
        return None
    
    def _create_row_hash(self, df: pd.DataFrame, key_col) -> pd.DataFrame:
        """Create a hash for each row to enable full-row comparison"""
        df = df.copy()
        
        if isinstance(key_col, list):
            df['_composite_key'] = df[key_col].fillna('').astype(str).agg('||'.join, axis=1)
            return df
        elif key_col is None:
            df['_row_hash'] = df.fillna('').astype(str).agg('||'.join, axis=1)
            return df
        else:
            return df
    
    def _get_comparison_key(self, key_col):
        """Get the actual column name to use for comparison"""
        if isinstance(key_col, list):
            return '_composite_key'
        elif key_col is None:
            return '_row_hash'
        else:
            return key_col
    
    def _count_duplicates(self, df: pd.DataFrame, key_col) -> int:
        """Count duplicate records based on key column"""
        if df.empty:
            return 0
        
        df = self._create_row_hash(df.copy(), key_col)
        comp_key = self._get_comparison_key(key_col)
        
        return df.duplicated(subset=[comp_key], keep=False).sum()
    
    def _count_new_records(self, df1: pd.DataFrame, df2: pd.DataFrame, key_col) -> int:
        """Count records in df2 that don't exist in df1"""
        if df2.empty:
            return 0
        if df1.empty:
            return len(df2)
        
        df1_copy = self._create_row_hash(df1.copy(), key_col)
        df2_copy = self._create_row_hash(df2.copy(), key_col)
        comp_key = self._get_comparison_key(key_col)
        
        new_keys = set(df2_copy[comp_key]) - set(df1_copy[comp_key])
        return len(new_keys)
    
    def _count_deleted_records(self, df1: pd.DataFrame, df2: pd.DataFrame, key_col) -> int:
        """Count records in df1 that don't exist in df2"""
        if df1.empty:
            return 0
        if df2.empty:
            return len(df1)
        
        df1_copy = self._create_row_hash(df1.copy(), key_col)
        df2_copy = self._create_row_hash(df2.copy(), key_col)
        comp_key = self._get_comparison_key(key_col)
        
        deleted_keys = set(df1_copy[comp_key]) - set(df2_copy[comp_key])
        return len(deleted_keys)
    
    def _count_modified_records(self, df1: pd.DataFrame, df2: pd.DataFrame, key_col) -> int:
        """Count modified records"""
        if df1.empty or df2.empty:
            return 0
        
        df1_copy = self._create_row_hash(df1.copy(), key_col)
        df2_copy = self._create_row_hash(df2.copy(), key_col)
        comp_key = self._get_comparison_key(key_col)
        
        common_keys = set(df1_copy[comp_key]).intersection(set(df2_copy[comp_key]))
        if not common_keys:
            return 0
        
        df1_common = df1_copy[df1_copy[comp_key].isin(common_keys)].set_index(comp_key).sort_index()
        df2_common = df2_copy[df2_copy[comp_key].isin(common_keys)].set_index(comp_key).sort_index()
        
        all_cols = df1_common.columns.intersection(df2_common.columns).tolist()
        common_cols = [col for col in all_cols if col not in ['_composite_key', '_row_hash']]
        
        modified_count = 0
        
        for key in common_keys:
            if key not in df1_common.index or key not in df2_common.index:
                continue
            
            try:
                row1 = df1_common.loc[key, common_cols]
                row2 = df2_common.loc[key, common_cols]
                
                if not isinstance(row1, pd.Series):
                    row1 = pd.Series(row1, index=common_cols)
                if not isinstance(row2, pd.Series):
                    row2 = pd.Series(row2, index=common_cols)
                
                differences = (row1 != row2).fillna(False)
                
                if differences.any():
                    modified_count += 1
                    
            except Exception as e:
                logger.warning(f"Error comparing key '{key}': {e}")
                continue
        
        return modified_count
    
    def _read_sheet(self, file_path: Path, sheet_name: str) -> pd.DataFrame:
        """Read a sheet from Excel file with error handling"""
        try:
            header_row = self.header_rows.get(sheet_name, 0)
            df = pd.read_excel(file_path, sheet_name=sheet_name, header=header_row)
            
            df = df.loc[:, ~df.columns.str.contains('^Unnamed', case=False, na=False)]
            df = self._normalize_column_names(df)
            df = df.dropna(how='all')
            df = df.dropna(axis=1, how='all')
            
            logger.info(f"Read sheet '{sheet_name}' from {file_path.name}: {len(df)} rows, {len(df.columns)} columns")
            return df
        except Exception as e:
            logger.error(f"Error reading sheet '{sheet_name}' from {file_path.name}: {e}")
            return pd.DataFrame()
    
    def compare_sheets(self) -> List[ComparisonResult]:
        """Compare all specified sheets between the two files"""
        logger.info(f"Starting comparison: {self.file1_path.name} vs {self.file2_path.name}")
        
        if self.sheets is None:
            xl_file = pd.ExcelFile(self.file1_path)
            self.sheets = xl_file.sheet_names
            logger.info(f"Comparing all sheets: {self.sheets}")
        
        for sheet_name in self.sheets:
            logger.info(f"\n{'='*60}")
            logger.info(f"Processing sheet: {sheet_name}")
            logger.info(f"{'='*60}")
            
            try:
                df1 = self._read_sheet(self.file1_path, sheet_name)
                df2 = self._read_sheet(self.file2_path, sheet_name)
                
                row_count_f1 = len(df1)
                row_count_f2 = len(df2)
                col_count_f1 = len(df1.columns) if not df1.empty else 0
                col_count_f2 = len(df2.columns) if not df2.empty else 0
                
                if df1.empty and df2.empty:
                    logger.warning(f"Sheet '{sheet_name}' is empty in both files. Skipping.")
                    continue
                
                if df1.empty or df2.empty:
                    if df1.empty:
                        key_col = self._detect_key_column(df2, sheet_name)
                        result = ComparisonResult(
                            sheet_name=sheet_name,
                            key_column=str(key_col) if isinstance(key_col, list) else (key_col if key_col else "Full Row Hash"),
                            row_count_file1=row_count_f1,
                            row_count_file2=row_count_f2,
                            col_count_file1=col_count_f1,
                            col_count_file2=col_count_f2,
                            new_records_count=len(df2),
                            duplicates_file2_count=self._count_duplicates(df2, key_col)
                        )
                    else:
                        key_col = self._detect_key_column(df1, sheet_name)
                        result = ComparisonResult(
                            sheet_name=sheet_name,
                            key_column=str(key_col) if isinstance(key_col, list) else (key_col if key_col else "Full Row Hash"),
                            row_count_file1=row_count_f1,
                            row_count_file2=row_count_f2,
                            col_count_file1=col_count_f1,
                            col_count_file2=col_count_f2,
                            deleted_records_count=len(df1),
                            duplicates_file1_count=self._count_duplicates(df1, key_col)
                        )
                    self.results.append(result)
                    continue
                
                key_col = self._detect_key_column(df1, sheet_name)
                df1, df2 = self._harmonize_datatypes(df1, df2)
                
                logger.info("Counting changes...")
                duplicates_count_f1 = self._count_duplicates(df1, key_col)
                duplicates_count_f2 = self._count_duplicates(df2, key_col)
                new_records_count = self._count_new_records(df1, df2, key_col)
                deleted_records_count = self._count_deleted_records(df1, df2, key_col)
                modified_records_count = self._count_modified_records(df1, df2, key_col)
                
                key_col_display = str(key_col) if isinstance(key_col, list) else (key_col if key_col else "Full Row Hash")
                
                result = ComparisonResult(
                    sheet_name=sheet_name,
                    key_column=key_col_display,
                    row_count_file1=row_count_f1,
                    row_count_file2=row_count_f2,
                    col_count_file1=col_count_f1,
                    col_count_file2=col_count_f2,
                    new_records_count=new_records_count,
                    deleted_records_count=deleted_records_count,
                    modified_records_count=modified_records_count,
                    duplicates_file1_count=duplicates_count_f1,
                    duplicates_file2_count=duplicates_count_f2
                )
                self.results.append(result)
                
                logger.info(f"\nSheet '{sheet_name}' Summary:")
                logger.info(f"  Key Column: {key_col_display}")
                logger.info(f"  Row Count: {row_count_f1} vs {row_count_f2}")
                logger.info(f"  Column Count: {col_count_f1} vs {col_count_f2}")
                logger.info(f"  New Records: {new_records_count}")
                logger.info(f"  Deleted Records: {deleted_records_count}")
                logger.info(f"  Modified Records: {modified_records_count}")
                logger.info(f"  Duplicates in File1: {duplicates_count_f1}")
                logger.info(f"  Duplicates in File2: {duplicates_count_f2}")
                
            except Exception as e:
                logger.error(f"Error processing sheet '{sheet_name}': {e}", exc_info=True)
                continue
        
        return self.results
    
    def generate_report(self, output_path: str = None) -> str:
        """Generate comprehensive Excel report with validation summary"""
        if not self.results:
            logger.warning("No comparison results to report.")
            return None
        
        if output_path is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_path = f'comparison_report_{timestamp}.xlsx'
        
        logger.info(f"\nGenerating report: {output_path}")
        
        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
            workbook = writer.book
            
            header_format = workbook.add_format({
                'bold': True,
                'bg_color': '#4472C4',
                'font_color': 'white',
                'border': 1,
                'align': 'center',
                'valign': 'vcenter'
            })
            
            title_format = workbook.add_format({
                'bold': True,
                'bg_color': '#4472C4',
                'font_color': 'white',
                'border': 1,
                'align': 'left',
                'valign': 'vcenter',
                'font_size': 11
            })
            
            yes_format = workbook.add_format({
                'bg_color': '#FF6B6B',
                'font_color': 'white',
                'bold': True,
                'border': 1,
                'align': 'center'
            })
            
            no_format = workbook.add_format({
                'bg_color': '#D3D3D3',
                'border': 1,
                'align': 'center'
            })
            
            match_format = workbook.add_format({
                'border': 1,
                'align': 'center'
            })
            
            mismatch_format = workbook.add_format({
                'bg_color': '#FF6B6B',
                'font_color': 'white',
                'border': 1,
                'align': 'center'
            })
            
            comment_format = workbook.add_format({
                'border': 1,
                'align': 'left'
            })
            
            self._create_validation_summary(writer, header_format, title_format, 
                                          yes_format, no_format, match_format, 
                                          mismatch_format, comment_format)
        
        logger.info(f"Report generated successfully: {output_path}")
        return output_path
    
    def _create_validation_summary(self, writer, header_format, title_format,
                                  yes_format, no_format, match_format, 
                                  mismatch_format, comment_format):
        """Create validation summary sheet with counts only"""
        workbook = writer.book
        worksheet = workbook.add_worksheet('Validation Summary')
        
        worksheet.set_column('A:A', 25)
        worksheet.set_column('B:B', 20)
        worksheet.set_column('C:C', 20)
        worksheet.set_column('D:D', 60)
        
        current_row = 0
        
        worksheet.write(current_row, 0, 'Tab Validation Summary', title_format)
        worksheet.write(current_row, 1, '', title_format)
        worksheet.write(current_row, 2, '', title_format)
        worksheet.write(current_row, 3, 'Comments', title_format)
        current_row += 1
        
        worksheet.write(current_row, 0, 'Validations', header_format)
        worksheet.write(current_row, 1, self.file1_label, header_format)
        worksheet.write(current_row, 2, self.file2_label, header_format)
        worksheet.write(current_row, 3, '', header_format)
        current_row += 1
        
        total_sheets_f1 = len(self.results)
        total_sheets_f2 = len(self.results)
        
        worksheet.write(current_row, 0, 'Total Tabs Count', match_format)
        worksheet.write(current_row, 1, total_sheets_f1, match_format)
        worksheet.write(current_row, 2, total_sheets_f2, match_format)
        worksheet.write(current_row, 3, 'Count Match' if total_sheets_f1 == total_sheets_f2 else 'Count Mismatch', comment_format)
        current_row += 1
        
        worksheet.write(current_row, 0, 'Tabs Added', match_format)
        worksheet.write(current_row, 1, 'No', no_format)
        worksheet.write(current_row, 2, 'No', no_format)
        worksheet.write(current_row, 3, 'No new tabs', comment_format)
        current_row += 1
        
        worksheet.write(current_row, 0, 'Tabs Removed', match_format)
        worksheet.write(current_row, 1, 'No', no_format)
        worksheet.write(current_row, 2, 'No', no_format)
        worksheet.write(current_row, 3, 'No tabs removed', comment_format)
        current_row += 1
        
        for result in self.results:
            current_row += 1
            
            worksheet.write(current_row, 0, f'Tab Name: {result.sheet_name}', title_format)
            worksheet.write(current_row, 1, '', title_format)
            worksheet.write(current_row, 2, '', title_format)
            worksheet.write(current_row, 3, 'Comments', title_format)
            current_row += 1
            
            worksheet.write(current_row, 0, 'Validations', header_format)
            worksheet.write(current_row, 1, self.file1_label, header_format)
            worksheet.write(current_row, 2, self.file2_label, header_format)
            worksheet.write(current_row, 3, '', header_format)
            current_row += 1
            
            row_match = result.row_count_file1 == result.row_count_file2
            worksheet.write(current_row, 0, 'Row Count', match_format)
            worksheet.write(current_row, 1, result.row_count_file1, 
                          match_format if row_match else mismatch_format)
            worksheet.write(current_row, 2, result.row_count_file2, 
                          match_format if row_match else mismatch_format)
            worksheet.write(current_row, 3, 
                          'Row Count is match' if row_match else 'Row Count is mismatch', 
                          comment_format)
            current_row += 1
            
            col_match = result.col_count_file1 == result.col_count_file2
            worksheet.write(current_row, 0, 'Column Count', match_format)
            worksheet.write(current_row, 1, result.col_count_file1, 
                          match_format if col_match else mismatch_format)
            worksheet.write(current_row, 2, result.col_count_file2, 
                          match_format if col_match else mismatch_format)
            worksheet.write(current_row, 3, 
                          'Column Count is match' if col_match else 'Column Count is mismatch', 
                          comment_format)
            current_row += 1
            
            has_new = result.new_records_count > 0
            worksheet.write(current_row, 0, 'New Records', match_format)
            worksheet.write(current_row, 1, 'No', no_format)
            worksheet.write(current_row, 2, 'Yes' if has_new else 'No', 
                          yes_format if has_new else no_format)
            comment = f'{result.new_records_count} New Records found' if has_new else ''
            worksheet.write(current_row, 3, comment, comment_format)
            current_row += 1
            
            has_modified = result.modified_records_count > 0
            worksheet.write(current_row, 0, 'Modified Records', match_format)
            worksheet.write(current_row, 1, 'No', no_format)
            worksheet.write(current_row, 2, 'Yes' if has_modified else 'No', 
                          yes_format if has_modified else no_format)
            comment = f'{result.modified_records_count} Modified Records found' if has_modified else ''
            worksheet.write(current_row, 3, comment, comment_format)
            current_row += 1
            
            has_deleted = result.deleted_records_count > 0
            worksheet.write(current_row, 0, 'Deleted Records', match_format)
            worksheet.write(current_row, 1, 'No', no_format)
            worksheet.write(current_row, 2, 'Yes' if has_deleted else 'No', 
                          yes_format if has_deleted else no_format)
            comment = f'{result.deleted_records_count} Deleted Records found' if has_deleted else ''
            worksheet.write(current_row, 3, comment, comment_format)
            current_row += 1
            
            has_dup = result.duplicates_file1_count > 0 or result.duplicates_file2_count > 0
            worksheet.write(current_row, 0, 'Duplicate Records', match_format)
            worksheet.write(current_row, 1, 'No', no_format)
            worksheet.write(current_row, 2, 'Yes' if has_dup else 'No', 
                          yes_format if has_dup else no_format)
            comment = ''
            if has_dup:
                comment = f'File1: {result.duplicates_file1_count}, File2: {result.duplicates_file2_count} duplicates'
            worksheet.write(current_row, 3, comment, comment_format)
            current_row += 1


def main():
    """Example usage"""
    
    FILE1 = 'SampleData1.xlsx'
    FILE2 = 'SampleData.xlsx'
    
    SHEETS = ['Sample Orders']
    
    HEADER_ROWS = {
        'Sample Orders': 0
    }
    
    FILE1_LABEL = "Old Version"
    FILE2_LABEL = "New Version"
    
    try:
        comparator = ExcelComparator(
            file1_path=FILE1,
            file2_path=FILE2,
            sheets=SHEETS,
            header_rows=HEADER_ROWS,
            file1_label=FILE1_LABEL,
            file2_label=FILE2_LABEL,
            chunk_size=10000
        )
        
        results = comparator.compare_sheets()
        report_path = comparator.generate_report()
        
        print(f"\n{'='*60}")
        print(f"Comparison complete!")
        print(f"Report saved to: {report_path}")
        print(f"{'='*60}")
        
    except Exception as e:
        logger.error(f"Comparison failed: {e}", exc_info=True)
        raise


if __name__ == '__main__':
    main()