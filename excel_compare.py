"""
Excel File Comparison Tool - Production Ready
Compares multiple sheets from two Excel files with automatic key detection
and generates comprehensive validation reports.

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
    new_records: pd.DataFrame = field(default_factory=pd.DataFrame)
    deleted_records: pd.DataFrame = field(default_factory=pd.DataFrame)
    modified_records: pd.DataFrame = field(default_factory=pd.DataFrame)
    duplicates_file1: pd.DataFrame = field(default_factory=pd.DataFrame)
    duplicates_file2: pd.DataFrame = field(default_factory=pd.DataFrame)
    column_changes: Dict[str, int] = field(default_factory=dict)


class ExcelComparator:
    """Production-ready Excel file comparator with automatic key detection"""
    
    def __init__(self, file1_path: str, file2_path: str, 
                 sheets: Optional[List[str]] = None,
                 header_rows: Optional[Dict[str, int]] = None,
                 file1_label: str = "Report Version - 3",
                 file2_label: str = "Report Version - 4",
                 chunk_size: int = 10000):
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
        """
        self.file1_path = Path(file1_path)
        self.file2_path = Path(file2_path)
        self.sheets = sheets
        self.header_rows = header_rows or {}
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
        common_cols = df1.columns.intersection(df2.columns)
        
        for col in common_cols:
            # Convert to string for comparison if types differ
            if df1[col].dtype != df2[col].dtype:
                df1[col] = df1[col].astype(str)
                df2[col] = df2[col].astype(str)
            
            # Handle NaN values consistently
            df1[col] = df1[col].fillna('')
            df2[col] = df2[col].fillna('')
        
        return df1, df2
    
    def _detect_key_column(self, df: pd.DataFrame, sheet_name: str) -> str:
        """
        Automatically detect the best key column
        
        Returns the first column with unique values, or first column as fallback
        """
        if df.empty:
            return df.columns[0] if len(df.columns) > 0 else 'index'
        
        # Try to find a column with unique values
        for col in df.columns:
            if df[col].notna().sum() > 0:  # Has non-null values
                unique_ratio = df[col].nunique() / len(df)
                if unique_ratio > 0.95:  # 95% or more unique values
                    logger.info(f"Sheet '{sheet_name}': Auto-detected key column: {col}")
                    return col
        
        # Fallback to first column
        first_col = df.columns[0]
        logger.warning(f"Sheet '{sheet_name}': No unique column found, using first column: {first_col}")
        return first_col
    
    def _find_duplicates(self, df: pd.DataFrame, key_col: str) -> pd.DataFrame:
        """Find duplicate records based on key column"""
        if df.empty:
            return pd.DataFrame()
        
        duplicates = df[df.duplicated(subset=[key_col], keep=False)]
        return duplicates.sort_values(by=key_col)
    
    def _find_new_records(self, df1: pd.DataFrame, df2: pd.DataFrame, key_col: str) -> pd.DataFrame:
        """Find records in df2 that don't exist in df1"""
        if df2.empty:
            return pd.DataFrame()
        if df1.empty:
            return df2
        
        new_keys = set(df2[key_col]) - set(df1[key_col])
        return df2[df2[key_col].isin(new_keys)]
    
    def _find_deleted_records(self, df1: pd.DataFrame, df2: pd.DataFrame, key_col: str) -> pd.DataFrame:
        """Find records in df1 that don't exist in df2"""
        if df1.empty:
            return pd.DataFrame()
        if df2.empty:
            return df1
        
        deleted_keys = set(df1[key_col]) - set(df2[key_col])
        return df1[df1[key_col].isin(deleted_keys)]
    
    def _find_modified_records(self, df1: pd.DataFrame, df2: pd.DataFrame, 
                              key_col: str) -> Tuple[pd.DataFrame, Dict[str, int]]:
        """Find modified records and track changes per column"""
        if df1.empty or df2.empty:
            return pd.DataFrame(), {}
        
        # Get common keys
        common_keys = set(df1[key_col]).intersection(set(df2[key_col]))
        if not common_keys:
            return pd.DataFrame(), {}
        
        # Filter to common records
        df1_common = df1[df1[key_col].isin(common_keys)].set_index(key_col).sort_index()
        df2_common = df2[df2[key_col].isin(common_keys)].set_index(key_col).sort_index()
        
        # Get common columns
        common_cols = df1_common.columns.intersection(df2_common.columns)
        
        # Track changes per column
        column_changes = {}
        modified_rows = []
        
        for key in common_keys:
            if key not in df1_common.index or key not in df2_common.index:
                continue
            
            row1 = df1_common.loc[key, common_cols]
            row2 = df2_common.loc[key, common_cols]
            
            # Compare values
            differences = row1 != row2
            
            if differences.any():
                changed_cols = common_cols[differences].tolist()
                
                # Track column-level changes
                for col in changed_cols:
                    column_changes[col] = column_changes.get(col, 0) + 1
                
                # Create change record
                change_record = {key_col: key}
                for col in changed_cols:
                    change_record[f'{col}_old'] = row1[col]
                    change_record[f'{col}_new'] = row2[col]
                
                modified_rows.append(change_record)
        
        modified_df = pd.DataFrame(modified_rows) if modified_rows else pd.DataFrame()
        return modified_df, column_changes
    
    def _read_sheet(self, file_path: Path, sheet_name: str) -> pd.DataFrame:
        """Read a sheet from Excel file with error handling"""
        try:
            header_row = self.header_rows.get(sheet_name, 0)
            df = pd.read_excel(file_path, sheet_name=sheet_name, header=header_row)
            df = self._normalize_column_names(df)
            
            # Remove completely empty rows
            df = df.dropna(how='all')
            
            logger.info(f"Read sheet '{sheet_name}' from {file_path.name}: {len(df)} rows")
            return df
        except Exception as e:
            logger.error(f"Error reading sheet '{sheet_name}' from {file_path.name}: {e}")
            return pd.DataFrame()
    
    def compare_sheets(self) -> List[ComparisonResult]:
        """Compare all specified sheets between the two files"""
        logger.info(f"Starting comparison: {self.file1_path.name} vs {self.file2_path.name}")
        
        # Get sheet names
        if self.sheets is None:
            xl_file = pd.ExcelFile(self.file1_path)
            self.sheets = xl_file.sheet_names
            logger.info(f"Comparing all sheets: {self.sheets}")
        
        for sheet_name in self.sheets:
            logger.info(f"\n{'='*60}")
            logger.info(f"Processing sheet: {sheet_name}")
            logger.info(f"{'='*60}")
            
            try:
                # Read sheets
                df1 = self._read_sheet(self.file1_path, sheet_name)
                df2 = self._read_sheet(self.file2_path, sheet_name)
                
                # Store original counts
                row_count_f1 = len(df1)
                row_count_f2 = len(df2)
                col_count_f1 = len(df1.columns) if not df1.empty else 0
                col_count_f2 = len(df2.columns) if not df2.empty else 0
                
                # Skip if both sheets are empty
                if df1.empty and df2.empty:
                    logger.warning(f"Sheet '{sheet_name}' is empty in both files. Skipping.")
                    continue
                
                # Handle case where one sheet is empty
                if df1.empty or df2.empty:
                    if df1.empty:
                        key_col = self._detect_key_column(df2, sheet_name)
                        result = ComparisonResult(
                            sheet_name=sheet_name,
                            key_column=key_col,
                            row_count_file1=row_count_f1,
                            row_count_file2=row_count_f2,
                            col_count_file1=col_count_f1,
                            col_count_file2=col_count_f2,
                            new_records=df2,
                            deleted_records=pd.DataFrame(),
                            duplicates_file2=self._find_duplicates(df2, key_col)
                        )
                    else:
                        key_col = self._detect_key_column(df1, sheet_name)
                        result = ComparisonResult(
                            sheet_name=sheet_name,
                            key_column=key_col,
                            row_count_file1=row_count_f1,
                            row_count_file2=row_count_f2,
                            col_count_file1=col_count_f1,
                            col_count_file2=col_count_f2,
                            new_records=pd.DataFrame(),
                            deleted_records=df1,
                            duplicates_file1=self._find_duplicates(df1, key_col)
                        )
                    self.results.append(result)
                    continue
                
                # Detect key column
                key_col = self._detect_key_column(df1, sheet_name)
                
                # Harmonize data types
                df1, df2 = self._harmonize_datatypes(df1, df2)
                
                # Find duplicates
                logger.info("Finding duplicates...")
                duplicates_file1 = self._find_duplicates(df1, key_col)
                duplicates_file2 = self._find_duplicates(df2, key_col)
                
                # Find new records
                logger.info("Finding new records...")
                new_records = self._find_new_records(df1, df2, key_col)
                
                # Find deleted records
                logger.info("Finding deleted records...")
                deleted_records = self._find_deleted_records(df1, df2, key_col)
                
                # Find modified records
                logger.info("Finding modified records...")
                modified_records, column_changes = self._find_modified_records(df1, df2, key_col)
                
                # Store results
                result = ComparisonResult(
                    sheet_name=sheet_name,
                    key_column=key_col,
                    row_count_file1=row_count_f1,
                    row_count_file2=row_count_f2,
                    col_count_file1=col_count_f1,
                    col_count_file2=col_count_f2,
                    new_records=new_records,
                    deleted_records=deleted_records,
                    modified_records=modified_records,
                    duplicates_file1=duplicates_file1,
                    duplicates_file2=duplicates_file2,
                    column_changes=column_changes
                )
                self.results.append(result)
                
                # Log summary
                logger.info(f"\nSheet '{sheet_name}' Summary:")
                logger.info(f"  Key Column: {key_col}")
                logger.info(f"  Row Count: {row_count_f1} vs {row_count_f2}")
                logger.info(f"  Column Count: {col_count_f1} vs {col_count_f2}")
                logger.info(f"  New Records: {len(new_records)}")
                logger.info(f"  Deleted Records: {len(deleted_records)}")
                logger.info(f"  Modified Records: {len(modified_records)}")
                logger.info(f"  Duplicates in File1: {len(duplicates_file1)}")
                logger.info(f"  Duplicates in File2: {len(duplicates_file2)}")
                
            except Exception as e:
                logger.error(f"Error processing sheet '{sheet_name}': {e}", exc_info=True)
                continue
        
        return self.results
    
    def generate_report(self, output_path: str = None) -> str:
        """
        Generate comprehensive Excel report with validation summary
        
        Args:
            output_path: Custom output path (optional)
        
        Returns:
            Path to the generated report
        """
        if not self.results:
            logger.warning("No comparison results to report.")
            return None
        
        # Generate default output path if not provided
        if output_path is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_path = f'comparison_report_{timestamp}.xlsx'
        
        logger.info(f"\nGenerating report: {output_path}")
        
        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
            workbook = writer.book
            
            # Create formats
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
            
            # Generate Validation Summary Sheet
            self._create_validation_summary(writer, header_format, title_format, 
                                          yes_format, no_format, match_format, 
                                          mismatch_format, comment_format)
            
            # Generate detail sheets for each result
            for result in self.results:
                sheet_prefix = result.sheet_name[:25]  # Limit sheet name length
                
                # New Records
                if not result.new_records.empty:
                    sheet_name = f'{sheet_prefix}_New'[:31]
                    result.new_records.to_excel(writer, sheet_name=sheet_name, index=False)
                    self._format_sheet(writer, sheet_name, header_format, result.new_records)
                
                # Deleted Records
                if not result.deleted_records.empty:
                    sheet_name = f'{sheet_prefix}_Deleted'[:31]
                    result.deleted_records.to_excel(writer, sheet_name=sheet_name, index=False)
                    self._format_sheet(writer, sheet_name, header_format, result.deleted_records)
                
                # Modified Records
                if not result.modified_records.empty:
                    sheet_name = f'{sheet_prefix}_Modified'[:31]
                    result.modified_records.to_excel(writer, sheet_name=sheet_name, index=False)
                    self._format_sheet(writer, sheet_name, header_format, result.modified_records)
                
                # Duplicates File1
                if not result.duplicates_file1.empty:
                    sheet_name = f'{sheet_prefix}_Dup_F1'[:31]
                    result.duplicates_file1.to_excel(writer, sheet_name=sheet_name, index=False)
                    self._format_sheet(writer, sheet_name, header_format, result.duplicates_file1)
                
                # Duplicates File2
                if not result.duplicates_file2.empty:
                    sheet_name = f'{sheet_prefix}_Dup_F2'[:31]
                    result.duplicates_file2.to_excel(writer, sheet_name=sheet_name, index=False)
                    self._format_sheet(writer, sheet_name, header_format, result.duplicates_file2)
        
        logger.info(f"Report generated successfully: {output_path}")
        return output_path
    
    def _create_validation_summary(self, writer, header_format, title_format,
                                  yes_format, no_format, match_format, 
                                  mismatch_format, comment_format):
        """Create validation summary sheet like the provided image"""
        worksheet = workbook = writer.book.add_worksheet('Validation Summary')
        
        # Set column widths
        worksheet.set_column('A:A', 25)
        worksheet.set_column('B:B', 20)
        worksheet.set_column('C:C', 20)
        worksheet.set_column('D:D', 60)
        
        current_row = 0
        
        # Tab Validation Summary Section
        worksheet.write(current_row, 0, 'Tab Validation Summary', title_format)
        worksheet.write(current_row, 1, '', title_format)
        worksheet.write(current_row, 2, '', title_format)
        worksheet.write(current_row, 3, 'Comments', title_format)
        current_row += 1
        
        # Headers
        worksheet.write(current_row, 0, 'Validations', header_format)
        worksheet.write(current_row, 1, self.file1_label, header_format)
        worksheet.write(current_row, 2, self.file2_label, header_format)
        worksheet.write(current_row, 3, '', header_format)
        current_row += 1
        
        # Total Tabs Count
        total_sheets_f1 = len(self.results)
        total_sheets_f2 = len(self.results)
        
        worksheet.write(current_row, 0, 'Total Tabs Count', match_format)
        worksheet.write(current_row, 1, total_sheets_f1, match_format)
        worksheet.write(current_row, 2, total_sheets_f2, match_format)
        worksheet.write(current_row, 3, 'Count Match' if total_sheets_f1 == total_sheets_f2 else 'Count Mismatch', comment_format)
        current_row += 1
        
        # Tabs Added
        worksheet.write(current_row, 0, 'Tabs Added', match_format)
        worksheet.write(current_row, 1, 'No', no_format)
        worksheet.write(current_row, 2, 'No', no_format)
        worksheet.write(current_row, 3, 'No new tabs', comment_format)
        current_row += 1
        
        # Tabs Removed
        worksheet.write(current_row, 0, 'Tabs Removed', match_format)
        worksheet.write(current_row, 1, 'No', no_format)
        worksheet.write(current_row, 2, 'No', no_format)
        worksheet.write(current_row, 3, 'No tabs removed', comment_format)
        current_row += 1
        
        # Process each sheet
        for result in self.results:
            current_row += 1
            
            # Tab Name Header
            worksheet.write(current_row, 0, f'Tab Name: {result.sheet_name}', title_format)
            worksheet.write(current_row, 1, '', title_format)
            worksheet.write(current_row, 2, '', title_format)
            worksheet.write(current_row, 3, 'Comments', title_format)
            current_row += 1
            
            # Sub-headers
            worksheet.write(current_row, 0, 'Validations', header_format)
            worksheet.write(current_row, 1, self.file1_label, header_format)
            worksheet.write(current_row, 2, self.file2_label, header_format)
            worksheet.write(current_row, 3, '', header_format)
            current_row += 1
            
            # Row Count
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
            
            # Column Count
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
            
            # New Records
            has_new = len(result.new_records) > 0
            worksheet.write(current_row, 0, 'New Records', match_format)
            worksheet.write(current_row, 1, 'No', no_format)
            worksheet.write(current_row, 2, 'Yes' if has_new else 'No', 
                          yes_format if has_new else no_format)
            comment = f'{len(result.new_records)}-New Records available in New Records tab' if has_new else ''
            worksheet.write(current_row, 3, comment, comment_format)
            current_row += 1
            
            # Modified Records
            has_modified = len(result.modified_records) > 0
            worksheet.write(current_row, 0, 'Modified Records', match_format)
            worksheet.write(current_row, 1, 'No', no_format)
            worksheet.write(current_row, 2, 'Yes' if has_modified else 'No', 
                          yes_format if has_modified else no_format)
            comment = f'{len(result.modified_records)}-Modified Records available in Modified Record tab' if has_modified else ''
            worksheet.write(current_row, 3, comment, comment_format)
            current_row += 1
            
            # Deleted Records
            has_deleted = len(result.deleted_records) > 0
            worksheet.write(current_row, 0, 'Deleted Records', match_format)
            worksheet.write(current_row, 1, 'No', no_format)
            worksheet.write(current_row, 2, 'Yes' if has_deleted else 'No', 
                          yes_format if has_deleted else no_format)
            comment = f'{len(result.deleted_records)}-Deleted Record details available in Deleted Records Data tab' if has_deleted else ''
            worksheet.write(current_row, 3, comment, comment_format)
            current_row += 1
            
            # Duplicate Records
            has_dup = len(result.duplicates_file1) > 0 or len(result.duplicates_file2) > 0
            worksheet.write(current_row, 0, 'Duplicate Records', match_format)
            worksheet.write(current_row, 1, 'No', no_format)
            worksheet.write(current_row, 2, 'Yes' if has_dup else 'No', 
                          yes_format if has_dup else no_format)
            worksheet.write(current_row, 3, '', comment_format)
            current_row += 1
    
    def _format_sheet(self, writer, sheet_name, header_format, df):
        """Apply formatting to a worksheet"""
        worksheet = writer.sheets[sheet_name]
        
        # Format header row
        for col_num, col_name in enumerate(df.columns):
            worksheet.write(0, col_num, col_name, header_format)
        
        # Auto-fit columns
        for i, col in enumerate(df.columns):
            try:
                max_len = max(df[col].astype(str).map(len).max(), len(col)) + 2
                worksheet.set_column(i, i, min(max_len, 50))
            except:
                worksheet.set_column(i, i, 15)  # Default width if error


def main():
    """Example usage"""
    
    # Configuration
    FILE1 = 'SampleData1.xlsx'  # Base/old file
    FILE2 = 'SampleData.xlsx'  # Comparison/new file
    
    # Optional: Specify sheets to compare
    SHEETS = ['Sample Orders']  # None = all sheets, or ['Sheet1', 'Sheet2']
    
    # Optional: Specify header rows for specific sheets
    HEADER_ROWS = {
        'Sample Orders': 0
        # 'Sheet1': 0,  # First row is header (default)
        # 'Sheet2': 1,  # Second row is header
    }
    
    # Optional: Custom labels for the report
    FILE1_LABEL = "Report Version - 1"
    FILE2_LABEL = "Report Version - 2"
    
    try:
        # Initialize comparator
        comparator = ExcelComparator(
            file1_path=FILE1,
            file2_path=FILE2,
            sheets=SHEETS,
            header_rows=HEADER_ROWS,
            file1_label=FILE1_LABEL,
            file2_label=FILE2_LABEL,
            chunk_size=10000  # Adjust for memory constraints
        )
        
        # Run comparison
        results = comparator.compare_sheets()
        
        # Generate report
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