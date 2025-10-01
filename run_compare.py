from excel_compare import compare_excel_files

# Pass sheet info as a dict: {sheetname: header_row_number}
sheet_info = {
    "Sample Orders": 0,
    # Add more sheets as needed
}

# Convert to list of dicts for compare_excel_files
sheet_info_list = [
    {"sheet1": sheet, "sheet2": sheet, "header1": header, "header2": header}
    for sheet, header in sheet_info.items()
]

compare_excel_files("SampleData1.xlsx", "SampleData.xlsx", sheet_info_list, "ComparisonReport.xlsx")
