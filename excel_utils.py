import logging
from io import BytesIO
from openpyxl import load_workbook


class EmptyInputError(Exception):
    pass

def update_excel_sheet(file_content, modifications):
    try:
        if not file_content:
            raise EmptyInputError("file_content is empty")
        if not modifications:
            raise EmptyInputError("modifications are empty")
        
        # Load the workbook from the file content
        workbook = load_workbook(filename=BytesIO(file_content))
        sheet = workbook.active
        
        # Apply modifications
        for cell, value in modifications.items():
            sheet[cell] = value
        
        # Save the modified workbook to a BytesIO object
        modified_workbook = BytesIO()
        workbook.save(modified_workbook)
        modified_workbook.seek(0)
        return modified_workbook.read()
    
    except EmptyInputError as e:
        logging.error(e)
        return None
    except Exception as e:
        logging.error(f"An unknown error occurred: {e}")
        return None
