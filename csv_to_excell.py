import pandas as pd
import os

def csv_to_excel(csv_file, excel_file):
    # Try different encodings
    try:
        df = pd.read_csv(csv_file, encoding='utf-8')
    except UnicodeDecodeError:
        try:
            df = pd.read_csv(csv_file, encoding='latin1')
        except UnicodeDecodeError:
            df = pd.read_csv(csv_file, encoding='cp1252')
    
    # Save to Excel
    df.to_excel(excel_file, index=False)
    print(f"Excel file has been created at: {excel_file}")

if __name__ == "__main__":
    csv_file = os.path.join("citi_data", "citi_rutherford_hardware_sheet(citi_datacenter_room_dc7).csv")
    excel_file = os.path.join("citi_data", "citi_rutherford_hardware_sheet.xlsx")
    
    csv_to_excel(csv_file, excel_file)