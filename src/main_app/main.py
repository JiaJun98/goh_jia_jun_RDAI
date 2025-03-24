from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from langchain_community.llms import Ollama
import pandas as pd
from datetime import datetime, date
import os
from io import BytesIO
from uuid import uuid4
import time
from datetime import datetime
import uvicorn

from main_app.llm import run, final_assessment

app = FastAPI(debug=True)
llm = "mistral:7b"

# Configure logging
import logging

app = FastAPI()

# Enable CORS for all origins (adjust as needed)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

logging.basicConfig(
    level=logging.DEBUG,                    # Set logging level to DEBUG
    filename="debug.log",                   # Log file name
    filemode="w",                           # Overwrite the log file each run
    format="%(asctime)s - %(levelname)s - %(message)s"  # Log format
)

# Directory to save processed files
SAVE_DIR = "/tmp/volume" #NOTE: To be a temporary docker volume
if not os.path.exists(SAVE_DIR):
    os.makedirs(SAVE_DIR)

# Dictionary to track job statuses
job_status = {}

rename_dict = {
    "Event Main Incident Report No. Year Month": "YYYYMMDD",
    "Event Main Incident Report No.": "Incident Report No.",
    "Event Offence Type": "Offence Type",
    "Event Person Related Name": "Person Name",
    "Event Person Related PersonID": "Person ID",
    "Event Person Related Person Type": "Person Type",
    "Event Person Related Occupation": "Occupation"
}

@app.get("/heartbeat")
async def heartbeat():
    return {"status": "Beating", "timestamp": time.time()}

@app.post("/process-excel/")
async def process_excel(file: UploadFile = File(...)):
    if not file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(status_code=400, detail="Invalid file format. Please upload an Excel file.")

    logging.info(f"Starting processing")
    # Read the Excel file into a DataFrame
    contents = await file.read()
    excel_file = BytesIO(contents)
    logging.info(excel_file)
    #print(excel_file)
    sheet_names = pd.ExcelFile(excel_file).sheet_names
    print(sheet_names)
    processed_sheets = {}
    start_time = datetime.now()
    today = date.today()

    # Calculate total rows across all sheets
    total_rows = 0
    for sheet_name in sheet_names:
        df = pd.read_excel(excel_file, sheet_name=sheet_name) # Read the uploaded xlsx file into a pandas DataFrame
        total_rows += len(df)
    logging.info(f"Total rows to process: {total_rows}")

    # Rewind the file for actual processing
    excel_file.seek(0)
    rows_processed = 0

    def process_row(sheet_name, row):
        """
        Helper function to process a single row, applying all transformations.
        Updates progress after processing the row.
        """
        nonlocal rows_processed
        results = run(row, llm)
        summary = results['summary'].strip()
        assessment = results['assessment'].strip()
        remarks = final_assessment(sheet_name,assessment)

        rows_processed += 1
        progress_percentage = min(int((rows_processed / total_rows) * 100), 100)
        #job_status[task_id]["progress"] = progress_percentage
        logging.debug(f"Progress updated to {progress_percentage}%")
        return summary, remarks
    
    for sheet_name in sheet_names:
            logging.debug(f"Processing sheet: {sheet_name}")
            df = pd.read_excel(excel_file, sheet_name=sheet_name)
            if not df.empty:
                column_headers = pd.read_excel(excel_file, sheet_name=0, nrows=0).columns
                df.columns = column_headers
                df = df.rename(columns={"Breif Facts": "Event Facts Facts of Case"})

                # Apply the process_row function to each row
                processed_data = df['Event Facts Facts of Case'].apply(lambda x: process_row(sheet_name, x))
                df['Brief Facts'], df['Remarks'] = zip(*processed_data)

                #processed_df = df.drop(columns=['Event Facts Facts of Case', 'Event Entry Log Incident Text'])
                processed_df = df.drop(columns=['Event Entry Log Incident Text'])
                print(processed_df)
                #NOTE: Need to not drop Event Facts Facts of Case
                
                ##NOTE: Currently incoporating a mapper to change column names
                processed_df = processed_df.rename(columns=rename_dict)
                processed_df["YYYYMMDD"] = processed_df["YYYYMMDD"].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else 'N/A')
                processed_sheets[sheet_name] = processed_df
                #NOTE: Currently incoporating a mapping to rename the following columns
                    # Event Main Incident Report No. Year Month -> YYYYMMDD
                    # ⁠Event Main Incident Report No. -> Incident Report No.
                    # ⁠Event Offence Type -> Offence Type
                    # ⁠Event Person Related Name -> Person Name
                    # ⁠Event Person Related PersonID -> Person ID
                    # ⁠Event Person Related Person Type -> Person Type
                    # ⁠Event Person Related Occupation -> Occupation 

                #NOTE: Printer for time taken per row in one sheet
                now = datetime.now()
                logging.info(f"Time taken for sheet {sheet_name}: {(now - start_time)/len(df)}")

    # Save processed data to a file
    output_filename = f"processed_{datetime.now().strftime('%Y%m%d%H%M%S')}.xlsx"
    output_file_path = os.path.join(SAVE_DIR, output_filename)
    print(processed_sheets)
    with pd.ExcelWriter(output_file_path, engine='openpyxl') as writer:
        for sheet_name, df in processed_sheets.items():
            df.to_excel(writer, index=False, sheet_name=sheet_name)
    return FileResponse(
                output_file_path,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                filename=output_filename,
            )