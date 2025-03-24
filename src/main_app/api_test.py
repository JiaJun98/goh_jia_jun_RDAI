from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from langchain_community.llms import Ollama
import pandas as pd
from datetime import datetime, date
import os
from io import BytesIO
from uuid import uuid4
from time import time
from datetime import datetime
import uvicorn

from llm import run, final_assessment

app = FastAPI(debug=True)
llm = Ollama(model="mistral:7b")

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
SAVE_DIR = "processed_files"
os.makedirs(SAVE_DIR, exist_ok=True)

# Dictionary to track job statuses
job_status = {}

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

    def process_row(row):
        """
        Helper function to process a single row, applying all transformations.
        Updates progress after processing the row.
        """
        nonlocal rows_processed
        results = run(row, llm)
        summary = results['summary'].strip()
        assessment = results['assessment'].strip()
        remarks = final_assessment(assessment)

        rows_processed += 1
        progress_percentage = min(int((rows_processed / total_rows) * 100), 100)
        #job_status[task_id]["progress"] = progress_percentage
        logging.debug(f"Progress updated to {progress_percentage}%")

        return summary, remarks

    for sheet_name in sheet_names:
        df = pd.read_excel(excel_file, sheet_name=sheet_name)
        column_headers = pd.read_excel(excel_file, sheet_name=0, nrows=0).columns
        df.columns = column_headers
        df = df.rename(columns={"Breif Facts": "Event Facts Facts of Case"})
        
        # Apply the process_row function to each row
        processed_data = df['Event Facts Facts of Case'].apply(process_row)
        df['Brief Facts'], df['Remarks'] = zip(*processed_data)

        processed_df = df.drop(columns=['Event Facts Facts of Case', 'Event Entry Log Incident Text'])
        processed_sheets[sheet_name] = processed_df

        #NOTE: Printer for time taken per row in one sheet
        now = datetime.now()
        logging.info(f"Time taken for sheet {sheet_name}: {(now - start_time)/len(df)}")


    # Save processed data to a file
    output_filename = f"processed_{datetime.now().strftime('%Y%m%d%H%M%S')}.xlsx"
    output_file_path = os.path.join(SAVE_DIR, output_filename)
    with pd.ExcelWriter(output_file_path, engine='openpyxl') as writer:
        for sheet_name, df in processed_sheets.items():
            df.to_excel(writer, index=False, sheet_name=sheet_name)
    return FileResponse(output_file_path, filename=output_filename)

# def process_file(file_content: bytes, task_id: str):
#     """
#     Background function to process the uploaded file.

#     Args:
#     - file_content: content of the Excel file to be uploaded
#     - task_id: unique Task ID of the uploaded excel file being processed

#     Returns:
#     """
#     try:
#         logging.info(f"Starting processing for task_id: {task_id}")
#         processed_sheets = {}
#         start_time = datetime.now()
#         today = date.today()
        
#         # Use BytesIO to handle the file content
#         excel_file = BytesIO(file_content)
#         sheet_names = pd.ExcelFile(excel_file).sheet_names #NOTE: Need to capture the time
#         logging.debug(f"Sheet names: {sheet_names}")

#         # Calculate total rows across all sheets
#         total_rows = 0
#         for sheet_name in sheet_names:
#             df = pd.read_excel(excel_file, sheet_name=sheet_name) # Read the uploaded xlsx file into a pandas DataFrame
#             total_rows += len(df)
#         logging.info(f"Total rows to process: {total_rows}")
        
#         # Rewind the file for actual processing
#         excel_file.seek(0)

#         rows_processed = 0

#         def process_row(row):
#             """
#             Helper function to process a single row, applying all transformations.
#             Updates progress after processing the row.
#             """
#             nonlocal rows_processed
#             results = run(row, llm)
#             summary = results['summary'].strip()
#             assessment = results['assessment'].strip()
#             remarks = final_assessment(assessment)

#             rows_processed += 1
#             progress_percentage = min(int((rows_processed / total_rows) * 100), 100)
#             job_status[task_id]["progress"] = progress_percentage
#             logging.debug(f"Progress updated to {progress_percentage}%")

#             return summary, remarks
        
#         for sheet_name in sheet_names:
#             logging.debug(f"Processing sheet: {sheet_name}")
#             df = pd.read_excel(excel_file, sheet_name=sheet_name)
#             column_headers = pd.read_excel(excel_file, sheet_name=0, nrows=0).columns
#             df.columns = column_headers
#             df = df.rename(columns={"Breif Facts": "Event Facts Facts of Case"})

#             # Apply the process_row function to each row
#             processed_data = df['Event Facts Facts of Case'].apply(process_row)
#             df['Brief Facts'], df['Remarks'] = zip(*processed_data)

#             processed_df = df.drop(columns=['Event Facts Facts of Case', 'Event Entry Log Incident Text'])
#             processed_sheets[sheet_name] = processed_df

#             #NOTE: Printer for time taken per row in one sheet
#             now = datetime.now()
#             logging.info(f"Time taken for sheet {sheet_name}: {(now - start_time)/len(df)}")

#         # Save processed data to a mounted drive
#         output_filename = f"Debtors_FW_and_FDW_{today.month} {today.year} (For MOM)"
#         output_file_path = os.path.join(SAVE_DIR, f"{output_filename}.xlsx")
#         with pd.ExcelWriter(output_file_path, engine="openpyxl") as writer:
#             for sheet_name, df in processed_sheets.items():
#                 df.to_excel(writer, index=False, sheet_name=sheet_name)
#         end_time = datetime.now()
#         duration = end_time - start_time
#         logging.info(f"Time taken for task_id {task_id}: {duration}")

#         job_status[task_id] = {
#             "status": "Done",
#             "progress": 100,
#             "result": {
#                 "file_path": output_file_path,
#                 "file_name": output_filename
#             }
#         }
#         logging.info(f"Processing completed for task_id {task_id}")
#     except Exception as e:
#         error_message = str(e)
#         logging.error(f"Error processing task {task_id}: {error_message}")
#         job_status[task_id] = {"status": "Failed", "progress": 0, "error": error_message}


if __name__ == "__main__":
    uvicorn.run("api_test:app", host="0.0.0.0", port=8000, reload=True)