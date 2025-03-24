import os, json, logging, uuid
from typing import Dict
from io import BytesIO
from datetime import datetime, date
import pandas as pd
from langchain_community.llms import Ollama
from fastapi import FastAPI, File, UploadFile, BackgroundTasks, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse

from app_base.base import AppBaseFastAPI
from app_base.database.database import get_redis_connection
from .app.api import UMSF_CHECKER_ROUTER

from main_app.llm import run, final_assessment

UMSF_HOSTNAME = os.getenv("UMSF_HOSTNAME", "umsf")

app = AppBaseFastAPI(UMSF_HOSTNAME)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # List the origins you want to allow
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(UMSF_CHECKER_ROUTER)

app = FastAPI(debug=True)
# llm = Ollama(model="mistral:7b")
llm = "mistral:7b"

logging.basicConfig(
    level=logging.DEBUG,                    # Set logging level to DEBUG
    filename="debug.log",                   # Log file name
    filemode="w",                           # Overwrite the log file each run
    format="%(asctime)s - %(levelname)s - %(message)s"  # Log format
)

# Job tracking dictionary
job_status: Dict[str, Dict] = {}

# Temporary global variable for docker volume mount
SAVE_DIR = "/tmp/volume" #NOTE: To be a temporary docker volume
if not os.path.exists(SAVE_DIR):
    os.makedirs(SAVE_DIR)

REQUIRED_COLUMNS = [
    "Event Main Incident Report No. Year Month",
    "Event Main Incident Report No.",
    "Event Offence Type",
    "Event Person Related Name",
    "Event Person Related PersonID",
    "Event Person Related Person Type",
    "Event Person Related Occupation",
    "Event Facts Facts of Case",
    "Event Entry Log Incident Text"
]

ASSESSMENT_REQUIRED_COLUMNS = [    
    "Event Facts Facts of Case",
    "Event Entry Log Incident Text"
]

EXCEL_ROW_OFFSET = 2  # Excel rows start at 1, and headers occupy the first row

#OUTPUT_COLUMNS = ["Details", "Event Facts Facts of Case", "Event Entry Log Incident Text"]

# Column renaming dictionary
rename_dict = {
    "Event Main Incident Report No. Year Month": "YYYYMMDD",
    "Event Main Incident Report No.": "Incident Report No.",
    "Event Offence Type": "Offence Type",
    "Event Person Related Name": "Person Name",
    "Event Person Related PersonID": "Person ID",
    "Event Person Related Person Type": "Person Type",
    "Event Person Related Occupation": "Occupation"
}

def validate_excel_structure(excel_file):
    """
    Validate the uploaded Excel file.
    - Check if it contains at least one non-empty row (excluding headers).
    - Check if all required columns are present.

    Returns:
    - (bool, dict): Tuple indicating whether the sheet is empty and any missing columns.
    """
    try:
        excel_data = pd.ExcelFile(excel_file)
        sheet_names = excel_data.sheet_names
        logging.info(f"Found sheet names: {sheet_names}")

        missing_columns_report = {}
        empty_rows_report = {}
        has_data = False

        for sheet_name in sheet_names:
            df = pd.read_excel(excel_data, sheet_name=sheet_name)
            # Check 1: check if dataframe is NOT empty
            if df.empty:
                logging.info(f"Sheet '{sheet_name}' is empty. Skipping further checks.")
                continue
            has_data = True
            # Check 2: check if there are any required columns that are missing 
            missing_col_set = set(REQUIRED_COLUMNS) - set(df.columns)
            if missing_col_set:
                missing_columns_report[sheet_name] = list(missing_col_set)
                logging.warning(f"Sheet '{sheet_name}' is missing required columns: {missing_col_set}")
                continue
            # Check 3: Check every row if there is data for assessment
            df_required = df[ASSESSMENT_REQUIRED_COLUMNS].astype(str).apply(lambda x: x.str.strip())
            empty_rows = df_required[df_required.isna().all(axis=1) | df_required.eq("").all(axis=1) | df_required.eq("nan").all(axis=1)].index.tolist()
            if empty_rows:
                empty_rows = [row + EXCEL_ROW_OFFSET for row in empty_rows]  # Adjust for Excel row numbering
                empty_rows_report[sheet_name] = empty_rows
                logging.warning(f"Sheet '{sheet_name}' has empty rows at: {empty_rows}")

        return has_data, missing_columns_report, empty_rows_report
    except Exception as e:
        logging.error(f"Error during Excel validation: {str(e)}")
        raise HTTPException(status_code=500, detail="Error while validating the Excel file.")

@app.post("/get-excel-preview/", responses={
    200: {
        "description": "Successfully processed Excel file.",
        "content": {
            "application/json": {
                "example": {
                    "Sheet1": [
                        {
                            "Details": "Event Main Incident Report No.: 2023\nEvent Offence Type: Theft",
                            "Event Facts Facts of Case": "Some details about the incident",
                            "Event Entry Log Incident Text": "Some entry log text"
                        }
                    ]
                }
            }
        }
    },
    400: {
        "description": "Bad Request - Missing columns, invalid Excel file, or no valid data found.",
        "content": {
            "application/json": {
                "example": {"detail": "Missing columns found in one or more sheets."}
            }
        }
    },
    415: {
        "description": "Unsupported Media Type - Invalid file format uploaded.",
        "content": {
            "application/json": {
                "example": {"detail": "Invalid file type. Please upload an Excel file."}
            }
        }
    },
    500: {
        "description": "Internal Server Error - Unexpected issue occurred.",
        "content": {
            "application/json": {
                "example": {"detail": "An error occurred while processing the Excel file."}
            }
        }
    }
})
async def get_excel_preview(file: UploadFile = File(...)):
    """
    Endpoint to provide a preview of Excel to be displayed on the web app.

    Args:
    - file: Excel file to be uploaded.

    Returns:
    - JSON: Preview of the Excel file in the specified format.
    """
    # Validate file type
    if file.content_type not in [
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/vnd.ms-excel",
    ]:
        logging.error("Invalid file type uploaded.")
        raise HTTPException(status_code=415, detail="Invalid file type. Please upload an Excel file.")

    try:
        file_content = await file.read()
        excel_file = BytesIO(file_content)

        # Validate file structure
        excel_file.seek(0)
        has_data, missing_columns_report, empty_rows_report = validate_excel_structure(excel_file)

        if not has_data:
            logging.warning("Uploaded Excel file is empty.")
            raise HTTPException(status_code=400, detail={"error": "The uploaded file is empty."})

        if missing_columns_report:
            logging.warning(f"Missing columns found: {missing_columns_report}")
            raise HTTPException(status_code=400, detail={
                "error": "Missing columns found in one or more sheets.",
                "missing_columns_report": missing_columns_report
            })
        
        if empty_rows_report:
            logging.warning(f"Missing data in LLM required info, empty rows found: {empty_rows_report}")
            raise HTTPException(status_code=400, detail={
                "error": "Missing data required for assessment, empty rows found.",
                "empty_rows_report": empty_rows_report
            })

        # Reset file pointer for reading data
        excel_file.seek(0)
        excel_data = pd.ExcelFile(excel_file)
        sheet_names = excel_data.sheet_names
        logging.info(f"Processing sheets: {sheet_names}")

        final_json = {}
        for sheet_name in sheet_names:
            df = pd.read_excel(excel_data, sheet_name=sheet_name)
            if df.empty:
                continue  # Skip empty sheets

            df_filtered_and_ordered = df.loc[:, REQUIRED_COLUMNS].astype(str)
            df_filtered_and_ordered = df_filtered_and_ordered.rename(columns=rename_dict)

            final_json[sheet_name] = df_filtered_and_ordered.to_dict(orient="records")

        if not final_json:
            logging.warning("No valid data found in any sheet.")
            raise HTTPException(status_code=400, detail="Excel file contains no valid data.")

        return final_json

    except HTTPException:
        raise 
    except Exception as e:
        logging.error(f"Error processing Excel file: {str(e)}")
        raise HTTPException(status_code=500, detail="An error occurred while processing the Excel file.")

@app.post("/upload-excel-with-task-id/", responses={
    200: {
        "description": "Successfully uploaded Excel file and processing started.",
        "content": {
            "application/json": {
                "example": {
                    "task_id": "a1b2c3d4-e5f6-7g8h-9i0j-k1l2m3n4o5p6",
                    "message": "File is being processed in the background"
                }
            }
        }
    },
    400: {
        "description": "Bad Request - Invalid file format or validation failed.",
        "content": {
            "application/json": {
                "example": {"detail": "Excel validation failed: Missing required columns."}
            }
        }
    },
    415: {
        "description": "Unsupported Media Type - Invalid file type uploaded.",
        "content": {
            "application/json": {
                "example": {"detail": "Invalid file type. Please upload an Excel file."}
            }
        }
    },
    500: {
        "description": "Internal Server Error - An unexpected error occurred.",
        "content": {
            "application/json": {
                "example": {"detail": "An error occurred while uploading the Excel file."}
            }
        }
    }
})
async def upload_excel_task_id(file: UploadFile = File(...), background_tasks: BackgroundTasks = BackgroundTasks()):
    """
    Endpoint to process an Excel file using the UMSF Gen-AI algorithm.

    Args:
    - file (UploadFile): Excel file to be uploaded.
    - background_tasks (BackgroundTasks): FastAPI background tasks to process the file.

    Returns:
    - JSON: Contains the task ID and a message indicating processing has started.
    """
    # Validate file type
    if file.content_type not in [
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/vnd.ms-excel",
    ]:
        logging.error("Invalid file type uploaded.")
        raise HTTPException(status_code=415, detail="Invalid file type. Please upload an Excel file.")

    # Generate a unique task ID
    task_id = str(uuid.uuid4())
    job_status[task_id] = {"status": "In Progress", "progress": 0, "result": None}

    try:
        # Read the file into memory
        file_content = await file.read()
        excel_file = BytesIO(file_content)

        # Validate Excel structure
        try:
            has_data, missing_columns_report, empty_rows_report = validate_excel_structure(excel_file)
        except ValueError as ve:
            logging.error(f"Validation error: {str(ve)}")
            raise HTTPException(status_code=400, detail=f"Excel validation failed: {str(ve)}")
        except Exception as e:
            logging.error(f"Unexpected error during validation: {str(e)}")
            raise HTTPException(status_code=500, detail="Unexpected error during validation.")

        if not has_data:
            logging.warning("Uploaded Excel file is empty.")
            raise HTTPException(status_code=400, detail={"error": "The uploaded file is empty."})

        if missing_columns_report:
            logging.warning(f"Missing columns found: {missing_columns_report}")
            raise HTTPException(status_code=400, detail={
                "error": "Missing columns found in one or more sheets.",
                "missing_columns_report": missing_columns_report
            })
        
        if empty_rows_report:
            logging.warning(f"Missing data in LLM required info, empty rows found: {empty_rows_report}")
            raise HTTPException(status_code=400, detail={
                "error": "Missing data required for assessment, empty rows found.",
                "empty_rows_report": empty_rows_report
            })

        # Add task to background processing
        background_tasks.add_task(process_file, file_content, task_id)
        logging.info(f"Task {task_id} is being processed in the background.")
        
        return {"task_id": task_id, "message": "File is being processed in the background"}
    except HTTPException:
        raise
    except Exception as e:
        error_message = str(e)
        logging.error(f"Unhandled error during task creation: {error_message}")
        raise HTTPException(status_code=500, detail=f"Unhandled error during task creation: {error_message}")

@app.get("/job-status/{task_id}", responses={
    200: {
        "description": "Successfully retrieved task status.",
        "content": {
            "application/json": {
                "example": {
                    "task_id": "1234",
                    "status": "In Progress",
                    "progress": 50
                }
            }
        }
    },
    404: {
        "description": "Task ID not found.",
        "content": {
            "application/json": {
                "example": {"detail": "Task ID not found."}
            }
        }
    },
    500: {
        "description": "Internal Server Error.",
        "content": {
            "application/json": {
                "example": {"detail": "An error occurred while retrieving task status."}
            }
        }
    }
})
async def get_job_status(task_id: str):
    """
    Endpoint to get the status of a specific task by Task ID.

    Args:
    - task_id: Unique Task ID of the uploaded Excel file being processed.

    Returns:
    - JSON: Contains the task ID, current status, and progress percentage.
    """
    try:
        # Check if task_id exists in job_status
        if task_id not in job_status:
            logging.warning(f"Task ID '{task_id}' not found.")
            raise HTTPException(status_code=404, detail="Task ID not found.")

        # Retrieve status information
        status_info = job_status[task_id]
        return {
            "task_id": task_id,
            "status": status_info["status"],
            "progress": status_info.get("progress", None),
            "time_remaining": status_info.get("time_remaining", None)
        }
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error retrieving job status for Task ID '{task_id}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error retrieving job status for Task ID '{task_id}': {str(e)}")

def process_file(file_content: bytes, task_id: str):
    """
    Background function to process the uploaded file.

    Args:
    - bytes: File content of the Excel file to be uploaded
    - task_id: Unique Task ID of the uploaded excel file being processed

    Returns:
    """
    try:
        logging.info(f"Starting processing for task_id: {task_id}")
        processed_sheets = {}
        start_time = datetime.now()
        today = date.today()
        
        # Use BytesIO to handle the file content
        excel_file = BytesIO(file_content)
        sheet_names = pd.ExcelFile(excel_file).sheet_names #NOTE: Need to capture the time
        logging.debug(f"Sheet names: {sheet_names}")

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
            remarks = final_assessment(sheet_name, assessment)

            rows_processed += 1
            progress_percentage = min(int((rows_processed / total_rows) * 100), 100)
            job_status[task_id]["progress"] = progress_percentage
            time_now = datetime.now()
            time_so_far = time_now - start_time
            approx_time_remaining = (time_so_far/progress_percentage)*(100-progress_percentage)
            total_seconds = int(approx_time_remaining.total_seconds())  # Convert timedelta to seconds
            minutes = total_seconds // 60  # Get the number of whole minutes
            seconds = total_seconds % 60  # Get the remaining seconds
            job_status[task_id]["time_remaining"] = (minutes, seconds)
            logging.debug(f"Progress updated to {progress_percentage}%")
            logging.debug(f"Time remaining updated to {minutes} Mins {seconds} Secs")

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

        # Save processed data to a mounted drive
        output_filename = f"Debtors_FW_and_FDW_{today.month} {today.year} (For MOM)"
        output_file_path = os.path.join(SAVE_DIR, f"{output_filename}.xlsx")
        with pd.ExcelWriter(output_file_path, engine="openpyxl") as writer:
            for sheet_name, df in processed_sheets.items():
                df.to_excel(writer, index=False, sheet_name=sheet_name)
        end_time = datetime.now()
        duration = end_time - start_time
        logging.info(f"Time taken for task_id {task_id}: {duration}")

        job_status[task_id] = {
            "status": "Done",
            "progress": 100,
            "result": {
                "file_path": output_file_path,
                "file_name": output_filename
            }
        }
        logging.info(f"Processing completed for task_id {task_id}")
    except Exception as e:
        error_message = str(e)
        logging.error(f"Error processing task {task_id}: {error_message}")
        job_status[task_id] = {"status": "Failed", "progress": 0, "error": error_message}

@app.get("/download/{task_id}", responses={
    200: {
        "description": "Successfully retrieved the processed file as JSON.",
        "content": {
            "application/json": {
                "example": {
                    "Sheet1": [
                        {"ColumnA": "Value1", "ColumnB": "Value2"},
                        {"ColumnA": "Value3", "ColumnB": "Value4"}
                    ],
                    "Sheet2": [
                        {"ColumnC": "Value5", "ColumnD": "Value6"}
                    ]
                }
            }
        }
    },
    400: {
        "description": "Bad Request - Task is not ready for download.",
        "content": {
            "application/json": {
                "example": {"detail": "File not ready."}
            }
        }
    },
    404: {
        "description": "Task ID not found or processed file does not exist.",
        "content": {
            "application/json": {
                "example": {"detail": "Task ID not found."}
            }
        }
    },
    500: {
        "description": "Internal Server Error - Failed to process the file.",
        "content": {
            "application/json": {
                "example": {"detail": "Failed to process the file: Unexpected error."}
            }
        }
    }
})
async def download_json(task_id: str):

    """
    Endpoint to download the JSON of the processed Excel file by Task ID.

    Args:
    - task_id: Unique Task ID of the uploaded Excel file being processed.

    Returns:
    - JSON: Contains the Excel file data converted into a JSON format.
    """
    # Check if task ID exists
    if task_id not in job_status:
        logging.error(f"Task ID not found: {task_id}")
        raise HTTPException(status_code=404, detail="Task ID not found.")

    # Check if the task is completed
    task_status = job_status[task_id]["status"]
    if task_status != "Done":
        logging.warning(f"Task {task_id} is not ready. Current status: {task_status}")
        raise HTTPException(status_code=400, detail="File not ready.")

    result = job_status[task_id]["result"]
    file_path = result.get("file_path")

    # return FileResponse(
    #     path=result["file_path"],
    #     media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    #     filename=result["file_name"]
    # )

    try:
        # Check if the file exists
        if not file_path or not os.path.exists(file_path):
            logging.error(f"File not found for Task ID '{task_id}': {file_path}")
            raise HTTPException(status_code=404, detail="Processed file not found.")
        # Process the Excel file and convert it to JSON
        all_sheets_data = {}
        with pd.ExcelFile(file_path) as excel_data:
            for sheet_name in excel_data.sheet_names:
                # Read each sheet into a DataFrame
                df = pd.read_excel(excel_data, sheet_name=sheet_name)

                # Replace NaN and infinite values with None
                df = df.replace({pd.NA: None, float("nan"): None, float("inf"): None, float("-inf"): None}).astype(str)
                # logging.info(f"Current sheet_name: {sheet_name}, Type: {type(sheet_name)}")
                # logging.info(f"Remarks column: {df['Remarks']}")
                # df['Remarks'] = df['Remarks'] + sheet_name
                # Convert DataFrame to JSON-compliant dictionary
                all_sheets_data[sheet_name] = df.to_dict(orient="records")

        logging.info(f"Task {task_id} successfully processed and returned as JSON.")
        return JSONResponse(content=all_sheets_data)

    except ValueError as ve:
        # Handle Excel-specific errors
        logging.error(f"ValueError while reading Excel file for Task ID '{task_id}': {str(ve)}")
        raise HTTPException(status_code=400, detail=f"Invalid Excel file: {str(ve)}")
    except HTTPException:
        raise
    except Exception as e:
        # Handle unexpected errors
        logging.error(f"Unexpected error processing Task ID '{task_id}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"Unexpected error processing Task ID '{task_id}': {str(e)}")

@app.post("/upload-excel/", responses={
    200: {
        "description": "Successfully processed Excel file.",
        "content": {
            "application/json": {
                "example": {
                    "Sheet1": [
                        {"Brief Facts": "Summary1", "Remarks": "Assessment1"},
                        {"Brief Facts": "Summary2", "Remarks": "Assessment2"}
                    ],
                    "Sheet2": [
                        {"Brief Facts": "Summary3", "Remarks": "Assessment3"}
                    ]
                }
            }
        }
    },
    400: {
        "description": "Bad Request - Invalid file type or validation failure.",
        "content": {
            "application/json": {
                "example": {"detail": "Excel validation failed: Missing required columns."}
            }
        }
    },
    415: {
        "description": "Unsupported Media Type - Invalid file format uploaded.",
        "content": {
            "application/json": {
                "example": {"detail": "Invalid file type. Please upload an Excel file."}
            }
        }
    },
    500: {
        "description": "Internal Server Error - Failed to process the Excel file.",
        "content": {
            "application/json": {
                "example": {"detail": "Failed to process the Excel file: Unexpected error occurred."}
            }
        }
    }
})
async def upload_excel(file: UploadFile = File(...)):
    """
    Endpoint to process an Excel file based on the UMSF Gen-AI algorithm.

    Args:
    - file: Excel file to be uploaded.

    Returns:
    - JSON: JSON representation of the processed Excel file.
    """
    # Validate file type
    if file.content_type not in [
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/vnd.ms-excel",
    ]:
        logging.error("Invalid file type uploaded.")
        raise HTTPException(status_code=415, detail="Invalid file type. Please upload an Excel file.")

    try:
        start_time = datetime.now()
        content = await file.read() #file.file not possible
        # Use BytesIO to handle the file content
        excel_file = BytesIO(content)

        # Validate Excel structure
        try:
            has_data, missing_columns_report, empty_rows_report = validate_excel_structure(excel_file)
        except ValueError as ve:
            logging.error(f"Validation error: {str(ve)}")
            raise HTTPException(status_code=400, detail=f"Excel validation failed: {str(ve)}")
        except Exception as e:
            logging.error(f"Unexpected error during validation: {str(e)}")
            raise HTTPException(status_code=500, detail="Unexpected error during validation.")

        if not has_data:
            logging.warning("Uploaded Excel file is empty.")
            raise HTTPException(status_code=400, detail={"error": "The uploaded file is empty."})

        if missing_columns_report:
            logging.warning(f"Missing columns found: {missing_columns_report}")
            raise HTTPException(status_code=400, detail={
                "error": "Missing columns found in one or more sheets.",
                "missing_columns_report": missing_columns_report
            })
        
        if empty_rows_report:
            logging.warning(f"Missing data in LLM required info, empty rows found: {empty_rows_report}")
            raise HTTPException(status_code=400, detail={
                "error": "Missing data required for assessment, empty rows found.",
                "empty_rows_report": empty_rows_report
            })

        sheet_names = pd.ExcelFile(content).sheet_names #NOTE: Need to capture the time
        column_headers = pd.read_excel(content, sheet_name=0,nrows=0).columns

        processed_sheets = {}
        for sheet_name in sheet_names:
            df = pd.read_excel(content, sheet_name=sheet_name)
            if not df.empty:
                df.columns = column_headers
                df = df.rename(columns={"Breif Facts": "Event Facts Facts of Case"}) #NOTE: Might need to change depending on column needed
                df['results'] = df['Event Facts Facts of Case'].apply(lambda x: run(x, llm))
                df["Event Main Incident Report No. Year Month"] = df["Event Main Incident Report No. Year Month"].dt.strftime('%Y-%m-%d').fillna('N/A') 
                df['Brief Facts'] = df['results'].apply(lambda x: x['summary'].strip())
                df['Remarks'] = df['results'].apply(lambda x: x['assessment'].strip())
                df['Remarks'] = df['Remarks'].apply(lambda x: final_assessment(x))
                
                processed_df = df.drop(columns=['Event Facts Facts of Case', 'results', 'Event Entry Log Incident Text'])
                #NOTE: Currently incoporating a mapping to rename the following columns
                #Event Main Incident Report No. Year Month -> YYYYMMDD
                # ⁠Event Main Incident Report No. -> Incident Report No.
                # ⁠Event Offence Type -> Offence Type
                # ⁠Event Person Related Name -> Person Name
                # ⁠Event Person Related PersonID -> Person ID
                # ⁠Event Person Related Person Type -> Person Type
                # ⁠Event Person Related Occupation -> Occupation 
                processed_df = processed_df.rename(columns=rename_dict)
                processed_sheets[sheet_name] = processed_df
                #NOTE: Printer for time taken per row in one sheet
                now = datetime.now()
                print(f"Time taken in {sheet_name} per row: {(now - start_time)/len(df)}")
        #NOTE: For testing timing
        end_time = datetime.now()
        duration = end_time - start_time
        print(f"Total time taken: {duration}")
        json_data = {sheet_name: json.loads(df.to_json(orient="records")) for sheet_name, df in processed_sheets.items()}
        return JSONResponse(json_data)
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Unhandled error processing Excel file: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Unhandled error processing Excel file: {str(e)}")

@app.post("/save-and-download-excel/", responses={
    200: {
        "description": "Successfully generated and returned the Excel file.",
        "content": {
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": {}
        }
    },
    400: {
        "description": "Bad Request - Invalid or empty input data.",
        "content": {
            "application/json": {
                "example": {"detail": "Invalid or empty input data."}
            }
        }
    },
    500: {
        "description": "Internal Server Error - Failed to generate the Excel file.",
        "content": {
            "application/json": {
                "example": {"detail": "An error occurred while generating the Excel file: unexpected error."}
            }
        }
    }
})
async def save_and_download_excel(data: dict = Body(...)):
    # Testing examples in FASTAPI
    #     {
    #   "Sheet1": [
    #     {"ColA": "Value1A", "ColB": "Value1B", "ColC": "Value1C"},
    #     {"ColA": "Value2A", "ColB": "Value2B", "ColC": "Value2C"}
    #   ],
    #   "Sheet2": [
    #     {"ColA": "Value3A", "ColB": "Value3B", "ColC": "Value3C"},
    #     {"ColA": "Value4A", "ColB": "Value4B", "ColC": "Value4C"}
    #   ]
    # }
    """
    Combines edited DataFrames from frontend into a .xlsx file and returns it as a downloadable file.

    Args:
    - data (dict): A dictionary where keys are sheet names, and values are JSON representations of DataFrames.

    Returns:
    - FileResponse: The combined Excel file.
    """
    today = date.today()

    # Validate input data
    if not data or not isinstance(data, dict):
        logging.error("Invalid or empty input data for Excel generation.")
        raise HTTPException(status_code=400, detail="Invalid or empty input data.")

    try:
        sheets = {}
        for sheet_name, sheet_data in data.items():
            if not isinstance(sheet_data, list):
                logging.error(f"Invalid data format for sheet '{sheet_name}'. Expected a list of dictionaries.")
                raise HTTPException(status_code=400, detail=f"Invalid data format for sheet '{sheet_name}'.")

            # Convert input data to a DataFrame
            sheets[sheet_name] = pd.DataFrame(sheet_data)

        # Generate the Excel file
        output_filename = f"Debtors_FW_and_FDW_{today.month}_{today.year}.xlsx"
        output_file_path = os.path.join(SAVE_DIR, output_filename)

        with pd.ExcelWriter(output_file_path, engine="openpyxl") as writer:
            for sheet_name, df in sheets.items():
                df.replace(to_replace=[None, "None"], value="", inplace=True)
                df.to_excel(writer, index=False, sheet_name=sheet_name)

        logging.info(f"Excel file generated successfully: {output_file_path}")

        # Return the Excel file for download
        return FileResponse(
            output_file_path,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=output_filename,
        )
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error generating Excel file: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred while generating the Excel file: {str(e)}")