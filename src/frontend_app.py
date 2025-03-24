import streamlit as st
import requests
import os


def upload_and_process():
    st.title("LLM Excel Processor")
    file = st.file_uploader("Upload Excel File", type=["xlsx", "xls"])

    if file is not None:
        with st.spinner("Processing your file... Please wait."):
            # Send the file to FastAPI for processing
            response = requests.post(
                "http://main_app:8000/process-excel/",  # Replace with the correct FastAPI URL
                files={"file": (file.name, file.getvalue(), 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')}
            )

        if response.status_code == 200:
            # Retrieve the processed file from the response
            processed_file_content = response.content

            # Provide a download button for the processed file
            st.download_button(
                label="Download Processed File",
                data=processed_file_content,
                file_name=f"processed_{file.name}",
                mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
            st.success("File processed successfully.")
        else:
            st.error("Error processing file.")

if __name__ == "__main__":
    upload_and_process()

