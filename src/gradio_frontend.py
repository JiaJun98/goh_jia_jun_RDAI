import gradio as gr
import requests
import os

def upload_and_process(file, progress=gr.Progress()):
    if file is None:
        return "No file uploaded.", None

    # Send the file to the FastAPI backend
    response = requests.post(
        "http://localhost:8000/process-excel/",
        files={"file": (file.name, file, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')}
    )

    if response.status_code == 200:
        result = response.json()
        download_path = result.get("file_path", None)
        download_name = result.get("file_name", None)

        if download_path and os.path.exists(download_path):
            return f"File processed successfully.", download_path
        else:
            return "Processing completed, but the file could not be found.", None
    else:
        return "Error processing file.", None

with gr.Blocks() as demo:
    gr.Markdown("### Upload and Process Excel File")
    with gr.Row():
        with gr.Column():
            file_input = gr.File(label="Upload Excel File")
            process_button = gr.Button("Process")
            
        with gr.Column():
            status = gr.Textbox(label="Status")
            download_link = gr.File(label="Download Processed File")

    process_button.click(
        upload_and_process,
        inputs=file_input,
        outputs=[status, download_link]
    )

if __name__ == "__main__":
    demo.launch()
