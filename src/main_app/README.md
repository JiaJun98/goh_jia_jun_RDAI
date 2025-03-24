1. Change directory to this folder 
    - cd src/main_app
2. Start your virtual environment
    - source venv/bin/activate
    - venv setup:
        - Use python 3.13
        - pip install -r requirements.txt
3. Ensure Ollama is up and running
    - ollama start 
4. Start the server
    - uvicorn api_test:app --reload
5. Access the API docs
    - on your browser of choice, access the following url
    - http://127.0.0.1:8000/docs#/