import re, os, requests, json
from dotenv import load_dotenv
from langchain.chains import LLMChain
from langchain.schema.output_parser import StrOutputParser
from langchain_core.runnables import RunnablePassthrough
from langchain_community.llms import Ollama
from dotenv import load_dotenv

# from main_app.template 
from main_app.template import summ_few_shot_prompt_template, assessment_prompt_template

#Need some data processing based on what the user wants...
load_dotenv()

class LLMChain:
    def __init__(self, llm):
        self.llm = llm

class OllamaAPIWrapper:
    def __init__(self, model: str = "mistral:7b"):
        self.model = model

    def __call__(self, prompt) -> str:
        if isinstance(prompt, str):
            prompt_str = prompt
        else:
            prompt_str = str(prompt)

        url = "http://ollama:11434/api/generate"
        payload = {
            "model": self.model,
            "prompt": prompt_str
        }
        try:
            response = requests.post(url, json=payload, stream=True)
            response.raise_for_status()

            # Process the response line by line
            final_response = ""
            for line in response.iter_lines():
                if line:
                    try:
                        line_json = json.loads(line)
                        final_response += line_json.get("response", "")
                    except json.JSONDecodeError as e:
                        raise RuntimeError(f"Error parsing JSON line: {line.decode('utf-8')} - {e}")

            return final_response.strip()
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Error communicating with Ollama API: {e}")

class CustomChain(LLMChain):
    def __init__(self, llm):
        super().__init__(llm)
        
        # Prompts for summary and assessment
        summary_prompt = summ_few_shot_prompt_template
        assessment_prompt = assessment_prompt_template

        # Define the summary chain
        summary_chain = {"report": RunnablePassthrough()} | summary_prompt | llm | StrOutputParser()

        # Define the assessment chain
        assessment_chain = assessment_prompt | llm | StrOutputParser()

        # Combine chains into a custom chain
        self.custom_chain = (
            {"summary": summary_chain}
            | RunnablePassthrough.assign(assessment=assessment_chain)
        )

    def get_chain(self):
        """
        Returns the constructed custom chain.
        """
        return self.custom_chain

def create_chain(llm):
    """
    Create a custom chain using the provided language model.

    Args:
    - llm: A callable language model instance.

    Returns:
    - A combined summary and assessment chain.
    """
    chain_instance = CustomChain(llm)
    return chain_instance.get_chain()

def run(input_data, model="mistral:7b"):
    """
    Executes the CustomChain and returns the summary and assessment.

    Args:
    - input_data: The input data for the chain.
    - model: The Ollama model to use (default is "mistral:7b").

    Returns:
    - dict: A dictionary containing 'summary' and 'assessment'.
    """
    print("\nCalling Ollama API via CustomChain\n") 
    llm = OllamaAPIWrapper(model=model)  # Use the API wrapper as the LLM
    chain = create_chain(llm)
    results = chain.invoke(input_data)
    print("Returning LLM result")  
    return results

def final_assessment(sheet_name, llm_is_wronged_text):
    """
    Filters the text output of the LLM and returns Yes, No or Maybe
    YES > CATX has been wronged
    NO > CATX is not wronged

    Args:
        llm_is_wronged_text (str): text output of the row of "llm_is_wronged_text"

    Returns:
        string: "Yes", "No" or "Maybe"
    """
    if "maybe" in llm_is_wronged_text.lower():
        return "Not sure"
    elif "yes" in llm_is_wronged_text.lower():
        return sheet_name + " has been wronged"
    else:
        return sheet_name + " has not been wronged"

