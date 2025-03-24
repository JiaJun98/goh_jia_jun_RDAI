import os
from langchain_core.prompts import ChatPromptTemplate
from langchain.prompts import PromptTemplate
from langchain.prompts.few_shot import FewShotPromptTemplate
from langchain.document_loaders import Docx2txtLoader, TextLoader

CURR_DIR = os.getcwd()
trial_discount_several_dealers_report = Docx2txtLoader(os.path.join(CURR_DIR,"main_app/example_data/bad_discount_headache_medicine.docx")).load()
trial_discount_several_dealers_ans = TextLoader(os.path.join(CURR_DIR,"main_app/example_data/bad_discount_headache_medicine_answer.txt")).load()
bad_discount_headache_medicine_report = Docx2txtLoader(os.path.join(CURR_DIR,"main_app/example_data/bad_experience_buying_milk.docx")).load()
bad_discount_headache_medicine_answer_ans = TextLoader(os.path.join(CURR_DIR,"main_app/example_data/bad_experience_buying_milk_answer.txt")).load()
bad_experience_buying_milk_report = Docx2txtLoader(os.path.join(CURR_DIR,"main_app/example_data/trial_discount_several_dealers.docx")).load()
bad_experience_buying_milk_ans = TextLoader(os.path.join(CURR_DIR,"main_app/example_data/trial_discount_several_dealers_answer.txt")).load()


examples = [
    {
        "report": trial_discount_several_dealers_report[0].page_content,
        "answer": trial_discount_several_dealers_ans[0].page_content
    },
    {
        "report": bad_discount_headache_medicine_report[0].page_content,
        "answer": bad_discount_headache_medicine_answer_ans[0].page_content
    },
    {
        "report": bad_experience_buying_milk_report[0].page_content,
        "answer": bad_experience_buying_milk_ans[0].page_content
    }
]

base_template = """
<s> As a service quality officer, your task is to assess if an individual has been wronged during obtaining a discount.

Additional definitions for clarity:
- 'Forced discount': Discount given when the individual is in the supermarket; requiring individual to do additional things.
- 'Trial discount': A lesser amount discount given initially to assess the customers reliability before extending the full discount. </s>

[INST] Referencing the provided report, generate a summary by answering ALL 13 of the questions below. Answer each question individually and do not return the questions. When appropriate, give answers such as "Yes", "No" or "Not Applicable".
 
Questions:
1. Did the individual enquire for a discount?
2. What was the requested discount?
3. Describe the agreement details, such as additional requirements.
4. Was any discount further offered to him/her?
5. What was the amount of discount received?
6. If the full requested discount was not extended to him/her, did he/her refuse to take the trial discount?
7. If the discount was refused, was a discount 'forced' upon him/her? (i.e. given to him/her without her consent)
8. If the discount was extended, did he/she made additional givings?
9. Were miscellaneous requirements given? (i.e, additional pokemon cards)
10. Was the individual harassed for not making the requirements?
11. Was the discount surrendered to the supermarket?
12. Was the discount transferred back to the giver?
13. Did the individual cancel the discount?

Report:
{report}

Return the answer to the above 13 questions and any additional notes in point form:
{answer} [/INST]
"""

example_prompt = PromptTemplate(
    input_variables=["report", "answer"],
    template=base_template
)

summ_few_shot_prompt_template = FewShotPromptTemplate(
    examples=examples,
    example_prompt=example_prompt,
    suffix="report: {report}",
    input_variables=["report"]
)

###########################
#Return a summary
#Return yes/no -> Remove maybe (based on user)

assessment_template = """
<s> [INST] As a service quality officer, given the summary: {summary}, is the indiviudal in the summary considered wronged during the offering of a discount? 
Return 'Yes' if they have been wronged and have been subjected to additional requirements, 'No' if they have returned the full requirements based on the discount agreement, or 'Maybe' if uncertain. DO NOT RETURN MORE THAN ONE WORD, THIS IS VERY IMPORTANT TO ME. [/INST] </s>
"""

assessment_prompt_template = PromptTemplate.from_template(assessment_template)
