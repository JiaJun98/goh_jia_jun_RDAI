import os
from langchain_core.prompts import ChatPromptTemplate
from langchain.prompts import PromptTemplate
from langchain.prompts.few_shot import FewShotPromptTemplate
from langchain.document_loaders import Docx2txtLoader, TextLoader

CURR_DIR = os.getcwd()
trial_loan_report = Docx2txtLoader(os.path.join(CURR_DIR,"umsf_example_data/G202203272051 - trial loan_several UMLs.docx")).load()
trial_loan_ans = TextLoader(os.path.join(CURR_DIR,"umsf_example_data/trial_loan_answer.txt")).load()
enquire_force_loan_report = Docx2txtLoader(os.path.join(CURR_DIR,"umsf_example_data/G202307112064 - Enquire_given forced loan.docx")).load()
enquire_force_loan_ans = TextLoader(os.path.join(CURR_DIR,"umsf_example_data/forced_loan_answer.txt")).load()
refuse_full_payment_report = Docx2txtLoader(os.path.join(CURR_DIR,"umsf_example_data/L202307142070 - admit borrow_UML refused full payment.docx")).load()
refuse_full_payment_ans = TextLoader(os.path.join(CURR_DIR,"umsf_example_data/payment_refused_answer.txt")).load()
make_repayment_report = Docx2txtLoader(os.path.join(CURR_DIR,"umsf_example_data/G202307122049 - admit being a UML borrower.docx")).load()
make_repayment_ans = TextLoader(os.path.join(CURR_DIR, "umsf_example_data/admit_borrower_answer.txt")).load()


examples = [
    {
        "report": trial_loan_report[0].page_content,
        "answer": trial_loan_ans[0].page_content
    },
    {
        "report": enquire_force_loan_report[0].page_content,
        "answer": enquire_force_loan_ans[0].page_content
    },
    {
        "report": refuse_full_payment_report[0].page_content,
        "answer": refuse_full_payment_ans[0].page_content
    },
    {
        "report": make_repayment_report[0].page_content,
        "answer": make_repayment_ans[0].page_content
    }
]

base_template = """
<s> As a police investigation officer, your task is to assess if an individual is involved with an Unlicensed Money Lender (UML).

Additional definitions for clarity:
- 'Forced loan': Money transferred to an individual's account without consent after they refused loan terms, with demands for repayments.
- 'Trial loan': A lesser amount loan given initially to assess the borrower's reliability before extending the full amount. </s>

[INST] Referencing the provided report, generate a summary by answering ALL 13 of the questions below. Answer each question individually and do not return the questions. When appropriate, give answers such as "Yes", "No" or "Not Applicable".
 
Questions:
1. Did the individual enquire for a loan from a money lender?
2. What was the requested loan amount?
3. Describe the loan agreement details, such as interest and repayment period.
4. Was any loan extended to him/her?
5. What was the amount of loan received?
6. If the full requested amount was not extended to him/her, did he/her refuse to take the trial loan?
7. If the loan was refused, was a loan 'forced' upon him/her? (i.e. given to him/her without her consent)
8. If the loan was extended (be it trial, forced, or actual loan amount), did he/she make repayments?
9. Were miscellaneous fees paid to the moneylender? (i.e, admin fee, cancellation fee, lawyer fee)
10. Was the individual harassed for not making repayments?
11. Was the loan surrendered to the police?
12. Was the loan transferred back to the money lender?
13. Did the individual cancel the loan?

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
<s> [INST] As a police investigator, given the summary: {summary}, is the foreign worker in the summary considered an UML (Unlicensed Money Lender) borrower? 
Return 'Yes' if they have taken a loan and did not return the full amount based on the load agreement, 'No' if they have returned the full amount based on the loan agreement, or 'Maybe' if uncertain. DO NOT RETURN MORE THAN ONE WORD, THIS IS VERY IMPORTANT TO ME. [/INST] </s>
"""

assessment_prompt_template = PromptTemplate.from_template(assessment_template)
