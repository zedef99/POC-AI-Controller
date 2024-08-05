# POC-AI-Controller
Connection between OpenAI and Anaplan to support the AFC function. 

The PoC aims to improve certain aspects that have an impact on the day-to-day activities of the AFC function and that today are very difficult to automate with the existing solution.

The 4 main focuses are:

1. **Error detection**: Detect possible outliers and anomalies in the data.

2. **Due To Analysis**: Perform a "due to" analysis on a large dataset.

3. **Executive summary**: Create a summary from the income statement

4. **Free Q&A**: create a box to allow the controller to ask spot questions.

In this repository there are two .py files:

- fixed_question.py (the code to answer question 1, 2 and 3

- free_questtion.py (the code to answer question 4)

The architecture underlying the code is the following:

![Architecture](architecture.png)


 

