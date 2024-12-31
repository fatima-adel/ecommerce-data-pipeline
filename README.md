# Data Engineering Project - Ready D25 &nbsp; &nbsp; &nbsp;<img align="center" width="50" alt="image" src="https://github.com/Ready-Talent/data-engineering-project-template/assets/70844012/4c10ad15-6b48-4ce3-9829-9e823191b410">

## Project Description 
You work in a major consultancy firm, and you have been tasked to lead a new project. The Brazilian e-commerce company “Olist” has hired your consulting firm to produce some insights using its data. You will lead this project which means you have to plan what tools to use, models to build and approach to take.
<br>
<br>

### 1- Transfers:
The data that you will work with is stored in multiple sources. First, some of the tables are stored in a postgres database and some will be retrieved via an API. You are expected to move this data from the different sources to BigQuery. Feel free to use the suitable tool for each source.<br>
<br>

### 2- Modeling:
You are expected to apply the necessary modeling techniques to transform the raw data in the landing tables to multiple dimension and fact tables and store them in BigQuery. 
<br>
<br>

### 3- Reporting:
You are expected to build views in BigQuery from the dimension and fact tables and use them to build a dashboard on Looker Studio that answers at least 6 of the below questions. The questions in bold are mandatory ones. Feel free to answer more questions that are not mentioned below if you want

1- Who are the top customers by total order value?  
2- What is the average number of orders per customer?  
3- Which customer segments have the highest lifetime value?  
4- What are the top-selling products by quantity?  
5- Which product categories generate the most revenue?  
6- What is the average delivery time for products in each category?  
7- What is the total number of orders placed each month (trend over time)?  
8- What percentage of orders are delivered on time versus delayed?  
9- What are the regions with the highest number of orders?  
10- What is the distribution of payment methods used by customers?  
11- Which payment method contributes the most revenue?  
12- What is the monthly revenue trend for the business?  
13- Which sellers have the highest order volumes?  
14- What is the average seller rating?  
15- What is the average review score for products across categories?  

<br>

### Additional information:
- You can retrieve the table order_payments by calling the API endpoint:

  https://us-central1-ready-de-25.cloudfunctions.net/order_payments_table

- You can retrieve the table - by calling the API endpoint:

  https://us-central1-ready-de-25.cloudfunctions.net/sellers_table

- You can find the raw data in the postgreDB with the following connection details:
  -
  -
  -
  -
- Your code is expected to follow the clean coding guidelines discussed in previous sessions (folder naming and hierarchy, variable naming, no unused import etc.).

- You are expected to follow Github best practices ( New branch with a clear and descriptive name, short and clear commit message and Good PR description)

- Keep the commits and history in the project. don't push the whole code in one commit

- Don't push your commits directly to main branch, Create a branch for the relevant changes/features then do a pull request from the feature branch into the main branch

- You are expected to create an ERD for your models and a document explaining your approach to the project

- Testing is done in your local Airflow, for the final version we’ll turn on the composer so you can experience the production release and code reviews.

- For any questions, don’t hesitate to use our slack channel to ask for help

- For any questions, don’t hesitate to use our slack channel to ask for help

### Deliverables:
- Architecture diagram (ERD)
- Project documentation
- Final presentation
