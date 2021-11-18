# Let's cook
Capstone project for Udacity Data Engineering nanodegree

Step 1: Scope the Project and Gather Data
## Scope 
Explain what you plan to do in the project in more detail. What data do you use? What is your end solution look like? What tools did you use? etc>
Explain what end use cases you'd like to prepare the data for (e.g., analytics table, app back-end, source-of-truth database, etc.)

In the project, I will build a data model that can be used as an app back-end. The app can be used to search for recipes given desired ingredients, time requirements, rating, etc.
The raw input data on recipes and ratings will be transformed into a relational data model that makes it possible to easily find recipes for different search criteria. 
TODO: continue

## Describe and Gather Data 
The data for this project comes from a Kaggle dataset that was created by scraping the recipes on food.com (https://www.kaggle.com/irkaal/foodcom-recipes-and-reviews). The recipes dataset contains 522,517 recipes from 312 different categories. This dataset provides information about each recipe like cooking times, servings, ingredients, nutrition, instructions, and more.
The reviews dataset contains 1,401,982 reviews from 271,907 different users. This dataset provides information about the author, rating, review text, and more.

## Data Exploration
Explore the data to identify data quality issues, like missing values, duplicate data, etc.
Document steps necessary to clean the data

## Data Model
Map out the conceptual data model and explain why you chose that model
List the steps necessary to pipeline the data into the chosen data model

## ETL to Model the Data
Create the data pipelines and the data model
Include a data dictionary
Run data quality checks to ensure the pipeline ran as expected
Integrity constraints on the relational database (e.g., unique key, data type, etc.)
Unit tests for the scripts to ensure they are doing the right thing
Source/count checks to ensure completeness

Step 5: Complete Project Write Up
What's the goal? What queries will you want to run? How would Spark or Airflow be incorporated? Why did you choose the model you chose?
Clearly state the rationale for the choice of tools and technologies for the project.
Document the steps of the process.
Propose how often the data should be updated and why.
Post your write-up and final data model in a GitHub repo.
## Ideas for different scenarios:
What should be done in case...
*... the data was increased by 100x.
*... the pipelines were run on a daily basis by 7am.
*... the database needed to be accessed by 100+ people.