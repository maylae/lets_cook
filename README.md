# Let's cook
Capstone project for Udacity Data Engineering nanodegree

## Scope 
In the project, I will build a data model that can be used as an app back-end. The app can be used to search for recipes given desired ingredients, time requirements, rating, etc.
The raw input data on recipes and ratings will be transformed into a relational data model that makes it possible to easily find recipes for different search criteria. 
The data is provided as two files (csv, parquet). The files are processed using Python as the dataset is small enough to be processed locally. The final data model is stored in an AWS Redshift database.
The data model visualization is created using draw.io.
For bigger datasets I would have used Spark for the data preparation. But as the dataset was small enough for processing it in Python, I chose the this option because it was easier to develop the code locally.

## Describe and Gather Data 
The data for this project comes from a Kaggle dataset that was created by scraping the recipes on food.com (https://www.kaggle.com/irkaal/foodcom-recipes-and-reviews). The recipes dataset contains 522,517 recipes from 312 different categories. This dataset provides information about each recipe like cooking times, servings, ingredients, nutrition, instructions, and more.
The reviews dataset contains 1,401,982 reviews from 271,907 different users. This dataset provides information about the author, rating, review text, and more.
The data was downloaded at the start of the project and is stored locally.

## Data Exploration
The data exploration is done in the Jupyter notebook EDA.ipynb. I checked for missing values which were not present in necessary columns. The columns where we have missing data are not deemed very important. That's why the missing values are kept.
I did not find any duplicate reviews or recipes. 
The timestamp columns were transformed into DateTime datatype. The columns for cook time, prep time and total time in the recipe dataset are parsed to transform them into floats representing the time in minutes.
The ingredients for the recipes were a bit problematic because the amount needed of the ingredients is stored in a separate column and the lengths of the arrays in the ingredient parts and quantities do not always match. Therefore, I decided to only keep the ingredients for which there are quantities.

## Data Model
A visualisation of the target data model can be found in Data Model.
The data model is in 3rd normal form. This avoids redundancy and makes it easy to create flexible analyses, e.g. concerning the activity of authors.
By keeping the recipe categories and keywords in separate tables it is easy to rename a category or keyword and to edit the keywords for recipes. Moreover, the authors are stored in a separate table to make sure, that only one verson of the author name is mapped to an author id. In the original data, an author id could have different values for the author name in the recipe and review table when an author changed his name. 

First, I extract the unique authors from the both datasets and only keep the most current author name for each author id. These are stored in the authors table.
Second, the recipe categories are extracted from the recipe dataset. Each category is assigned an id. Only this category id is kept with the recipe and I store the category id - name mapping in the categories table.
The recipe keywords are stored in an array in the original recipe dataset. The array is separated, each keyword is assigned a keyword id. A mapping table recipe_keywords holds the mapping between keywords and recipes.
Similarly, the recipe ingredients parts and quantities arrays in the recipe dataset are separated. The distinct ingredients are assigned an ingredient id. A mapping table recipe_ingredients holds the recipe id, ingredient id and the quantity of the ingredient needed for the recipe.
Also, the recipe images are extracted from the recipe dataset where they are stored in arrays and now are located in a separate recipe_images table. Now, it is easier to add or remove iages from a recipe.
In the reviews dataset I drop the author name as it can be loaded from the authors table. Other than that, nothing needs to be done before loading the dataset into the reviews table.

Integrity constraints on the relational database (e.g., unique key, data type, foreign keys) are added.

## Data Dictionary
Please see data/data_dictionary for a data dictionary of the final data model.

# Data quality
Unit tests ensure that the code runs without errors and that the number or rows in the database matches the input data.

# Possible usage
## Queries
Possible queries to make use of the data model could be
* Get all recipes which take less than 30 minutes with 
  `SELECT * FROM recipes WHERE total_time < 30`
* Get all recipes in the category Dessert: 
  `SELECT recipes.* FROM recipes WHERE category_id IN (SELECT category_id FROM categories WHERE category_name = 'Dessert')`
* Get all reviews for recipe name 'Super Coleslaw'
  `SELECT * FROM reviews WHERE recipe_id IN (SELECT recipe_id FROM recipes WHERE name="Super Coleslaw")`

## Data update
The dataset is updated once a month. If I used the data model for a cooking website, I would update my database once every 5 minutes to show recent comments in the app but not overload the ETL pipeline. More frequent updates are not really necessary because I am not working with time critical data.
Updating the data in the database should be done using time partitions: Only the most recent new recipes and new comments should be processed.
## Ideas for different scenarios:
What should be done in case...
* *... the data was increased by 100x.* I would use Spark for the data preparation and store it in a Redshift database. 
* *... the pipelines were run on a daily basis by 7am.* I would use Airflow for scheduling the ETL job.
* *... the database needed to be accessed by 100+ people.* I would still load the data into an AWS Redshift database to be able to scale the accesses quickly and dynamically.

# Instructions
## Needed packages
To execute the code, you need to have Python3 installed as well as the following libraries:
* pandas
* numpy
* psycopg2
* pathlib
* zipfile
* os
* sys

## Run the code
Before running the code, you need to download the files recipes.parquet.zip and reviews.csv.zip from
https://www.kaggle.com/irkaal/foodcom-recipes-and-reviews and store it in the data folder within the lets_cook directory.
Create a dwh.cfg based on the dwh_example.cfg that contains your AWS credentials.
Run the etl.py script to process the data and load the data model into AWS Redshift.
Run the cleanup.py script to delete the cluster.