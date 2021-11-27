import numpy as np
import pandas as pd
import os
import sys
import psycopg2
from psycopg2.extras import execute_values
import configparser
from data_parser import DataParser
from pathlib import Path
from zipfile import ZipFile
from sql_queries import *
from cluster_connect import ClusterConnector

def unzip_file(file, target_filepath):
    """Extracts all files from a zip file into a target file path

    Args:
        file (pathlib.Path or str): Path to a zip file
        target_filepath (pathlib.Path or str): Path to the directory into which the files should be extracted

    Returns:
        list of str: Names of the extracted files
    """
    # opening the zip file in READ mode
    with ZipFile(file, 'r') as zip:
    # extracting all the files
        print('Extracting files now...')
        zip.extractall(path = target_filepath)
        print('Done!')
        return zip.namelist()

class ETLProcess:
    def __init__(self, config_file, recipes_file, reviews_file):
        self.config = configparser.ConfigParser()
        self.config.read(config_file)
        
        self.recipes_file = recipes_file
        self.reviews_file = reviews_file

    def connect_to_db(self):

        # setup connection to Redshift cluster
        connector = ClusterConnector(self.config)
        connector.setup_resources()
        connector.get_cluster_endpoint_arn()

        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
                connector.DWH_ENDPOINT, 
                self.config['DWH']['DWH_DB'], 
                self.config['DWH']['DWH_DB_USER'],
                self.config['DWH']['DWH_DB_PASSWORD'],
                self.config['DWH']['DWH_PORT']))

        cur = conn.cursor()
        return conn, cur

    def execute_query(self, conn, cur, query, data):
        """Execute the SQL insert query

        Args:
            cur (Cursor): Cursor for the database
            query (str): SQL insert statement
            data (pd.Dataframe): Data to insert into the table
        """
        try:
            args_str = ",".join(["(" + ', '.join(["'"+str(y)+"'" for y in x]) + ")" for x in data.values])
            formatted_query = query.replace("%s", args_str)
            cur.execute(formatted_query)
            conn.commit()
        except Exception as e:
            print(e)

    def create_tables(self, cur, conn):
        """ Create all tables and add constraints

        Args:
            cur (Cursor): The cursor for navigating the database
            conn (Connection): Connection to the database
        """
        print("Creating tables")
        for query in create_table_queries:
            cur.execute(query)
            conn.commit()
        cur.execute(foreign_key_query)
        conn.commit()

    def drop_tables(self, cur, conn):
        """ Drop all tables

        Args:
            cur (Cursor): The cursor for navigating the database
            conn (Connection): Connection to the database
        """
        print("Dropping tables")
        for query in drop_table_queries:
            cur.execute(query)
            conn.commit()

    def load_data(self, cur, conn):
        """ This procedure processes the recipes data.
        It extracts the recipe information in order to store it into the recipes table.

        Args:
            cur (Cursor): The cursor variable for the database
        """
        print("Loading data into database.")
        # Prepare recipe categories
        categories = self.recipes['RecipeCategory'].unique()
        categories = pd.DataFrame(categories).reset_index()
        categories.columns = ['CategoryId', 'RecipeCategory']
        self.recipes = pd.merge(self.recipes, categories, on='RecipeCategory')
        print("Writing categories into database.")
        self.execute_query(conn, cur, categories_table_insert, categories)

        # Prepare authors data
        newest_authors_reviews = self.reviews.loc[self.reviews.groupby(['AuthorId'])['DateModified'].idxmax()][['AuthorId','AuthorName']]
        newest_authors_recipes = self.recipes.loc[self.recipes.groupby(['AuthorId'])['DatePublished'].idxmax()][['AuthorId','AuthorName']]
        authors = pd.merge(newest_authors_reviews, newest_authors_recipes, on='AuthorId', how='outer')
        authors = authors.drop_duplicates(subset=['AuthorId', 'AuthorName_x', 'AuthorName_y'])
        authors['AuthorName'] = authors['AuthorName_x'].where(authors['AuthorName_x'] > "", authors['AuthorName_y'])
        authors = authors[['AuthorId', 'AuthorName']]
        print("Writing authors into database.")
        self.execute_query(conn, cur, authors_table_insert, authors)

        # Prepare recipes data
        df = self.recipes[['RecipeId', 'Name', 'AuthorId','CookTime','PrepTime','TotalTime', 'DatePublished', 'Description', 'CategoryId', 
            'Calories', 'FatContent', 'SaturatedFatContent', 'CholesterolContent', 'SodiumContent', 'CarbohydrateContent', 'FiberContent', 
            'SugarContent', 'ProteinContent', 'RecipeServings', 'RecipeYield','RecipeInstructions']]
        df[['AuthorId','CategoryId']] = df[['AuthorId','CategoryId']].astype(pd.Int64Dtype())
        print("Writing recipes into database.")
        self.execute_query(conn, cur, recipes_table_insert, df)

        # Prepare recipe images data
        df = self.recipes[['RecipeId', 'Images']]
        df = df.explode('Images')
        print("Writing recipe images into database.")
        self.execute_query(conn, cur, recipe_images_table_insert, df)


        # Prepare recipe keywords data
        keywords = self.recipes['Keywords'].explode().unique()
        keywords = pd.DataFrame(keywords).reset_index()
        keywords.columns = ['KeywordId', 'Keywords']
        print("Writing keywords into database.")
        self.execute_query(conn, cur, keywords_table_insert, keywords)
        
        recipe_keywords = self.recipes[['RecipeId','Keywords']].explode('Keywords')
        recipe_keywords = pd.merge(recipe_keywords, keywords, on='Keywords')
        df = recipe_keywords[['RecipeId','KeywordId']]
        print("Writing recipe_keywords into database.")
        self.execute_query(conn, cur, recipe_keywords_table_insert, df)
        
        # Prepare recipe ingredients data
        ingredients = self.recipes['RecipeIngredientParts'].explode().str.lower().unique()
        ingredients = pd.DataFrame(ingredients).reset_index()
        ingredients.columns = ['IngredientId', 'RecipeIngredient']
        print("Writing ingredients into database.")
        self.execute_query(conn, cur, ingredients_table_insert, ingredients)
        
        recipe_ingredient_parts = self.recipes[['RecipeId','RecipeIngredientParts']].explode('RecipeIngredientParts')
        recipe_ingredient_parts = recipe_ingredient_parts.reset_index(drop=True)
        recipe_ingredient_parts['recipe_ingredient_num'] = recipe_ingredient_parts.groupby('RecipeId').cumcount()
        
        recipe_ingredient_quants = self.recipes[['RecipeId','RecipeIngredientQuantities']].explode('RecipeIngredientQuantities')
        recipe_ingredient_quants = recipe_ingredient_quants.reset_index(drop=True)
        recipe_ingredient_quants['recipe_ingredient_num'] = recipe_ingredient_quants.groupby('RecipeId').cumcount()

        recipe_ingredients = pd.merge(recipe_ingredient_parts, recipe_ingredient_quants, on=['RecipeId','recipe_ingredient_num'], how="inner")
        recipe_ingredients['RecipeIngredientParts'] = recipe_ingredients['RecipeIngredientParts'].str.lower()
        recipe_ingredients = pd.merge(recipe_ingredients, ingredients, right_on='RecipeIngredient', left_on='RecipeIngredientParts', how="left")
        df = recipe_ingredients[['RecipeId', 'IngredientId', 'RecipeIngredientQuantities']]
        print("Writing recipe ingredients into database.")
        self.execute_query(conn, cur, recipe_ingredients_table_insert, df)

        # Prepare reviews data
        df = self.reviews[['ReviewId', 'AuthorId', 'RecipeId', 'Rating', 'Review', 'DateSubmitted', 'DateModified']]
        print("Writing reviews into database.")
        self.execute_query(conn, cur, reviews_table_insert, df)
        

    def run(self):
        # Prepare data
        parser = DataParser()
        self.reviews = parser.prepare_reviews(self.reviews_file)
        self.recipes = parser.prepare_recipes(self.recipes_file)
        
        print("Connect to DB")
        conn, cur = self.connect_to_db()

        self.drop_tables(cur, conn)
        self.create_tables(cur, conn)
        self.load_data(cur, conn)

        conn.close()
            

if __name__ == "__main__":
    config_file = Path(r'.\config\dwh.cfg')
    data_filepath = Path(r'.\data')
    small_set = False

    if small_set:        
        reviews_file = data_filepath / 'reviews_small10.csv'
        recipes_file = data_filepath / 'recipes_small10.parquet'
    else:  
        reviews_file = data_filepath / 'reviews.csv.zip'
        recipes_zip_file = data_filepath / 'recipes.parquet.zip'
        
        recipes_filename = unzip_file(recipes_zip_file, data_filepath)[0]
        recipes_file = data_filepath / recipes_filename
    etl = ETLProcess(config_file, recipes_file, reviews_file)
    etl.run()