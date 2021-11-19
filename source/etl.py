import numpy as np
import pandas as pd
import psycopg2
import configparser
from datetime import timedelta
from pathlib import Path
from zipfile import ZipFile
from sql_queries import *
from cluster_connect import ClusterConnector


def drop_tables(cur, conn):
    print("Drop tables")
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    config = configparser.ConfigParser()
    config.read(r'C:\Users\A105938856\OneDrive - Deutsche Telekom AG\Schulungen\Udacity\lets_cook\source\dwh.cfg')

    # setup connection to Redshift cluster
    connector = ClusterConnector(config)
    print("Get cluster endpoint.")
    connector.get_cluster_endpoint_arn()
    
    # connect to Redshift cluster
    print("Connect to DB")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(connector.DWH_ENDPOINT, config['DWH']['DWH_DB'], config['DWH']['DWH_DB_USER'],config['DWH']['DWH_DB_PASSWORD'],config['DWH']['DWH_PORT']))
    cur = conn.cursor()
    
    # load data into Redshift postgres DB
    reviews = prepare_reviews()
    recipes = prepare_recipes()
    
    load_data(cur, reviews, recipes)

    conn.close()


if __name__ == "__main__":
    main()

def prepare_reviews():
    reviews_file = Path(r'''C:\Users\A105938856\OneDrive - Deutsche Telekom AG\Schulungen\Udacity\lets_cook\data\reviews.csv.zip''')
    reviews = pd.read_csv(reviews_file)
    # Convert dtype of timestamp columns
    format = '%Y-%m-%dT%H:%M:%SZ'
    reviews['DateSubmitted'] = pd.to_datetime(reviews['DateSubmitted'], format=format)
    reviews['DateModified'] = pd.to_datetime(reviews['DateModified'], format=format)

    # Clean other columns
    reviews['AuthorId'] = reviews['AuthorId'].astype(int)
    reviews['AuthorName'] = reviews['AuthorName'].str.strip()

    return reviews

def prepare_recipes():
    recipes_zip_file = Path(r'''C:\Users\A105938856\OneDrive - Deutsche Telekom AG\Schulungen\Udacity\lets_cook\data\recipes.parquet.zip''')
    recipes_filepath = Path(r'''C:\Users\A105938856\OneDrive - Deutsche Telekom AG\Schulungen\Udacity\lets_cook\data''')
    # opening the zip file in READ mode
    with ZipFile(recipes_zip_file, 'r') as zip:
        # extracting all the files
        print('Extracting recipe files now...')
        zip.extractall(path = recipes_filepath)
        print('Done!')
    recipes = pd.read_parquet(recipes_filepath / 'recipes.parquet')

    # Convert dtype of timestamp columns
    def parse_duration(col):
        hour_regex = r'PT(\d+)H.*'
        minute_regex = r'.?(\d+)M'
        col_hour = col.str.extract(hour_regex).replace(np.nan, 0).astype('float')
        col_minute = col.str.extract(minute_regex).replace(np.nan, 0).astype('float')
        duration_df = pd.concat({'hour':col_hour,'minute':col_minute}, axis=1)
        duration = duration_df.apply(lambda col: timedelta(hours=col[0], minutes=col[1], seconds=0), axis=1)
        return duration

    recipes[['CookTime', 'PrepTime', 'TotalTime']] = recipes[['CookTime', 'PrepTime', 'TotalTime']].apply(parse_duration)

    # Clean other columns
    recipes['AuthorId','RecipeId'] = recipes[['AuthorId','RecipeId']].astype(int)
    recipes['AuthorName'] = recipes['AuthorName'].str.strip()

    return recipes
    

def load_data(cur, reviews, recipes):
    """
    This procedure processes the recipes data.
    It extracts the recipe information in order to store it into the recipes table.

    INPUTS:
    * cur the cursor variable
    * recipes (pd.DataFrame)
    """

    # Prepare recipe categories
    categories = recipes['category'].unique()
    categories = categories.reset_index().rename({'index':'CategoryId'})
    recipes = recipes.join(categories, on='category')

    try:
        cur.execute(sql_queries.recipe_categories_table_insert, categories.values)
    except:
        pass


    # Prepare authors data
    newest_authors_reviews = reviews.loc[reviews.groupby(['AuthorId'])['DateModified'].idxmax()][['AuthorId','AuthorName']]
    newest_authors_recipes = recipes.loc[recipes.groupby(['AuthorId'])['DatePublished'].idxmax()][['AuthorId','AuthorName']]

    authors = pd.merge(newest_authors_reviews, newest_authors_recipes, on='AuthorId', how='outer')
    authors = authors.drop_duplicates(subset=['AuthorId', 'AuthorName_x', 'AuthorName_y'])
    authors['AuthorName'] = authors['AuthorName_x'].where(authors['AuthorName_x'] > "", authors['AuthorName_y'])
    authors = authors[['AuthorId', 'AuthorName']]

    try:
        cur.execute(sql_queries.authors_table_insert, authors.values)
    except:
        pass

    # Prepare reviews data
    df = reviews[['ReviewId', 'AuthorId', 'RecipeId', 'Rating', 'Review', 'DateSubmitted', 'DateModified']]
    try:
        cur.execute(sql_queries.reviews_table_insert, df.values)
    except:
        pass

    # Prepare recipes data
    df = recipes[['RecipeId', 'Name', 'AuthorId','CookTime','PrepTime','TotalTime', 'DatePublished', 'Description', 'CategoryId', 'Calories', 'FatContent', 'SaturatedFatContent', 'CholesterolContent', 'SodiumContent', 'CarbohydrateContent', 'FiberContent', 'SugarContent', 'ProteinContent', 'RecipeServings', 'RecipeYield','RecipeInstructions']]

    try:
        cur.execute(sql_queries.recipes_table_insert, df.values)
    except:
        pass

    # Prepare recipe images data
    df = recipes[['RecipeId', 'Images']]
    df = df.explode('Images')
    try:    
        cur.execute(recipe_images_table_insert, df.values)
    except:
        pass

    # Prepare recipe keywords data
    keywords = recipes['Keywords'].explode().unique()
    keywords = keywords.reset_index().rename({'index':'KeywordId'})
    try:    
        cur.execute(keywords_table_insert, keywords.values)
    except:
        pass

    recipe_keywords = recipes[['RecipeId','Keywords']].explode('Keywords')
    recipe_keywords = pd.merge(recipe_keywords, keywords, on='Keywords')
    df = recipe_keywords[['RecipeId','KeywordId']]
    try:    
        cur.execute(recipe_keywords_table_insert, df.values)
    except:
        pass

    # Prepare recipe ingredients data
    ingredients = recipes['RecipeIngredientParts'].explode().str.lower().unique()
    ingredients = ingredients.reset_index()
    ingredients.columns = ['IngredientId', 'RecipeIngredient']
    try:    
        cur.execute(recipe_ingredients_table_insert, ingredients.values)
    except:
        pass

    recipe_ingredient_parts = recipes[['RecipeId','RecipeIngredientParts']].explode('RecipeIngredientParts')
    recipe_ingredient_parts = recipe_ingredient_parts.reset_index(drop=True)
    recipe_ingredient_parts['recipe_ingredient_num'] = recipe_ingredient_parts.groupby('RecipeId').cumcount()
    
    recipe_ingredient_quants = recipes[['RecipeId','RecipeIngredientQuantities']].explode('RecipeIngredientQuantities')
    recipe_ingredient_quants = recipe_ingredient_quants.reset_index(drop=True)
    recipe_ingredient_quants['recipe_ingredient_num'] = recipe_ingredient_quants.groupby('RecipeId').cumcount()

    recipe_ingredients = pd.merge(recipe_ingredient_parts, recipe_ingredient_quants, on=['RecipeId','recipe_ingredient_num'], how="inner")
    recipe_ingredients['RecipeIngredientParts'] = recipe_ingredients['RecipeIngredientParts'].str.lower()
    recipe_ingredients = pd.merge(recipe_ingredients, ingredients, right_on='RecipeIngredient', left_on='RecipeIngredientParts', how="left")
    df = recipe_ingredients[['RecipeId', 'IngredientId', 'RecipeIngredientQuantities']]
    try:    
        cur.execute(recipe_ingredients_table_insert, df.values)
    except:
        pass