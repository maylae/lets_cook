from pathlib import Path
import pandas as pd
from etl import unzip_file
from data_parser import DataParser

# make smaller file with 100 recipes and corresponding reviews
parser = DataParser()
data_filepath = Path(r'.\data')
reviews_file = data_filepath / 'reviews.csv.zip'
recipes_zip_file = data_filepath / 'recipes.parquet.zip'

recipes_filename = unzip_file(recipes_zip_file, data_filepath)[0]
recipes_file = data_filepath / recipes_filename
recipes =  pd.read_parquet(recipes_file)
recipes_small = recipes.head(10)
recipes_small.to_parquet(r'.\data\recipes_small10.parquet', index=False)

reviews = pd.read_csv(reviews_file)
recipe_ids = recipes_small["RecipeId"].unique()
reviews_small = reviews.query('RecipeId in @recipe_ids')
reviews_small.to_csv(r'.\data\reviews_small10.csv', index=False)
