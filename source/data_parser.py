import numpy as np
import pandas as pd

class DataParser():

    def prepare_reviews(self, reviews_file):
        print("Preparing reviews data")
        reviews = pd.read_csv(reviews_file)
        # Convert dtype of timestamp columns
        format = '%Y-%m-%dT%H:%M:%SZ'
        reviews['DateSubmitted'] = pd.to_datetime(reviews['DateSubmitted'], format=format)
        reviews['DateModified'] = pd.to_datetime(reviews['DateModified'], format=format)

        # Clean other columns
        reviews['AuthorId'] = reviews['AuthorId'].astype(int)
        reviews['AuthorName'] = reviews['AuthorName'].str.strip()
        reviews['Review'] = reviews['Review'].str.replace('"',"'")

        return reviews

    def prepare_recipes(self, recipes_file):
        recipes = pd.read_parquet(recipes_file)

        print("Preparing recipe data")
        # Convert dtype of timestamp columns
        def parse_duration(col):
            hour_regex = r'PT(\d+)H.*'
            minute_regex = r'.?(\d+)M'
            col_hour = col.str.extract(hour_regex).replace(np.nan, 0).astype('float')
            col_minute = col.str.extract(minute_regex).replace(np.nan, 0).astype('float')
            duration_df = pd.concat({'hour':col_hour,'minute':col_minute}, axis=1)
            duration = duration_df.apply(lambda col: col[0]*60 + col[1], axis=1)
            return duration

        recipes[['CookTime', 'PrepTime', 'TotalTime']] = recipes[['CookTime', 'PrepTime', 'TotalTime']].apply(parse_duration)

        # Clean other columns
        recipes[['AuthorId','RecipeId']] = recipes[['AuthorId','RecipeId']].astype(int)
        recipes['AuthorName'] = recipes['AuthorName'].str.strip()
        recipes['RecipeInstructions'] = recipes['RecipeInstructions'].str.join(" ")
        for col in ['Description', 'RecipeIngredientParts', 'RecipeInstructions', 'RecipeYield', 'Name']:
            recipes[col] = recipes[col].astype(str).str.replace('"',"'").replace('"',"'")

        return recipes
        