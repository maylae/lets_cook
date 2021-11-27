# DROP TABLES
recipes_table_drop = "DROP TABLE IF EXISTS recipes;"
recipe_images_table_drop = "DROP TABLE IF EXISTS recipe_images;"
recipe_ingredients_table_drop = "DROP TABLE IF EXISTS recipe_ingredients;"
ingredients_table_drop = "DROP TABLE IF EXISTS ingredients;"
categories_table_drop = "DROP TABLE IF EXISTS categories;"
recipe_keywords_table_drop = "DROP TABLE IF EXISTS recipe_keywords;"
keyword_table_drop = "DROP TABLE IF EXISTS keywords;"
authors_table_drop = "DROP TABLE IF EXISTS authors;"
reviews_table_drop = "DROP TABLE IF EXISTS reviews;"

# CREATE TABLES

recipes_table_create = ("""
CREATE TABLE IF NOT EXISTS recipes (
            recipe_id INTEGER PRIMARY KEY, 
            name varchar, 
            author_id int,
            cook_time float,
            prep_time float,
            total_time float,
            date_published datetime,
            description varchar,
            category_id int,
            calories float,
            fat_content float,
            saturated_fat_content float,
            cholesterol_content float,
            sodium_content float,
            carbohydrate_content float,
            fiber_content float,
            sugar_content float,
            protein_content float,
            recipe_servings int,
            recipe_yield varchar(50),
            recipe_instructions varchar,
            FOREIGN KEY (category_id) REFERENCES categories (category_id)
            FOREIGN KEY (author_id) REFERENCES authors (author_id)
            );
""")

recipe_images_table_create = ("""
CREATE TABLE IF NOT EXISTS recipe_images (
            recipe_image_id INTEGER PRIMARY KEY AUTOINCREMENT, 
            recipe_id int NOT NULL,
            image_url varchar,
            FOREIGN KEY (recipe_id) REFERENCES recipes (recipe_id))
""")

ingredients_table_create = ("""
CREATE TABLE IF NOT EXISTS ingredients (
            ingredient_id INTEGER PRIMARY KEY, 
            name varchar(250))
""")

recipe_ingredients_table_create = ("""
CREATE TABLE IF NOT EXISTS recipe_ingredients (
            recipe_ingredient_id INTEGER PRIMARY KEY AUTOINCREMENT,
            ingredient_id int NOT NULL, 
            recipe_id int NOT NULL,
            ingredient_quantity VARCHAR(20),
            FOREIGN KEY (recipe_id) REFERENCES recipes (recipe_id),
            FOREIGN KEY (ingredient_id) REFERENCES ingredients (ingredient_id)
            )
""")

categories_table_create = ("""
CREATE TABLE IF NOT EXISTS categories (
            category_id INTEGER PRIMARY KEY, 
            category_name varchar(50));
""")

keywords_table_create = ("""
    CREATE TABLE IF NOT EXISTS keywords ( 
            keyword_id int PRIMARY KEY, 
            keyword varchar(50) NOT NULL);
""")

recipe_keywords_table_create = ("""
    CREATE TABLE IF NOT EXISTS recipe_keywords ( 
            recipe_keyword_id INTEGER PRIMARY KEY AUTOINCREMENT,
            recipe_id int NOT NULL,
            keyword_id int NOT NULL,
            FOREIGN KEY (recipe_id) REFERENCES recipes (recipe_id),
            FOREIGN KEY (keyword_id) REFERENCES keywords (keyword_id)
            );
""")

reviews_table_create = ("""
    CREATE TABLE IF NOT EXISTS reviews ( 
            review_id INTEGER PRIMARY KEY, 
            author_id int NOT NULL,
            recipe_id int NOT NULL,
            rating int NOT NULL,
            review varchar,
            date_submitted datetime,
            date_modified datetime,
            FOREIGN KEY (recipe_id) REFERENCES recipes (recipe_id),
            FOREIGN KEY (author_id) REFERENCES authors (author_id)
            );
""")

authors_table_create = ("""
    CREATE TABLE IF NOT EXISTS authors ( 
            author_id INTEGER PRIMARY KEY, 
            name varchar(250) NOT NULL);
""")

foreign_key_query = """

"""

# INSERT RECORDS

# recipes_table_insert = ("""
# INSERT INTO recipes (recipe_id, name, author_id, cook_time, prep_time, total_time, date_published, description, category_id, calories, fat_content, saturated_fat_content, cholesterol_content, sodium_content, carbohydrate_content, fiber_content, sugar_content, protein_content, recipe_servings, recipe_yield, recipe_instructions)
# VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
# """)

# recipe_images_table_insert = ("""
# INSERT INTO recipe_images (recipe_id, image_url)
# VALUES (%s, %s)
# """)

# ingredients_table_insert = ("""
# INSERT INTO ingredients (ingredient_id, name)
# VALUES (%s, %s)
# ON CONFLICT (ingredient_id)
#     DO UPDATE SET 
#         name = EXCLUDED.name
# """)

# recipe_ingredients_table_insert = ("""
# INSERT INTO recipe_ingredients (recipe_id, ingredient_id, ingredient_quantity)
# VALUES (%s, %s, %s)
# """)

# categories_table_insert = ("""
# INSERT INTO categories (category_id, category_name)
# VALUES (%s, %s)
# ON CONFLICT (category_id)
#     DO UPDATE SET 
#         category_name = EXCLUDED.category_name;
# """)

# recipe_keywords_table_insert = ("""
# INSERT INTO recipe_keywords (recipe_id, keyword_id)
# VALUES (%s, %s)
# """)

# keywords_table_insert = ("""
# INSERT INTO keywords (keyword_id, keyword)
# VALUES (%s, %s)
# ON CONFLICT (keyword_id)
#     DO UPDATE SET 
#         keyword = EXCLUDED.keyword;
# """)

# reviews_table_insert = ("""
# INSERT INTO reviews (review_id, author_id, recipe_id, rating, review, date_submitted, date_modified)
# VALUES (%s, %s, %s, %s, %s, %s, %s)
# ON CONFLICT (review_id)
#     DO UPDATE SET 
#         rating = EXCLUDED.rating,
#         review = EXCLUDED.review,
#         date_modified = EXCLUDED.date_modified;
# """)

# authors_table_insert = ("""
# INSERT INTO authors (author_id, name)
# VALUES (%s, %s)
# ON CONFLICT (author_id)
#     DO UPDATE SET 
#         name = EXCLUDED.name
# """)

recipes_table_insert = ("""
INSERT INTO recipes (recipe_id, name, author_id, cook_time, prep_time, total_time, date_published, description, category_id, calories, fat_content, saturated_fat_content, cholesterol_content, sodium_content, carbohydrate_content, fiber_content, sugar_content, protein_content, recipe_servings, recipe_yield, recipe_instructions)
VALUES %s
""")

recipe_images_table_insert = ("""
INSERT INTO recipe_images (recipe_id, image_url)
VALUES %s
""")

ingredients_table_insert = ("""
INSERT INTO ingredients (ingredient_id, name)
VALUES %s
""")
#ON CONFLICT (ingredient_id)
#    DO UPDATE SET 
#        name = EXCLUDED.name
#""")

recipe_ingredients_table_insert = ("""
INSERT INTO recipe_ingredients (recipe_id, ingredient_id, ingredient_quantity)
VALUES %s
""")

categories_table_insert = ("""
INSERT INTO categories (category_id, category_name)
VALUES %s
""")
#ON CONFLICT (category_id)
#    DO UPDATE SET 
#        category_name = EXCLUDED.category_name;
#""")

recipe_keywords_table_insert = ("""
INSERT INTO recipe_keywords (recipe_id, keyword_id)
VALUES %s
""")

keywords_table_insert = ("""
INSERT INTO keywords (keyword_id, keyword)
VALUES %s
""")
#ON CONFLICT (keyword_id)
#    DO UPDATE SET 
#        keyword = EXCLUDED.keyword;
#""")

reviews_table_insert = ("""
INSERT INTO reviews (review_id, author_id, recipe_id, rating, review, date_submitted, date_modified)
VALUES %s
"""
#ON CONFLICT (review_id)
 #   DO UPDATE SET 
#        rating = EXCLUDED.rating,
#        review = EXCLUDED.review,
#        date_modified = EXCLUDED.date_modified;
#"""
)

authors_table_insert = ("""
INSERT INTO authors (author_id, name)
VALUES %s
"""
# ON CONFLICT (author_id)
#   DO UPDATE SET 
#        name = EXCLUDED.name
#"""
)

# QUERY LISTS

create_table_queries = [authors_table_create, categories_table_create, keywords_table_create, recipes_table_create, recipe_images_table_create,
ingredients_table_create, recipe_ingredients_table_create, recipe_keywords_table_create, reviews_table_create]
drop_table_queries = [recipe_images_table_drop, recipe_ingredients_table_drop, ingredients_table_drop, categories_table_drop, recipe_keywords_table_drop, reviews_table_drop, recipes_table_drop, keyword_table_drop, authors_table_drop]