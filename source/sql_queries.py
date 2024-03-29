# DROP TABLES
recipes_table_drop = "DROP TABLE IF EXISTS recipes CASCADE;"
recipe_images_table_drop = "DROP TABLE IF EXISTS recipe_images CASCADE;"
recipe_ingredients_table_drop = "DROP TABLE IF EXISTS recipe_ingredients CASCADE;"
ingredients_table_drop = "DROP TABLE IF EXISTS ingredients CASCADE;"
categories_table_drop = "DROP TABLE IF EXISTS categories CASCADE;"
recipe_keywords_table_drop = "DROP TABLE IF EXISTS recipe_keywords CASCADE;"
keyword_table_drop = "DROP TABLE IF EXISTS keywords CASCADE;"
authors_table_drop = "DROP TABLE IF EXISTS authors CASCADE;"
reviews_table_drop = "DROP TABLE IF EXISTS reviews CASCADE;"

# CREATE TABLES

recipes_table_create = ("""
CREATE TABLE IF NOT EXISTS recipes (
            recipe_id int PRIMARY KEY, 
            name varchar(500), 
            author_id int,
            cook_time float,
            prep_time float,
            total_time float,
            date_published datetime,
            description varchar(max),
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
            recipe_servings varchar(50),
            recipe_yield varchar(50),
            recipe_instructions varchar(max)
            );
""")

recipe_images_table_create = ("""
CREATE TABLE IF NOT EXISTS recipe_images (
            recipe_image_id int IDENTITY(0,1) PRIMARY KEY, 
            recipe_id int NOT NULL,
            image_url varchar)
""")

ingredients_table_create = ("""
CREATE TABLE IF NOT EXISTS ingredients (
            ingredient_id int PRIMARY KEY, 
            name varchar(500))
""")

recipe_ingredients_table_create = ("""
CREATE TABLE IF NOT EXISTS recipe_ingredients (
            recipe_ingredient_id int IDENTITY(0,1) PRIMARY KEY,
            ingredient_id int NOT NULL, 
            recipe_id int NOT NULL,
            ingredient_quantity VARCHAR(20)
            )
""")

categories_table_create = ("""
CREATE TABLE IF NOT EXISTS categories (
            category_id int PRIMARY KEY, 
            category_name varchar(50));
""")

keywords_table_create = ("""
    CREATE TABLE IF NOT EXISTS keywords ( 
            keyword_id int PRIMARY KEY, 
            keyword varchar(50) NOT NULL);
""")

recipe_keywords_table_create = ("""
    CREATE TABLE IF NOT EXISTS recipe_keywords ( 
            recipe_keyword_id int IDENTITY(0,1) PRIMARY KEY,
            recipe_id int NOT NULL,
            keyword_id int NOT NULL);
""")

reviews_table_create = ("""
    CREATE TABLE IF NOT EXISTS reviews ( 
            review_id int PRIMARY KEY, 
            author_id int NOT NULL,
            recipe_id int NOT NULL,
            rating int NOT NULL,
            review varchar(max),
            date_submitted datetime,
            date_modified datetime);
""")

authors_table_create = ("""
    CREATE TABLE IF NOT EXISTS authors ( 
            author_id int PRIMARY KEY, 
            name varchar(250) NOT NULL);
""")

foreign_key_query = """
    ALTER TABLE recipes 
    ADD CONSTRAINT fk_category
    FOREIGN KEY (category_id) 
    REFERENCES categories (category_id);

    ALTER TABLE recipes 
    ADD CONSTRAINT fk_recipe_author
    FOREIGN KEY (author_id) 
    REFERENCES authors (author_id);

    ALTER TABLE recipe_keywords 
    ADD CONSTRAINT fk_recipe_keyword
    FOREIGN KEY (recipe_id) 
    REFERENCES recipes (recipe_id);

    ALTER TABLE recipe_keywords 
    ADD CONSTRAINT fk_keyword
    FOREIGN KEY (keyword_id) 
    REFERENCES keywords (keyword_id);

    ALTER TABLE recipe_ingredients 
    ADD CONSTRAINT fk_recipe_ingredient
    FOREIGN KEY (recipe_id) 
    REFERENCES recipes (recipe_id);

    ALTER TABLE recipe_ingredients 
    ADD CONSTRAINT fk_ingredient
    FOREIGN KEY (ingredient_id) 
    REFERENCES ingredients (ingredient_id);

    ALTER TABLE recipe_images 
    ADD CONSTRAINT fk_recipe_image
    FOREIGN KEY (recipe_id) 
    REFERENCES recipes (recipe_id);

    ALTER TABLE reviews 
    ADD CONSTRAINT fk_recipe
    FOREIGN KEY (recipe_id) 
    REFERENCES recipes (recipe_id);

    ALTER TABLE reviews 
    ADD CONSTRAINT fk_author
    FOREIGN KEY (author_id) 
    REFERENCES authors (author_id);
"""

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

recipe_ingredients_table_insert = ("""
INSERT INTO recipe_ingredients (recipe_id, ingredient_id, ingredient_quantity)
VALUES %s
""")

categories_table_insert = ("""
INSERT INTO categories (category_id, category_name)
VALUES %s
""")

recipe_keywords_table_insert = ("""
INSERT INTO recipe_keywords (recipe_id, keyword_id)
VALUES %s
""")

keywords_table_insert = ("""
INSERT INTO keywords (keyword_id, keyword)
VALUES %s
""")

reviews_table_insert = ("""
INSERT INTO reviews (review_id, author_id, recipe_id, rating, review, date_submitted, date_modified)
VALUES %s
"""
)

authors_table_insert = ("""
INSERT INTO authors (author_id, name)
VALUES %s
"""
)

# QUERY LISTS

create_table_queries = [recipes_table_create, recipe_images_table_create,recipe_ingredients_table_create, ingredients_table_create, categories_table_create, recipe_keywords_table_create, keywords_table_create, authors_table_create, reviews_table_create]
drop_table_queries = [recipe_images_table_drop, recipe_ingredients_table_drop, ingredients_table_drop, categories_table_drop, recipe_keywords_table_drop, reviews_table_drop, recipes_table_drop, keyword_table_drop, authors_table_drop]