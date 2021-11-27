import unittest
import os
import sys
import pandas as pd
from pathlib import Path
from etl import ETLProcess

class ETLTest(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        data_filepath = Path(r'.\data')
        self.reviews_file = data_filepath / 'reviews_small10.csv'
        self.recipes_file = data_filepath / 'recipes_small10.parquet'
        self.config_file = os.path.join(sys.path[0], 'dwh.cfg')
        self.db_name = 'test.db'

    def test_init(self):
        etl = ETLProcess(self.config_file, self.recipes_file, self.reviews_file, self.db_name)
        #etl.config['KEY'] != ''

    def test_db_connection(self):
        etl = ETLProcess(self.config_file, self.recipes_file, self.reviews_file, self.db_name)
        conn, cur = etl.connect_to_db()

    def test_drop_tables(self):
        etl = ETLProcess(self.config_file, self.recipes_file, self.reviews_file, self.db_name)
        conn, cur = etl.connect_to_db()
        etl.drop_tables(cur, conn)
        results = cur.execute("SELECT count(*) FROM sqlite_master WHERE type = 'table'")
        tables = results.fetchall()[0]
        self.assertEqual(len(tables), 1)

    def test_create_tables(self):
        etl = ETLProcess(self.config_file, self.recipes_file, self.reviews_file, self.db_name)
        conn, cur = etl.connect_to_db()
        etl.create_tables(cur, conn)
        results = cur.execute("SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name != 'android_metadata' AND name != 'sqlite_sequence';")
        tables = results.fetchall()[0][0]
        self.assertEqual(tables, 9)

    def test_load_data(self):
        etl = ETLProcess(self.config_file, self.recipes_file, self.reviews_file, self.db_name)
        conn, cur = etl.connect_to_db()
        etl.run()
        authors = cur.execute("SELECT * FROM authors;").fetchall()
        self.assertEqual(len(authors),57)
        recipes = cur.execute("SELECT * FROM recipes;").fetchall()
        self.assertEqual(len(recipes), 10)
        reviews = cur.execute("SELECT * FROM reviews;").fetchall()
        self.assertEqual(len(reviews), etl.recipes.shape[0])

if __name__ == "__main__": 
    unittest.main()