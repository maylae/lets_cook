import unittest
from pathlib import Path

from etl import ETLProcess

class ETLTest(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        data_filepath = Path(r'.\data')
        self.reviews_file = data_filepath / 'reviews_small10.csv'
        self.recipes_file = data_filepath / 'recipes_small10.parquet'
        self.config_file = Path(r'.\tests\dwh_test.cfg')

    def test_init(self):
        etl = ETLProcess(self.config_file, self.recipes_file, self.reviews_file)
        self.assertNotEqual(etl.config.get('AWS','KEY'), '')

    def test_load_data(self):
        etl = ETLProcess(self.config_file, self.recipes_file, self.reviews_file)
        conn, cur = etl.connect_to_db()
        etl.run()
        cur.execute("SELECT * FROM authors;")
        authors = cur.fetchall()
        self.assertEqual(len(authors),57)
        cur.execute("SELECT * FROM recipes;")
        recipes = cur.fetchall()
        self.assertEqual(len(recipes), 10)
        cur.execute("SELECT * FROM reviews;")
        reviews = cur.fetchall()
        self.assertEqual(len(reviews), etl.reviews.shape[0])

if __name__ == "__main__": 
    unittest.main()