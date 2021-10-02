import unittest
import config
import psycopg2
from DataLoading import crud
import Model.model as sbi


class TestCRUD(unittest.TestCase):
    def test_insert(self):
        """ This is an example to test insert operation """
        conn = None
        try:
            # read connection parameters
            params = config.config()

            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database...')
            conn = psycopg2.connect(**params)

            # create a cursor
            cur = conn.cursor()

            # execute a statement
            sql = "SELECT 1 from {table} where ticker = '{ticker}'".format(table='stock_basic_info', ticker='MMM')
            cur.execute(sql)

            row = cur.fetchone()
            if row is not None:
                print(row)
                crud.delete_from_stock_basic_info('MMM')

            stock = sbi.StockBasicInfo('MMM', '3M Company', 'Industrials', 'Industrial Conglomerates')
            ret = crud.insert_into_stock_basic_info(stock)

            self.assertEqual(ret, True)
            # close the communication with the PostgreSQL
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
                print('Database connection closed.')


if __name__ == '__main__':
    unittest.main()
