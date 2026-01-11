
import unittest
from semantic_sync.core.dax_transpiler import DaxToSqlTranspiler

class TestDaxTranspiler(unittest.TestCase):
    def setUp(self):
        self.transpiler = DaxToSqlTranspiler()

    def test_sum(self):
        dax = "SUM(Sales[Amount])"
        sql = self.transpiler.transpile(dax, source_table="Sales")
        self.assertEqual(sql, '(SELECT SUM("Amount") FROM "Sales")')
    
    def test_sum_quoted(self):
        dax = "SUM('Sales Table'[Total Amount])"
        sql = self.transpiler.transpile(dax)
        self.assertEqual(sql, '(SELECT SUM("Total Amount") FROM "Sales Table")')

    def test_average(self):
        dax = "AVERAGE(Sales[Price])"
        sql = self.transpiler.transpile(dax, source_table="Sales")
        self.assertEqual(sql, '(SELECT AVG("Price") FROM "Sales")')
        
    def test_distinctwords(self):
        dax = "DISTINCTCOUNT(Orders[OrderID])"
        sql = self.transpiler.transpile(dax)
        self.assertEqual(sql, '(SELECT COUNT(DISTINCT "OrderID") FROM "Orders")')

if __name__ == '__main__':
    unittest.main()
