"""
Sample Fabric Data Fixtures for Testing.

Contains realistic sample data structures mimicking Microsoft Fabric sample datasets
for use in validation tests.
"""

from semantic_sync.core.models import (
    SemanticModel,
    SemanticTable,
    SemanticColumn,
    SemanticMeasure,
    SemanticRelationship,
)


def create_sales_model() -> SemanticModel:
    """
    Create a sample Sales semantic model.
    
    Mimics the structure of Microsoft Fabric sample sales data with:
    - Products, Customers, Orders tables
    - Measures for analytics
    - Relationships between tables
    """
    return SemanticModel(
        name="SalesAnalytics",
        source="fabric",
        description="Sample sales analytics model for testing",
        tables=[
            # Products table
            SemanticTable(
                name="Products",
                description="Product catalog with pricing information",
                source_table="dbo.Products",
                columns=[
                    SemanticColumn(
                        name="ProductID",
                        data_type="Int64",
                        description="Unique product identifier",
                        is_nullable=False,
                    ),
                    SemanticColumn(
                        name="ProductName",
                        data_type="String",
                        description="Name of the product",
                    ),
                    SemanticColumn(
                        name="Category",
                        data_type="String",
                        description="Product category",
                    ),
                    SemanticColumn(
                        name="SubCategory",
                        data_type="String",
                        description="Product subcategory",
                    ),
                    SemanticColumn(
                        name="UnitPrice",
                        data_type="Decimal",
                        description="Price per unit",
                        format_string="$#,##0.00",
                    ),
                    SemanticColumn(
                        name="UnitCost",
                        data_type="Decimal",
                        description="Cost per unit",
                        format_string="$#,##0.00",
                    ),
                ],
            ),
            # Customers table
            SemanticTable(
                name="Customers",
                description="Customer information",
                source_table="dbo.Customers",
                columns=[
                    SemanticColumn(
                        name="CustomerID",
                        data_type="Int64",
                        description="Unique customer identifier",
                        is_nullable=False,
                    ),
                    SemanticColumn(
                        name="CustomerName",
                        data_type="String",
                        description="Full name of the customer",
                    ),
                    SemanticColumn(
                        name="Email",
                        data_type="String",
                        description="Customer email address",
                    ),
                    SemanticColumn(
                        name="Country",
                        data_type="String",
                        description="Customer country",
                    ),
                    SemanticColumn(
                        name="Region",
                        data_type="String",
                        description="Geographic region",
                    ),
                    SemanticColumn(
                        name="Segment",
                        data_type="String",
                        description="Customer segment (Consumer, Corporate, etc.)",
                    ),
                ],
            ),
            # Orders table
            SemanticTable(
                name="Orders",
                description="Sales order transactions",
                source_table="dbo.Orders",
                columns=[
                    SemanticColumn(
                        name="OrderID",
                        data_type="Int64",
                        description="Unique order identifier",
                        is_nullable=False,
                    ),
                    SemanticColumn(
                        name="CustomerID",
                        data_type="Int64",
                        description="Reference to customer",
                    ),
                    SemanticColumn(
                        name="ProductID",
                        data_type="Int64",
                        description="Reference to product",
                    ),
                    SemanticColumn(
                        name="OrderDate",
                        data_type="DateTime",
                        description="Date of the order",
                    ),
                    SemanticColumn(
                        name="Quantity",
                        data_type="Int64",
                        description="Quantity ordered",
                    ),
                    SemanticColumn(
                        name="TotalAmount",
                        data_type="Decimal",
                        description="Total order amount",
                        format_string="$#,##0.00",
                    ),
                    SemanticColumn(
                        name="Discount",
                        data_type="Decimal",
                        description="Discount percentage applied",
                        format_string="0.00%",
                    ),
                ],
            ),
            # Date dimension table
            SemanticTable(
                name="DateDim",
                description="Date dimension for time-based analysis",
                source_table="dbo.DateDimension",
                columns=[
                    SemanticColumn(
                        name="DateKey",
                        data_type="DateTime",
                        description="Date key",
                        is_nullable=False,
                    ),
                    SemanticColumn(
                        name="Year",
                        data_type="Int64",
                        description="Calendar year",
                    ),
                    SemanticColumn(
                        name="Quarter",
                        data_type="String",
                        description="Calendar quarter (Q1, Q2, Q3, Q4)",
                    ),
                    SemanticColumn(
                        name="Month",
                        data_type="String",
                        description="Month name",
                    ),
                    SemanticColumn(
                        name="MonthNumber",
                        data_type="Int64",
                        description="Month number (1-12)",
                    ),
                    SemanticColumn(
                        name="DayOfWeek",
                        data_type="String",
                        description="Day of week name",
                    ),
                ],
            ),
        ],
        measures=[
            SemanticMeasure(
                name="Total Revenue",
                expression="SUM(Orders[TotalAmount])",
                description="Sum of all order amounts",
                format_string="$#,##0.00",
                table_name="Orders",
            ),
            SemanticMeasure(
                name="Total Orders",
                expression="COUNTROWS(Orders)",
                description="Count of all orders",
                table_name="Orders",
            ),
            SemanticMeasure(
                name="Average Order Value",
                expression="AVERAGE(Orders[TotalAmount])",
                description="Average value per order",
                format_string="$#,##0.00",
                table_name="Orders",
            ),
            SemanticMeasure(
                name="Total Quantity Sold",
                expression="SUM(Orders[Quantity])",
                description="Total quantity of items sold",
                table_name="Orders",
            ),
            SemanticMeasure(
                name="Gross Profit",
                expression="SUMX(Orders, Orders[Quantity] * (RELATED(Products[UnitPrice]) - RELATED(Products[UnitCost])))",
                description="Gross profit from all orders",
                format_string="$#,##0.00",
                table_name="Orders",
            ),
            SemanticMeasure(
                name="Unique Customers",
                expression="DISTINCTCOUNT(Orders[CustomerID])",
                description="Count of unique customers who placed orders",
                table_name="Orders",
            ),
        ],
        relationships=[
            SemanticRelationship(
                name="Orders_to_Customers",
                from_table="Orders",
                from_column="CustomerID",
                to_table="Customers",
                to_column="CustomerID",
                cardinality="many-to-one",
                cross_filter_direction="single",
            ),
            SemanticRelationship(
                name="Orders_to_Products",
                from_table="Orders",
                from_column="ProductID",
                to_table="Products",
                to_column="ProductID",
                cardinality="many-to-one",
                cross_filter_direction="single",
            ),
            SemanticRelationship(
                name="Orders_to_Date",
                from_table="Orders",
                from_column="OrderDate",
                to_table="DateDim",
                to_column="DateKey",
                cardinality="many-to-one",
                cross_filter_direction="single",
            ),
        ],
    )


def create_inventory_model() -> SemanticModel:
    """
    Create a sample Inventory semantic model.
    
    Used for testing modifications and sync scenarios.
    """
    return SemanticModel(
        name="InventoryManagement",
        source="snowflake",
        description="Inventory tracking model",
        tables=[
            SemanticTable(
                name="Inventory",
                description="Current inventory levels",
                columns=[
                    SemanticColumn(
                        name="InventoryID",
                        data_type="Int64",
                        is_nullable=False,
                    ),
                    SemanticColumn(
                        name="ProductID",
                        data_type="Int64",
                    ),
                    SemanticColumn(
                        name="WarehouseID",
                        data_type="Int64",
                    ),
                    SemanticColumn(
                        name="QuantityOnHand",
                        data_type="Int64",
                        description="Current stock quantity",
                    ),
                    SemanticColumn(
                        name="ReorderLevel",
                        data_type="Int64",
                        description="Threshold for reorder",
                    ),
                    SemanticColumn(
                        name="LastUpdated",
                        data_type="DateTime",
                    ),
                ],
            ),
            SemanticTable(
                name="Warehouses",
                description="Warehouse locations",
                columns=[
                    SemanticColumn(
                        name="WarehouseID",
                        data_type="Int64",
                        is_nullable=False,
                    ),
                    SemanticColumn(
                        name="WarehouseName",
                        data_type="String",
                    ),
                    SemanticColumn(
                        name="Location",
                        data_type="String",
                    ),
                    SemanticColumn(
                        name="Capacity",
                        data_type="Int64",
                    ),
                ],
            ),
        ],
        measures=[
            SemanticMeasure(
                name="Total Stock",
                expression="SUM(Inventory[QuantityOnHand])",
                description="Total inventory across all warehouses",
            ),
            SemanticMeasure(
                name="Low Stock Items",
                expression="CALCULATE(COUNTROWS(Inventory), Inventory[QuantityOnHand] < Inventory[ReorderLevel])",
                description="Count of items below reorder level",
            ),
        ],
        relationships=[
            SemanticRelationship(
                name="Inventory_to_Warehouses",
                from_table="Inventory",
                from_column="WarehouseID",
                to_table="Warehouses",
                to_column="WarehouseID",
                cardinality="many-to-one",
            ),
        ],
    )


def create_modified_sales_model() -> SemanticModel:
    """
    Create a modified version of the Sales model.
    
    Used for testing change detection:
    - Modified description on Products table
    - New column added to Customers
    - Removed SubCategory from Products
    - New measure added
    """
    base_model = create_sales_model()
    
    # Modify Products table
    products_table = next(t for t in base_model.tables if t.name == "Products")
    products_table.description = "Updated product catalog with extended information"
    products_table.columns = [c for c in products_table.columns if c.name != "SubCategory"]
    
    # Add new column to Customers
    customers_table = next(t for t in base_model.tables if t.name == "Customers")
    customers_table.columns.append(
        SemanticColumn(
            name="LoyaltyTier",
            data_type="String",
            description="Customer loyalty tier (Bronze, Silver, Gold, Platinum)",
        )
    )
    
    # Add new measure
    base_model.measures.append(
        SemanticMeasure(
            name="Customer Lifetime Value",
            expression="SUMX(FILTER(Orders, Orders[CustomerID] = EARLIER(Customers[CustomerID])), Orders[TotalAmount])",
            description="Total revenue from a customer",
            format_string="$#,##0.00",
            table_name="Customers",
        )
    )
    
    return base_model


# Sample row data for integration tests
SAMPLE_PRODUCTS_DATA = [
    {"ProductID": 1, "ProductName": "Widget A", "Category": "Widgets", "SubCategory": "Standard", "UnitPrice": 29.99, "UnitCost": 15.00},
    {"ProductID": 2, "ProductName": "Widget B", "Category": "Widgets", "SubCategory": "Premium", "UnitPrice": 49.99, "UnitCost": 25.00},
    {"ProductID": 3, "ProductName": "Gadget X", "Category": "Gadgets", "SubCategory": "Basic", "UnitPrice": 99.99, "UnitCost": 50.00},
    {"ProductID": 4, "ProductName": "Gadget Y", "Category": "Gadgets", "SubCategory": "Advanced", "UnitPrice": 199.99, "UnitCost": 100.00},
    {"ProductID": 5, "ProductName": "Tool Alpha", "Category": "Tools", "SubCategory": "Hand Tools", "UnitPrice": 39.99, "UnitCost": 20.00},
]

SAMPLE_CUSTOMERS_DATA = [
    {"CustomerID": 1, "CustomerName": "John Smith", "Email": "john@example.com", "Country": "USA", "Region": "West", "Segment": "Consumer"},
    {"CustomerID": 2, "CustomerName": "Jane Doe", "Email": "jane@example.com", "Country": "USA", "Region": "East", "Segment": "Corporate"},
    {"CustomerID": 3, "CustomerName": "Bob Wilson", "Email": "bob@example.com", "Country": "Canada", "Region": "North", "Segment": "Small Business"},
    {"CustomerID": 4, "CustomerName": "Alice Brown", "Email": "alice@example.com", "Country": "UK", "Region": "Europe", "Segment": "Consumer"},
    {"CustomerID": 5, "CustomerName": "Charlie Davis", "Email": "charlie@example.com", "Country": "Germany", "Region": "Europe", "Segment": "Enterprise"},
]

SAMPLE_ORDERS_DATA = [
    {"OrderID": 1001, "CustomerID": 1, "ProductID": 1, "OrderDate": "2024-01-15", "Quantity": 2, "TotalAmount": 59.98, "Discount": 0.0},
    {"OrderID": 1002, "CustomerID": 2, "ProductID": 3, "OrderDate": "2024-01-16", "Quantity": 1, "TotalAmount": 99.99, "Discount": 0.0},
    {"OrderID": 1003, "CustomerID": 1, "ProductID": 2, "OrderDate": "2024-01-17", "Quantity": 3, "TotalAmount": 149.97, "Discount": 0.0},
    {"OrderID": 1004, "CustomerID": 3, "ProductID": 4, "OrderDate": "2024-01-18", "Quantity": 1, "TotalAmount": 179.99, "Discount": 0.1},
    {"OrderID": 1005, "CustomerID": 4, "ProductID": 5, "OrderDate": "2024-01-19", "Quantity": 5, "TotalAmount": 199.95, "Discount": 0.0},
    {"OrderID": 1006, "CustomerID": 5, "ProductID": 1, "OrderDate": "2024-01-20", "Quantity": 10, "TotalAmount": 269.91, "Discount": 0.1},
    {"OrderID": 1007, "CustomerID": 2, "ProductID": 2, "OrderDate": "2024-01-21", "Quantity": 2, "TotalAmount": 99.98, "Discount": 0.0},
    {"OrderID": 1008, "CustomerID": 3, "ProductID": 3, "OrderDate": "2024-01-22", "Quantity": 1, "TotalAmount": 99.99, "Discount": 0.0},
]
