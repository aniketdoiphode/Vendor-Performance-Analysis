from sqlalchemy import create_engine
import pandas as pd
import logging
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv('credentials.env')
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASS = os.getenv("MYSQL_PASS")
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = os.getenv("MYSQL_PORT")
MYSQL_DB   = "vendoranalytics"

logging.basicConfig(
    filename="logs/get_vendor_summary.log"
    level=logging.DEBUG,
    format="%(acstime)s - %(levelname)s - %(message)",
    filemode="a"
)

def create_vendor_summary(conn):
    """This function will merge the different tables to get the overall vendor summary and adding new columns in the resultant data"""
    vendor_sales_summary = pd.read_sql_query(
        """
            WITH FreightSummary AS (
                SELECT VendorNumber, SUM(Freight) AS FreightCost
                FROM vendor_invoice
                GROUP BY VendorNumber
            ),
            PurchaseSummary AS (
                SELECT 
                    p.VendorNumber,
                    p.VendorName,
                    p.Brand,
                    p.Description,
                    p.PurchasePrice,
                    pp.Price AS ActualPrice,
                    pp.Volume,
                    SUM(p.Quantity) AS TotalPurchaseQuantity,
                    SUM(p.Dollars) AS TotalPurchaseDollars
                FROM purchases p
                JOIN purchase_prices pp 
                  ON p.Brand = pp.Brand   -- consider joining on VendorNumber+Brand if possible
                WHERE p.PurchasePrice > 0
                GROUP BY p.VendorNumber, p.VendorName, p.Brand, p.Description, p.PurchasePrice, pp.Price, pp.Volume
            ),
            SalesSummary AS (
                SELECT 
                    VendorNo AS VendorNumber,   -- normalize name
                    Brand,
                    SUM(SalesQuantity) AS TotalSalesQuantity,
                    SUM(SalesDollars)   AS TotalSalesDollars,
                    SUM(SalesPrice)     AS TotalSalesPrice,
                    SUM(ExciseTax)      AS TotalExciseTax
                FROM sales
                GROUP BY VendorNo, Brand
            )
            SELECT 
                ps.VendorNumber,
                ps.VendorName,
                ps.Brand,
                ps.Description,
                ps.PurchasePrice,
                ps.ActualPrice,
                ps.Volume,
                ps.TotalPurchaseQuantity,
                ps.TotalPurchaseDollars,
                ss.TotalSalesQuantity,
                ss.TotalSalesDollars,
                ss.TotalSalesPrice,
                ss.TotalExciseTax,
                fs.FreightCost
            FROM PurchaseSummary ps
            LEFT JOIN SalesSummary ss 
              ON ps.VendorNumber = ss.VendorNumber AND ps.Brand = ss.Brand
            LEFT JOIN FreightSummary fs 
              ON ps.VendorNumber = fs.VendorNumber
            ORDER BY ps.TotalPurchaseDollars DESC;
        """, conn)
    return vendor_sales_summary

def clean_data(df):
    """This function will clean the data"""
    #Filling missing values with 0
    df.fillna(0, inplace=True)

    #Removing spaces from categorical columns
    df['VendorName'] = df['VendorName'].str.strip()
    df['Description'] = df['Description'].str.strip()

    #Creating new columns for better analysis
    df['GrossProfit'] = df['TotalSalesDollars'] - df['TotalPurchaseDollars']
    df['ProfitMargin'] = (df['GrossProfit'] / df['TotalSalesDollars'])*100
    df['StockTurnover'] = df['TotalSalesQuantity']/df['TotalPurchaseQuantity']
    df['SalesPurchaseRatio'] = df['TotalSalesDollars']/df['TotalPurchaseDollars']
    return df

def ingest_db(df, table_name, engine):
    """This function will ingest the dataframe into database table"""
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)

if __name__ == '__main__':
    #creating database connection
    connection_string = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASS}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
    conn = create_engine(connection_string)

    logging.info('Creating Vendor Summary Table..')
    summary_df = create_vendor_summary(conn)
    logging.info("Summary data -->", summary_df.head())

    logging.info('Cleaning data..')
    clean_df = clean_data(summary_df)
    logging.info("Clean data -->", clean_df.head())

    logging.info('Ingesting data..')
    ingest_db(clean_df, 'vendor_sales_summary', conn)
    logging.info('Completed..')