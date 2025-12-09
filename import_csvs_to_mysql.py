import os
import re
import math
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from sqlalchemy import create_engine, types as sql_types
from dateutil.parser import parse as dateparse
load_dotenv('credentials.env')

MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASS = os.getenv("MYSQL_PASS")
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = os.getenv("MYSQL_PORT")
MYSQL_DB   = "vendoranalytics"

# folder containing csv files
CSV_FOLDER = "D:\\Data Analysis projects\\Vendor Performance Analysis\\data\\"

# mapping CSV filenames to target table names
FILES = {
    "begin_inventory.csv": "begin_inventory",
    "end_inventory.csv"  : "end_inventory",
    "purchase_prices.csv": "purchase_prices",
    "purchases.csv"      : "purchases",
    "sales.csv"          : "sales",
    "vendor_invoice.csv" : "vendor_invoice",
}
# chunk size for to_sql
CHUNKSIZE = 20000

# Create SQLAlchemy engine
connect_string = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASS}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}?charset=utf8mb4"
engine = create_engine(connect_string, pool_recycle=3600, connect_args={"local_infile": 1})

# helpers
def is_date_series(s, samples=50, threshold=0.6):
    """Try to guess if a series is date-like by attempting to parse sample values."""
    s_nonnull = s.dropna().astype(str)
    if s_nonnull.empty:
        return False
    # sample some values
    sample_vals = s_nonnull.iloc[0: min(samples, len(s_nonnull))]
    parsed = 0
    for v in sample_vals:
        v = v.strip()
        if v == "":
            continue
        # common date-only formats: yyyy-mm-dd, dd-mm-yyyy, dd/mm/yyyy ...
        try:
            # allow fuzzy parsing but avoid numbers that are clearly not dates (like "750mL")
            dt = dateparse(v, fuzzy=False, dayfirst=True)
            # treat successful parse as date unless it's obviously a time-only or numeric
            parsed += 1
        except Exception:
            pass
    return (parsed / max(1, len(sample_vals))) >= threshold

def infer_sqlalchemy_types(df):
    """Return a dict colname -> SQLAlchemy type for use in DataFrame.to_sql"""
    mapping = {}
    for col in df.columns:
        ser = df[col]
        if pd.api.types.is_integer_dtype(ser) or pd.api.types.is_bool_dtype(ser):
            # use Integer unless values are huge
            maxv = None
            try:
                maxv = int(ser.dropna().abs().max())
            except Exception:
                maxv = None
            if maxv is not None and maxv > 2147483647:
                mapping[col] = sql_types.BigInteger()
            else:
                mapping[col] = sql_types.Integer()
        elif pd.api.types.is_float_dtype(ser):
            # use DECIMAL with precision guessed or Float
            mapping[col] = sql_types.Numeric(precision=12, scale=4)
        elif pd.api.types.is_datetime64_any_dtype(ser):
            mapping[col] = sql_types.Date()
        else:
            # object: infer length and pick VARCHAR(n) up to 1024 else TEXT
            maxlen = ser.dropna().astype(str).map(len).max() if len(ser.dropna())>0 else 0
            if maxlen is None:
                maxlen = 0
            if maxlen <= 191:
                mapping[col] = sql_types.VARCHAR(length=max(1, int(maxlen)))
            elif maxlen <= 1000:
                mapping[col] = sql_types.VARCHAR(length= max(191, int(min(maxlen,1000))))
            else:
                mapping[col] = sql_types.Text()
    return mapping

def normalize_column_names(cols):
    # simple normalization: strip, replace spaces with underscore
    new = []
    for c in cols:
        c2 = c.strip()
        c2 = re.sub(r'\s+', '_', c2)
        new.append(c2)
    return new

def load_csv_to_mysql(csv_path, table_name, if_exists="replace"):
    print(f"Processing {csv_path} -> {table_name}")
    # read with utf-8-sig to remove BOM automatically
    # low_memory=False to avoid mixed dtypes warnings
    df = pd.read_csv(csv_path, encoding='utf-8-sig', low_memory=False)
    # normalize column names (optional)
    df.columns = normalize_column_names(df.columns.tolist())

    # attempt to detect columns that are date-like (and convert)
    for col in df.columns:
        if df[col].dtype == object:
            if is_date_series(df[col]):
                # try parse with dayfirst True (handles dd-mm-yyyy) then fallback
                try:
                    df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce').dt.date
                    num_parsed = df[col].notna().sum()
                    if num_parsed == 0:
                        # try ISO format
                        df[col] = pd.to_datetime(df[col].astype(str), errors='coerce').dt.date
                except Exception:
                    # leave as is
                    pass

    # coerce numeric-looking columns
    # try to convert columns that look numeric but loaded as object (e.g., "8" or "12.99")
    for col in df.columns:
        if df[col].dtype == object:
            sample = df[col].dropna().astype(str).head(30).tolist()
            # if many samples look numeric, convert
            numeric_like = sum(1 for v in sample if re.match(r'^[+-]?\d+(\.\d+)?$', v.strip()))
            if len(sample) > 0 and (numeric_like / len(sample)) >= 0.6:
                # choose int if all integers
                if all(re.match(r'^[+-]?\d+$', v.strip()) for v in df[col].dropna().astype(str).head(200).tolist()):
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
                else:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

    # infer SQL types
    dtype_map = infer_sqlalchemy_types(df)

    # write to SQL in chunks
    df.to_sql(name=table_name, con=engine, if_exists=if_exists, index=False,
              dtype=dtype_map, chunksize=CHUNKSIZE, method='multi')
    print(f"Finished {table_name}. {len(df)} rows written (if_exists={if_exists}).\n")

# main loop for all files
for fname, tbl in FILES.items():
    path = os.path.join(CSV_FOLDER, fname)
    if not os.path.exists(path):
        print(f"SKIP: {path} does not exist.")
        continue
    try:
        load_csv_to_mysql(path, tbl, if_exists="replace")
    except Exception as e:
        print(f"ERROR importing {path} -> {tbl}: {e}")

print("All done.")