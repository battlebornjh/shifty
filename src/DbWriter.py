import psycopg2
import psycopg2.extras
import SeriesObjects as obj
from datetime import datetime,timezone
import time
import uuid
import os

POSTGRES_HOST = os.getenv("POSTGRES_HOST") or "localhost"

db_params = {
    "database": "postgres",
    "user": "yourUser",
    "password": "changeit",
    "host": POSTGRES_HOST,
    "port": "5432"
}

def insertShiftFound(guid, highCorrelation:obj.HighCorrelation):
    insertShiftFound2(guid, highCorrelation.target1,highCorrelation.target2,highCorrelation.shift1,highCorrelation.shift2,highCorrelation.size,highCorrelation.cc,highCorrelation.p)
def insertShiftFound2(guid, target1,target2,shift1,shift2,size,cc,p):
    insert_query = """
    INSERT INTO public.shift_found(
        target1, 
        target2, 
        shift1,
        shift2,
        size,
        cc,
        p,
        set_run_guid) 
    VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s);
    """

    highCorData = (target1,
                   target2,
                   shift1,
                   shift2,
                   size,
                   float(cc),
                   float(p),
                   guid)

    try:
        conn = psycopg2.connect(**db_params)
        # Open a cursor to perform database operations
        with conn.cursor() as cur:
            # Execute the INSERT statement
            psycopg2.extras.register_uuid()
            cur.execute(insert_query, highCorData)

            # Commit the transaction
            conn.commit()

    except psycopg2.Error as e:
        print(f"Error inserting data: {e}")
        conn.rollback() # Rollback in case of error

    finally:
        # Close the connection
        if conn:
            conn.close()

def insertSetRun(guid):
    insert_query = """
    INSERT INTO public.set_run(guid) 
    VALUES (%s);
    """
    data = (guid,)
    try:
        conn = psycopg2.connect(**db_params)
        # Open a cursor to perform database operations
        with conn.cursor() as cur:
            # Execute the INSERT statement
            psycopg2.extras.register_uuid()
            cur.execute(insert_query, data)

            # Commit the transaction
            conn.commit()

    except psycopg2.Error as e:
        print(f"Error inserting data: {e}")
        conn.rollback() # Rollback in case of error

    finally:
        # Close the connection
        if conn:
            conn.close()

def endSetRun(guid):
    insert_query = """
    UPDATE public.set_run SET end_dt = %s WHERE guid = %s;
    """
    curd = datetime.utcnow()
    data = (curd, guid)
    try:
        conn = psycopg2.connect(**db_params)
        # Open a cursor to perform database operations
        with conn.cursor() as cur:
            # Execute the INSERT statement
            psycopg2.extras.register_uuid()
            cur.execute(insert_query, data)

            # Commit the transaction
            conn.commit()

    except psycopg2.Error as e:
        print(f"Error inserting data: {e}")
        conn.rollback() # Rollback in case of error

    finally:
        # Close the connection
        if conn:
            conn.close()

def insertStockData(dataSeries):
    inserts = 0
    updates = 0
    skips = 0
    insert_query = """
    INSERT INTO public.stock_data(dt, symbol, open, high, low, close, volume) 
    VALUES (%s,%s,%s,%s,%s,%s,%s);
    """
    data = None
    try:
        conn = psycopg2.connect(**db_params)
        # Open a cursor to perform database operations
        with conn.cursor() as cur:
            # Execute the INSERT statement
            for ts in dataSeries:
                sd = getStockQuote(ts.dt, ts.symbol)
                if sd:
                    if (ts.open != sd.open or 
                        ts.high != sd.high or
                        ts.low != sd.low or
                        ts.close != sd.close or
                        ts.volume != sd.volume):
                        updateStockData(ts)
                        updates += 1
                    else:
                        skips += 1
                else:
                    data = (ts.dt, ts.symbol, ts.open, ts.high, ts.low, ts.close, ts.volume)
                    cur.execute(insert_query, data)
                    inserts += 1

            # Commit the transaction
            conn.commit()

    except psycopg2.Error as e:
        print(f"Error inserting data: {e}")
        conn.rollback() # Rollback in case of error

    finally:
        # Close the connection
        if conn:
            conn.close()
    
    print(f"Insert Stock Data Done.  Inserts:{inserts} Updates:{updates} Skips:{skips}")

def getStockQuote(dt, symbol):
    query = "SELECT * FROM public.stock_data WHERE dt = %s AND symbol = %s"
    ts = None
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        cur.execute(query, (dt,symbol)) # Pass values as a tuple
        record = cur.fetchone()
        if record:
            ts = obj.TimeSeriesDaily(dt, symbol, record[3], record[4], record[5], record[6], record[7])
        return ts

    except psycopg2.Error as e:
        print(f"Error inserting data: {e}")
        conn.rollback() # Rollback in case of error

    finally:
        # Close the connection
        if conn:
            conn.close()

def updateStockData(tsd):
    insert_query = """
    UPDATE public.stock_data set open = %s, high = %s, low = %s, close = %s, volume = %s
    WHERE dt = %s AND symbol = %s;
    """
    data = None
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        # Open a cursor to perform database operations
        data = (tsd.open, tsd.high, tsd.low, tsd.close, tsd.volume, tsd.dt, tsd.symbol)
        cur.execute(insert_query, data)
        conn.commit()

    except psycopg2.Error as e:
        print(f"Error inserting data: {e}")
        conn.rollback() # Rollback in case of error

    finally:
        # Close the connection
        if conn:
            conn.close()
