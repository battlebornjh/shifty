import psycopg2
import psycopg2.extras
import SeriesObjects as obj
from datetime import datetime,timezone
import time
import uuid

db_params = {
    "database": "postgres",
    "user": "yourUser",
    "password": "changeit",
    "host": "localhost",
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
