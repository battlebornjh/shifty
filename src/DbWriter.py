import psycopg2
import SeriesObjects as obj

db_params = {
    "database": "postgres",
    "user": "yourUser",
    "password": "changeit",
    "host": "localhost",
    "port": "5432"
}

def insertShiftFound(highCorrelation:obj.HighCorrelation):
    insertShiftFound2(highCorrelation.target1,highCorrelation.target2,highCorrelation.shift1,highCorrelation.shift2,highCorrelation.size,highCorrelation.cc,highCorrelation.p)
def insertShiftFound2(target1,target2,shift1,shift2,size,cc,p):
    insert_query = """
    INSERT INTO public.shift_found(
        target1, 
        target2, 
        shift1,
        shift2,
        size,
        cc,
        p) 
    VALUES (
        %s, %s, %s, %s, %s, %s, %s);
    """

    highCorData = (target1,
                   target2,
                   shift1,
                   shift2,
                   size,
                   float(cc),
                   float(p))

    try:
        conn = psycopg2.connect(**db_params)
        # Open a cursor to perform database operations
        with conn.cursor() as cur:
            # Execute the INSERT statement
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

