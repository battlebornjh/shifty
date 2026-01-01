import Series
import pika
import json
import uuid
from datetime import datetime, timedelta
import DbWriter as db
from GetData import getSeriesData
                                                                              ################################################### 26   
# dataSet = [[3,5,4,2,1,5,5,5,2,3,4,4,4,1,2,3,4,5,2,1,1,1,1,3,3,2,1,1,1,1,5,2,3,4,5,9,5,7,3,1,1,2,8,3,1,5,5,5,4,1,1,5,5,5,1,1,5,5,6,4,1,3,1],#0
#            [5,5,5,8,5,1,1,0,5,5,5,4,2,3,3,5,5,2,2,2,3,5,3,2,4,4,4,3,8,8,2,5,6,6,4,8,1,1,5,6,1,2,3,2,5,4,5,4,6,6,3,1,2,2,5,8,1,2,2,0,5,1,1],#1
#            [5,6,5,5,2,6,8,2,9,8,2,3,5,5,9,6,6,1,1,2,1,2,1,1,1,6,3,5,2,2,1,5,5,2,5,2,4,6,1,2,5,4,4,8,5,2,1,2,2,2,3,1,5,5,5,1,1,2,5,6,3,6,3],#2
#            [1,1,1,1,1,1,1,1,1,1,8,8,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,5,2,5,2,5,2,4,1,8,1,2,5,5,5,1,2,2,2,5,5,4,1,5,5,5,1,2,3,6,5,1,2,0],#3
#            [6,3,5,4,5,5,2,5,2,5,2,5,2,5,2,7,8,7,4,5,7,2,1,1,1,1,5,4,5,5,4,1,4,2,3,3,1,2,5,8,7,0,2,5,5,1,1,1,2,2,2,2,1,4,2,4,2,4,2,5,2,2,2],#4
#            [6,6,6,6,3,6,6,6,3,6,3,3,3,3,2,5,3,3,3,3,3,5,3,5,2,5,5,5,2,5,7,8,9,8,5,6,6,3,3,3,5,2,5,2,5,4,1,5,5,7,4,5,8,5,1,5,4,5,4,5,4,1,5],#5
#            [8,9,8,9,8,9,8,9,8,9,8,9,8,9,4,1,4,4,4,1,4,1,4,1,8,9,8,9,8,5,5,4,5,1,5,5,5,1,5,4,5,5,5,4,5,5,2,5,4,4,4,5,4,5,5,5,4,4,4,1,5,5,1],#6
#                                     ################################################### 26
#            [4,5,2,5,3,5,9,8,7,6,5,4,4,5,9,5,7,3,1,1,2,8,3,1,5,5,5,4,1,1,5,5,5,1,1,5,5,6,2,5,5,5,2,6,5,6,5,6,7,4,5,1,5,0,2,5,1,5,1,5,1,5,1],#7
#            [9,8,7,6,5,4,3,2,1,9,8,7,6,5,4,3,2,1,9,8,4,5,2,1,1,1,1,5,5,2,9,6,9,5,8,5,8,5,2,3,6,5,6,4,5,7,5,4,5,4,4,4,1,4,1,4,1,4,1,0,1,0,1],#8
#            [7,7,7,7,7,4,7,7,7,7,4,7,9,8,7,6,5,4,4,7,7,7,7,4,7,7,7,7,4,7,6,3,2,2,2,3,2,3,6,3,3,3,2,1,5,5,5,2,2,5,5,6,5,5,5,5,1,5,5,2,5,2,2],#9
#            [7,7,7,7,7,4,7,7,7,7,4,7,9,8,7,6,5,4,4,7,7,7,7,4,7,7,7,7,4,7,1,2,5,8,5,5,2,2,2,6,5,4,5,5,6,5,5,5,4,5,7,4,5,4,7,5,4,5,9,6,9,6,9],#10
#            [7,7,7,7,7,4,7,7,7,7,4,7,9,8,7,6,5,4,4,7,7,7,7,4,7,7,7,7,4,7,1,2,5,8,5,5,2,2,2,6,5,4,5,5,6,5,5,5,4,5,7,4,5,4,7,5,4,5,9,6,9,6,9]]#11
strt = datetime.strptime("2025-09-01", '%Y-%m-%d').date()
end = datetime.strptime("2025-12-22", '%Y-%m-%d').date()
#sd = getSeriesData(strt, end, ["AAPL", "ABAT", "NVDA", "NOW", "ORCL", "TTWO", "INTC", "IBM", "CRM"])
#dataSet = sd.data

if __name__ == "__main__":
    guid = uuid.uuid4()
    db.insertSetRun(guid)
    #processes = []
    for size in range(5,15):
        #Series.search_series(guid, dataSet, 0.98, 5, size)

        #queue
        credentials = pika.PlainCredentials("myuser", "mypassword")
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', credentials=credentials))
        channel = connection.channel()

        channel.queue_declare('Shifty')

        shift = {
            "guid": str(guid),
            "strt": strt.isoformat(),
            "end": end.isoformat(),
            "symbols": ["AAPL", "ABAT", "NVDA", "NOW", "ORCL", "TTWO", "INTC", "IBM", "CRM"],
            "corNumber": 0.997,
            "minShift": 3,
            "size": size
        }

        body = json.dumps(shift).encode("utf-8")

        properties = pika.BasicProperties(content_type='application/json', content_encoding='utf-8')

        channel.basic_publish(exchange='', routing_key="Shifty", body=body, properties=properties)
    

    db.endSetRun(guid)

    print("----------DONE------------------")
    # print(f"Looking for series of {sizes} elements, whith a minimum shift of {minShift}.")
    # print(f"{len(posCors)} matches found with a positive correlation coefficient higher than {posCorNumber}.")
    # print(f"{len(negCors)} matches found with a negative correlation coefficient less than {posCorNumber}.")
    # print(f"Total comparisons done: {len(comparisonsDone)}.")
    # print(f"Total dup comparisons skipped: {comparisonsSkipped}.")
    # print(f"Total minShift comparisons skipped: {comparisonsSkippedMinShift}.")
    #print(f"Elapsed time: {time.time() - start_time:.4f} seconds.")