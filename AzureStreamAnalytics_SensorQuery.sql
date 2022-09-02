------------
--Sensor 1--
------------
SELECT
 
Substring(BlobName,28,32) AS SERIAL_NUMBER,
System.Timestamp() AS WindowEndTime,
Min(SENSOR_1) AS minSensor1,
Max(SENSOR_1) AS maxSensor1
 
INTO [sensor1-alert]
FROM
[inputTimeSeries] TIMESTAMP BY CAST( SUBSTRING(DATE ,7 ,4)+'/'+
                                    SUBSTRING(DATE ,4 ,2)+'/'+
                                    SUBSTRING(DATE ,1 ,2)+' '+ HEURE AS DATETIME)
GROUP BY BlobName, TumblingWindow(second,30)
HAVING
  minSensor1 = maxSensor1 AND
  minSensor1 < 0.0
 
 
------------
--Sensor 2--
------------
SELECT
 
Substring(BlobName,28,32) AS SERIAL_NUMBER,
System.Timestamp() AS WindowEndTime,
Min(SENSOR_2) AS minSensor2,
Max(SENSOR_2) AS maxSensor2
INTO [sensor2-alert]
FROM
[inputTimeSeries] TIMESTAMP BY CAST( SUBSTRING(DATE ,7 ,4)+'/'+
                                    SUBSTRING(DATE ,4 ,2)+'/'+
                                    SUBSTRING(DATE ,1 ,2)+' '+ HEURE AS DATETIME)
GROUP BY BLobName, TumblingWindow(second,30)
HAVING
  minSensor2 > 45
  AND maxSensor2 < 50 
 
------------
--Sensor 4--
------------
SELECT
 
Substring(BlobName,28,32) AS SERIAL_NUMBER,
System.Timestamp() AS WindowEndTime,
Min(SENSOR_4) AS minSensor4,
Max(SENSOR_4) AS maxSensor4
INTO [sensor4-alert]
FROM
[inputTimeSeries] TIMESTAMP BY CAST( SUBSTRING(DATE ,7 ,4)+'/'+
                                    SUBSTRING(DATE ,4 ,2)+'/'+
                                    SUBSTRING(DATE ,1 ,2)+' '+ HEURE AS DATETIME)
GROUP BY BlobName,TumblingWindow(second,60)
HAVING
  minSensor4 = maxSensor4
 
 
 
------------
--Sensor 5--
------------
SELECT
 
Substring(BlobName,28,32) AS SERIAL_NUMBER,
System.Timestamp() AS WindowEndTime,
Min(SENSOR_5) AS minSensor5,
Max(SENSOR_5) AS maxSensor5
INTO [sensor5-alert]
FROM
[inputTimeSeries] TIMESTAMP BY CAST( SUBSTRING(DATE ,7 ,4)+'/'+
                                    SUBSTRING(DATE ,4 ,2)+'/'+
                                    SUBSTRING(DATE ,1 ,2)+' '+ HEURE AS DATETIME)
 
GROUP BY BlobName, TumblingWindow(second,30)
HAVING
  minSensor5 = maxSensor5
  
--------------
---Power BI---
--------------
 
SELECT HEURE, SENSOR_1, SENSOR_2, SENSOR_4, SENSOR_5
 
INTO [powerBI-RTdashboard]
FROM [inputTimeSeries] TIMESTAMP BY CAST( SUBSTRING(DATE ,7 ,4)+'/'+
                                    SUBSTRING(DATE ,4 ,2)+'/'+
                                    SUBSTRING(DATE ,1 ,2)+' '+ HEURE AS DATETIME)
