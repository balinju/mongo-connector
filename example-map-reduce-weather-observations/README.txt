Mongo Map Reduce Weather Observations Demo
===========================================

INTRODUCTION
   This demo shows how to use the map-reduce operation, and add objects passing its Json
   by getting weather observations from an HTTP service,
   adding them to a Mongo collection, and map-reducing it in order to get average temperatures for each city    

HOW TO DEMO:
  This demo has no prerequisites aside having a mongodb running locally.
  1. Run the MongoFunctionalTestDriver, or deploy this demo an a Mule Container. 
  	a. Add one or more a weather observations for one or more airports: 
  		Run the testAddWeatherObservation  test or alternatively hit 
  		http://localhost:9091/mongo-demo-add-weater-observations,
  		passing an ICAO code - http://en.wikipedia.org/wiki/List_of_airports_by_ICAO_code. 
  	Example: http://localhost:9091/mongo-demo-add-weater-observations?cityIcao=KMCO
  2. Consult the average temperature of any previously added airport: 
  		Run the testGetAverageTemperature test or alternatively hit
  		http://localhost:9091/mongo-demo-get-average-temperature. This will return the average of 
  		temperatures for each observation for the given airport
  		Example: http://localhost:9091/mongo-demo-get-average-temperature?cityIcao=KMCO
 	
HOW IT WORKS:
  * The AddWeatherObservation flow queries and HTTP service that has a JSon output, and adds the result 
  to a weatherObservations collection. 
  * The GetAverageTemperature flow uses map-reduce to group weather observations by ICAO code, 
  and get the average of temperatures for each observations group  
  
  