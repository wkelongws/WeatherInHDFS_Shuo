# WeatherInHDFS_Shuo

## souce code for processing weather data for NSF project TIMELI

Weather data are streamingly uploaded by another group on BOX then batch downloaded and pre-stored on HDFS in JSON format.

Data size is about 200GB per month. Total 4TB of 2 year data are stored on HDFS

This code provide 3 major functions:

1. parrallel process JSON data can convert data to CSV

2. calculated summary statistics of weather features for any given location in Iowa in any given aggreagation level higher than 5min

3. Rearrange data as matrix based on geolocation and convert data from CSV to PNG (images) then further convert the Sequence file format of Hadoop for space saving and future fast query.


### Here is [my presentaion](https://www.youtube.com/watch?v=fwpFZIJSotE&t=729s) of the weather visualizaion demo in 8th International Visualization in Transportation Symposium, Washington DC, July 27th, 2017 
