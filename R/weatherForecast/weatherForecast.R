############## Weather forecast BBD 2016 ##########################

# Set working directory
setwd("C:/Users/sanjana.p/Desktop/weatherForecast/")

# Read file with latitudes and longitudes of the cities whose forecast is needed
latLong = read.csv("weatherForecast_latLong.csv")

# Get weather forecast from Dark Sky 
library(jsonlite)

for(i in 1: nrow(latLong))
{
   lat = latLong$latitude[i]  
   long = latLong$longitude[i]
   
   LL = paste("https://api.forecast.io/forecast/08786e5c062fcedb081c634ce05948a9/", lat, ",", long, sep = "")
   
   jsonFile <- fromJSON(txt=LL)
   
   weatherForecast = jsonFile$daily$data
   weatherForecast$lat = jsonFile$latitude
   weatherForecast$long = jsonFile$longitude
   weatherForecast$Date = as.POSIXct(as.numeric(as.character(weatherForecast$time)),origin="1970-01-01",tz="Asia/Kolkata")
   weatherForecast$precipIntensityMaxTime = as.POSIXct(as.numeric(as.character(weatherForecast$precipIntensityMaxTime)),origin="1970-01-01",tz="Asia/Kolkata")
   weatherForecast$temperatureMinTime = as.POSIXct(as.numeric(as.character(weatherForecast$temperatureMinTime)),origin="1970-01-01",tz="Asia/Kolkata")
   weatherForecast$temperatureMaxTime = as.POSIXct(as.numeric(as.character(weatherForecast$temperatureMaxTime)),origin="1970-01-01",tz="Asia/Kolkata")
   weatherForecast$Day = weekdays(weatherForecast$Date)
   weatherForecast$rain = ifelse(weatherForecast$icon == "rain", 1,0)
   weatherForecast$city = latLong$city[i]
   weatherForecast = weatherForecast[c("city", "Date", "Day", "summary", "icon", "rain", "precipProbability", "precipIntensity", "precipIntensityMax", "precipIntensityMaxTime", "temperatureMin", "temperatureMinTime", "temperatureMax", "temperatureMaxTime", "dewPoint", "humidity", "windSpeed", "windBearing", "cloudCover", "pressure")]
   colnames(weatherForecast) = c("City", "Date", "Day", "Weather summary", "Weather icon", "Rain", "Precipitation probability", "Precipitation intensity", "Precipitation intensityMax", "Precipitation intensityMaxTime", "TemperatureMin", "TemperatureMinTime", "TemperatureMax", "TemperatureMaxTime", "Dew Point", "Humidity", "Wind speed", "Wind direction", "Cloud cover", "Pressure")
   
   weatherForecast = weatherForecast[1:4,]
   
   if(i==1)
   {
      weatherComplete = weatherForecast
   }else
   {
      weatherComplete = rbind(weatherComplete, weatherForecast)
   }
}

# Write weather forecast file to the required location
sysTime = format(Sys.time(),"%Y-%m-%d-%H-%M-%S") 
write.csv(weatherComplete, file = paste("weatherForecast_", sysTime, ".csv",  sep = ""), row.names = FALSE)
