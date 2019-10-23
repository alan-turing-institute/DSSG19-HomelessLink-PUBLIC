create table raw.weather_raw (

Unnamed varchar,
X varchar,
time varchar,
summary varchar,
icon varchar,
precipIntensity varchar,
precipProbability varchar,
temperature varchar,
apparentTemperature varchar,
dewPoint varchar,
humidity varchar,
pressure varchar,
windSpeed varchar,
windGust varchar,
windBearing varchar,
cloudCover varchar,
uvIndex varchar,
visibility varchar,
precipType varchar,
precipAccumulation varchar
);

\copy raw.weather_raw from '~/../../data/1_8_2019/London_all.csv' header csv delimiter ',';
