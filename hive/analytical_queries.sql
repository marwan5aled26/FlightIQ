-- Use the FlightIQ database
USE flightiq;

-- Total number of flights in the dataset
SELECT COUNT(*) AS total_flights
FROM flights_clean;

-- Top 10 carriers with highest average arrival delay
SELECT 
    OP_UNIQUE_CARRIER AS carrier_code,
    COUNT(*) AS total_flights,
    ROUND(AVG(ARR_DELAY), 2) AS avg_delay_minutes,
    ROUND(SUM(CASE WHEN IS_DELAYED = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS delay_percentage
FROM flights_clean
GROUP BY OP_UNIQUE_CARRIER
HAVING COUNT(*) > 1000
ORDER BY avg_delay_minutes DESC
LIMIT 10;

-- Top 10 origin airports with highest average arrival delay
SELECT 
    ORIGIN AS airport_code,
    ORIGIN_CITY_NAME AS city,
    COUNT(*) AS total_departures,
    ROUND(AVG(ARR_DELAY), 2) AS avg_delay_minutes,
    ROUND(SUM(CASE WHEN IS_DELAYED = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS delay_percentage
FROM flights_clean
GROUP BY ORIGIN, ORIGIN_CITY_NAME
HAVING COUNT(*) > 1000
ORDER BY avg_delay_minutes DESC
LIMIT 10;

-- Average delay by day of week
SELECT 
    DAY_OF_WEEK,
    CASE DAY_OF_WEEK
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
        WHEN 7 THEN 'Sunday'
    END AS day_name,
    COUNT(*) AS total_flights,
    ROUND(AVG(ARR_DELAY), 2) AS avg_delay_minutes,
    ROUND(SUM(CASE WHEN IS_DELAYED = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS delay_percentage
FROM flights_clean
GROUP BY DAY_OF_WEEK
ORDER BY DAY_OF_WEEK;

-- Average delay by month (seasonality analysis)
SELECT 
    YEAR,
    MONTH,
    COUNT(*) AS total_flights,
    ROUND(AVG(ARR_DELAY), 2) AS avg_delay_minutes,
    ROUND(SUM(CASE WHEN IS_DELAYED = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS delay_percentage
FROM flights_clean
GROUP BY YEAR, MONTH
ORDER BY YEAR, MONTH;

-- Average delay breakdown by cause
SELECT 
    ROUND(AVG(CARRIER_DELAY), 2) AS avg_carrier_delay,
    ROUND(AVG(WEATHER_DELAY), 2) AS avg_weather_delay,
    ROUND(AVG(NAS_DELAY), 2) AS avg_nas_delay,
    ROUND(AVG(SECURITY_DELAY), 2) AS avg_security_delay,
    ROUND(AVG(LATE_AIRCRAFT_DELAY), 2) AS avg_late_aircraft_delay,
    ROUND(AVG(CARRIER_DELAY + WEATHER_DELAY + NAS_DELAY + SECURITY_DELAY + LATE_AIRCRAFT_DELAY), 2) AS avg_total_delay
FROM flights_clean;

-- Distance impact on delays
SELECT 
    CASE 
        WHEN DISTANCE < 500 THEN 'Short (<500 miles)'
        WHEN DISTANCE BETWEEN 500 AND 1000 THEN 'Medium (500-1000 miles)'
        WHEN DISTANCE BETWEEN 1000 AND 2000 THEN 'Long (1000-2000 miles)'
        ELSE 'Very Long (>2000 miles)'
    END AS distance_category,
    COUNT(*) AS total_flights,
    ROUND(AVG(ARR_DELAY), 2) AS avg_delay_minutes,
    ROUND(SUM(CASE WHEN IS_DELAYED = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS delay_percentage
FROM flights_clean
GROUP BY 
    CASE 
        WHEN DISTANCE < 500 THEN 'Short (<500 miles)'
        WHEN DISTANCE BETWEEN 500 AND 1000 THEN 'Medium (500-1000 miles)'
        WHEN DISTANCE BETWEEN 1000 AND 2000 THEN 'Long (1000-2000 miles)'
        ELSE 'Very Long (>2000 miles)'
    END
ORDER BY avg_delay_minutes DESC;