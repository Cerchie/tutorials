SET 'auto.offset.reset' = 'earliest';

SELECT CITY_ID, NAME, STATE FROM CITIES EMIT CHANGES LIMIT 6;