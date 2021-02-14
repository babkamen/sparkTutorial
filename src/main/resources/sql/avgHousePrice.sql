Select MAX(CAST(Price as DECIMAL(10,2))) as maxPrice, ROUND(AVG(CAST(`Price SQ Ft` as DECIMAL(10,2)))) as avgPricePerSqFt, Location
from houses
GROUP BY Location
ORDER BY avgPricePerSqFt