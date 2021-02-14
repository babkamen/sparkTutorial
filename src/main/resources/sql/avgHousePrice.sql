Select MAX(CAST(Price as DECIMAL(10,2))) as maxPrice, ROUND(AVG(`Price SQ Ft`)) as avgPricePerSqFt, Location
from houses
GROUP BY Location
ORDER BY avgPricePerSqFt