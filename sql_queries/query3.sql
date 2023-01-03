SELECT
    r.room_id,
    r.room_name,
    MAX(EXTRACT(YEAR FROM s.birthday)) -
    MIN(EXTRACT(YEAR FROM s.birthday))
    :: FLOAT AS age_diff
FROM rooms AS r
    JOIN students AS s USING(room_id)
GROUP BY 1
ORDER BY 3 DESC
LIMIT 5