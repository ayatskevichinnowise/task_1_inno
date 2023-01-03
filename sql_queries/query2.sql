SELECT
    r.room_id,
    r.room_name,
    ROUND(AVG(EXTRACT(YEAR FROM s.birthday)), 2)
    :: FLOAT AS avg_stud_age
FROM rooms AS r
    JOIN students AS s USING(room_id)
GROUP BY 1
ORDER BY 3
LIMIT 5