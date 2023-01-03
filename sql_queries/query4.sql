SELECT room_name
FROM (
        SELECT
            r.room_id,
            r.room_name,
            COUNT(DISTINCT s.sex) AS gender_variety
        FROM rooms AS r
            JOIN students AS s USING(room_id)
        GROUP BY 1
        HAVING COUNT(DISTINCT s.sex) > 1
    ) AS tmp