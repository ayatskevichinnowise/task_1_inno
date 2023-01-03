SELECT
    r.room_id,
    r.room_name,
    COUNT(s.student_id) AS count_of_students
FROM rooms r
    LEFT JOIN students s USING(room_id)
GROUP BY 1
ORDER BY 1