insertion = {
    "json_array": """
INSERT INTO {table}
SELECT *
FROM json_populate_recordset(null::{table},
                            {json})
ON CONFLICT DO NOTHING
                    """
    }

queries = {
    'students_in_rooms': '''
SELECT
    r.id,
    r.name,
    COUNT(s.id) AS count_of_students
FROM rooms r
    LEFT JOIN students s ON r.id = s.room
GROUP BY 1
ORDER BY 1
''',

    'top5_rooms_with_small_avg_age': '''
SELECT
    r.id,
    r.name,
    ROUND(AVG(EXTRACT(YEAR FROM s.birthday)), 2)
    :: FLOAT AS avg_stud_age
FROM rooms AS r
    JOIN students AS s ON r.id = s.room
GROUP BY 1
ORDER BY 3
LIMIT 5
''',

    'top5_rooms_high_age_diff': '''
SELECT
    r.id,
    r.name,
    MAX(EXTRACT(YEAR FROM s.birthday)) -
    MIN(EXTRACT(YEAR FROM s.birthday))
    :: FLOAT AS age_diff
FROM rooms AS r
    JOIN students AS s ON r.id = s.room
GROUP BY 1
ORDER BY 3 DESC
LIMIT 5
''',

    'rooms_with_diff_gender': '''
SELECT name
FROM (
        SELECT
            r.id,
            r.name,
            COUNT(DISTINCT s.sex) AS gender_variety
        FROM rooms AS r
            JOIN students AS s ON r.id = s.room
        GROUP BY 1
        HAVING COUNT(DISTINCT s.sex) > 1
    ) AS tmp'''
            }
