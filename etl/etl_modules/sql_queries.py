SQL_FILMWORD_DATA = """
    SELECT
        fw.id,
        fw.rating AS rating,
        fw.title,
        fw.description,
        COALESCE (json_agg(DISTINCT g.name)) AS genre,
        COALESCE (json_agg(DISTINCT p.full_name) FILTER (WHERE pfw.role = 'director'), '[]') as director,
        json_agg(DISTINCT p.full_name) FILTER (WHERE pfw.role = 'actor') as actors_names,
        json_agg(DISTINCT p.full_name) FILTER (WHERE pfw.role = 'writer') as writers_names,
        json_agg(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name)) FILTER (WHERE pfw.role = 'actor') as actors,
        json_agg(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name)) FILTER (WHERE pfw.role = 'writer') as writers
    FROM "content".film_work as fw
    LEFT JOIN "content".genre_film_work gfw on gfw.film_work_id = fw.id
    LEFT JOIN "content".genre g on g.id = gfw.genre_id
    LEFT JOIN "content".person_film_work pfw on pfw.film_work_id = fw.id
    LEFT JOIN "content".person p on p.id = pfw.person_id
    WHERE fw.id IN %s
    GROUP BY fw.id
"""

SQL_FILMWORK_IDS = """
    SELECT id
    FROM content.film_work as fw 
    {filter}
"""

SQL_FILMWORK_IDS_BY_PERSONS = """
    SELECT pfw.film_work_id
    FROM content.person as p
    LEFT JOIN content.person_film_work as pfw
    ON p.id = pfw.person_id
    {filter}
"""

SQL_FILMWORK_IDS_BY_GENRES = """
    SELECT gfw.film_work_id
    FROM content.genre as g
    LEFT JOIN content.genre_film_work as gfw
    ON g.id = gfw.genre_id
    {filter}
"""