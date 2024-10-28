from psycopg2 import sql

user_querie = sql.SQL(
    """
SELECT id, username, full_name, bio, category, publications, last_publication
FROM social_media.users;
"""
)

update_users_query = sql.SQL(
    """
    UPDATE social_media.users AS f
    SET username = t.username,
    full_name = t.full_name,
    bio = t.bio,
    category = t.category,
    publications = t.publications,
    last_publication = t.last_publication,
    FROM cte AS t
    WHERE f.id = t.id;
    """
)
