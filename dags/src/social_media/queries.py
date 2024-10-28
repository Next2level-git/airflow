from psycopg2 import sql

user_querie = sql.SQL(
    """
SELECT id::text, username, full_name, bio, category, publications, profile_picture
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
    profile_picture = t.profile_picture::bytea
    FROM cte AS t
    WHERE f.id::text = t.id::text;
    """
)

instagram_post_querie = sql.SQL(
    """
SELECT id::text, user_id, description, created_at, views_count, likes_count, comments_count
FROM social_media.instagram_publications;
"""
)

update_instagram_post_query = sql.SQL(
    """
    UPDATE social_media.instagram_publications AS f
    SET views_count = t.views_count,
    likes_count = t.likes_count,
    comments_count = t.comments_count
    FROM cte AS t
    WHERE f.id::text = t.id::text;
    """
)
