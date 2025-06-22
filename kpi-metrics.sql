-- Listen Count per Genre per Day
WITH listen_count AS (
  SELECT 
    DATE(listen_time) AS listen_date,
    track_genre,
    COUNT(*) AS listen_count
  FROM listens
  GROUP BY listen_date, track_genre
),

-- Unique Listeners per Genre per Day
unique_listeners AS (
  SELECT 
    DATE(listen_time) AS listen_date,
    track_genre,
    COUNT(DISTINCT user_id) AS unique_listeners
  FROM listens
  GROUP BY listen_date, track_genre
),

-- Total Listening Time per Genre per Day
total_listening_time AS (
  SELECT 
    DATE(listen_time) AS listen_date,
    track_genre,
    SUM(duration_ms) AS total_listening_time_ms
  FROM listens
  GROUP BY listen_date, track_genre
),

-- Average Listening Time per User per Genre per Day
avg_listening_time AS (
  SELECT 
    listen_date,
    track_genre,
    AVG(user_duration) AS avg_listening_time_per_user
  FROM (
    SELECT 
      user_id,
      DATE(listen_time) AS listen_date,
      track_genre,
      SUM(duration_ms) AS user_duration
    FROM listens
    GROUP BY user_id, listen_date, track_genre
  ) user_day_genre
  GROUP BY listen_date, track_genre
),

-- Top 3 Songs per Genre per Day
top_3_songs AS (
  SELECT *
  FROM (
    SELECT 
      DATE(listen_time) AS listen_date,
      track_genre,
      track_name,
      COUNT(*) AS play_count,
      ROW_NUMBER() OVER (
        PARTITION BY DATE(listen_time), track_genre 
        ORDER BY COUNT(*) DESC
      ) AS song_rank
    FROM listens
    GROUP BY listen_date, track_genre, track_name
  ) ranked_songs
  WHERE song_rank <= 3
),

-- Top 5 Genres per Day by Listen Count
top_5_genres AS (
  SELECT *
  FROM (
    SELECT 
      DATE(listen_time) AS listen_date,
      track_genre,
      COUNT(*) AS listen_count,
      ROW_NUMBER() OVER (
        PARTITION BY DATE(listen_time) 
        ORDER BY COUNT(*) DESC
      ) AS genre_rank
    FROM listens
    GROUP BY listen_date, track_genre
  ) ranked_genres
  WHERE genre_rank <= 5
)

-- Final Output: Combine All Metrics
SELECT 
  lc.listen_date,
  lc.track_genre,
  lc.listen_count,
  ul.unique_listeners,
  tlt.total_listening_time_ms,
  alt.avg_listening_time_per_user
FROM listen_count lc
LEFT JOIN unique_listeners ul
  ON lc.listen_date = ul.listen_date AND lc.track_genre = ul.track_genre
LEFT JOIN total_listening_time tlt
  ON lc.listen_date = tlt.listen_date AND lc.track_genre = tlt.track_genre
LEFT JOIN avg_listening_time alt
  ON lc.listen_date = alt.listen_date AND lc.track_genre = alt.track_genre
ORDER BY lc.listen_date, lc.track_genre;

-- Optional: View Top 3 Songs per Genre per Day
SELECT * FROM top_3_songs ORDER BY listen_date, track_genre, song_rank;

-- Optional: View Top 5 Genres per Day by Listen Count
SELECT * FROM top_5_genres ORDER BY listen_date, genre_rank;
