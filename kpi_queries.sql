-- ========================================
-- GENRE-LEVEL KPIs
-- ========================================

-- 1. Listen Count: Total number of times tracks in a genre have been played
SELECT 
    group_by AS genre,
    genre_listen_count AS listen_count
FROM public.kpi_table 
WHERE kpi_type = 'genre'
ORDER BY genre_listen_count DESC;

-- 2. Average Track Duration: The mean duration of all tracks within a genre
SELECT 
    group_by AS genre,
    ROUND(genre_avg_duration, 2) AS avg_track_duration_ms
FROM public.kpi_table 
WHERE kpi_type = 'genre'
ORDER BY genre_avg_duration DESC;

-- 3. Popularity Index: A computed score based on play counts, likes, and shares for tracks in a genre
SELECT 
    group_by AS genre,
    ROUND(genre_popularity_index, 2) AS popularity_index
FROM public.kpi_table 
WHERE kpi_type = 'genre'
ORDER BY genre_popularity_index DESC;

-- 4. Most Popular Track per Genre: The track with the highest engagement in each genre
SELECT 
    group_by AS genre,
    top_track AS most_popular_track,
    top_artist AS artist
FROM public.kpi_table 
WHERE kpi_type = 'genre'
ORDER BY group_by;

-- ========================================
-- HOURLY KPIs
-- ========================================

-- 5. Unique Listeners: The distinct number of users streaming music in a given hour
SELECT 
    CAST(group_by AS INTEGER) AS hour_of_day,
    genre_listen_count AS unique_listeners
FROM public.kpi_table 
WHERE kpi_type = 'hourly'
ORDER BY CAST(group_by AS INTEGER);

-- 6. Top Artists per Hour: The most streamed artists during each hour
SELECT 
    CAST(group_by AS INTEGER) AS hour_of_day,
    top_artist AS top_artist_this_hour
FROM public.kpi_table 
WHERE kpi_type = 'hourly'
ORDER BY CAST(group_by AS INTEGER);

-- 7. Track Diversity Index: A measure of how varied the tracks played in an hour are
SELECT 
    CAST(group_by AS INTEGER) AS hour_of_day,
    ROUND(genre_avg_duration, 4) AS track_diversity_index
FROM public.kpi_table 
WHERE kpi_type = 'hourly'
ORDER BY CAST(group_by AS INTEGER);