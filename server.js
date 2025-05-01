const express = require('express');
const { BigQuery } = require('@google-cloud/bigquery');
const redis = require('redis');
const cors = require('cors');

const app = express();
const bigquery = new BigQuery();
const redisClient = redis.createClient({ url: 'redis://localhost:6379' });

redisClient.connect().catch(console.error);
app.use(cors());
app.use(express.json());

//--------------------------------CALEBS CODE STARTS HERE--------------------------------
const searchQuery = `
  SELECT
    tconst,
    primary_title,
    start_year
  FROM
    \`bigquery-public-data.imdb.title_basics\`
  WHERE
    title_type = 'movie'
    AND genres IS NOT NULL
    AND primary_title LIKE @title
    AND start_year >= 1980
    AND start_year <= 2025
  ORDER BY
    start_year DESC
  LIMIT 10;
`;

const movieQuery = `
-- Step 1: Precompute target movie details with strict filters
WITH TargetMovie AS (
  SELECT
    tb.tconst,
    tb.primary_title,
    tb.start_year,
    tb.genres,
    tb.runtime_minutes,
    tr.average_rating,
    tr.num_votes,
    ARRAY_AGG(DISTINCT IF(tp.category = 'director', nb.primary_name, NULL) IGNORE NULLS) AS directors,
    ARRAY_AGG(DISTINCT IF(tp.category IN ('actor', 'actress'), nb.primary_name, NULL) IGNORE NULLS) AS actors,
    ARRAY_AGG(DISTINCT IF(tp.category IN ('actor', 'actress'), tp.nconst, NULL) IGNORE NULLS) AS actor_nconsts,
    ARRAY(SELECT DISTINCT TRIM(genre) FROM UNNEST(SPLIT(COALESCE(tb.genres, ''), ',')) AS genre WHERE TRIM(genre) != '') AS genre_list
  FROM
    \`bigquery-public-data.imdb.title_basics\` tb
  LEFT JOIN
    \`bigquery-public-data.imdb.title_ratings\` tr
  ON
    tb.tconst = tr.tconst
  LEFT JOIN
    \`bigquery-public-data.imdb.title_principals\` tp
  ON
    tb.tconst = tp.tconst
  LEFT JOIN
    \`bigquery-public-data.imdb.name_basics\` nb
  ON
    tp.nconst = nb.nconst
  WHERE
    tb.tconst = @target_tconst
    AND tb.title_type = 'movie'
    AND tb.genres IS NOT NULL
    AND tb.start_year BETWEEN 1980 AND 2025
  GROUP BY
    tb.tconst, tb.primary_title, tb.start_year, tb.genres, tb.runtime_minutes, tr.average_rating, tr.num_votes
),

-- Step 2: Create hash tables for genres and actors (materialized for performance)
GenreHash AS (
  SELECT DISTINCT TRIM(genre) AS genre
  FROM TargetMovie, UNNEST(genre_list) AS genre
  WHERE TRIM(genre) != ''
),

ActorHash AS (
  SELECT DISTINCT nconst
  FROM TargetMovie, UNNEST(actor_nconsts) AS nconst
  WHERE nconst IS NOT NULL
),

-- Step 3: Pre-filter candidate movies with strict filters (year, num_votes, title_type)
FilteredMovies AS (
  SELECT
    tb.tconst,
    tb.primary_title,
    tb.start_year,
    tb.genres,
    tr.average_rating,
    tr.num_votes,
    ARRAY(SELECT TRIM(genre) FROM UNNEST(SPLIT(COALESCE(tb.genres, ''), ',')) AS genre WHERE TRIM(genre) != '') AS genre_list
  FROM
    \`bigquery-public-data.imdb.title_basics\` tb
  JOIN
    \`bigquery-public-data.imdb.title_ratings\` tr
  ON
    tb.tconst = tr.tconst
  WHERE
    tb.tconst != @target_tconst
    AND tb.title_type = 'movie'
    AND tb.genres IS NOT NULL
    AND tr.num_votes > 5000 -- Stricter filter to reduce candidates
    AND tb.start_year BETWEEN 1980 AND 2025
),

-- Step 4: Compute genre matches using BNL join strategy
GenreMatches AS (
  SELECT
    fm.tconst,
    fm.primary_title,
    fm.start_year,
    fm.genres,
    fm.average_rating,
    fm.num_votes,
    COUNT(DISTINCT gh.genre) AS shared_genres
  FROM
    FilteredMovies fm
  CROSS JOIN
    UNNEST(fm.genre_list) AS genre
  JOIN
    GenreHash gh
  ON
    TRIM(genre) = gh.genre
  GROUP BY
    fm.tconst, fm.primary_title, fm.start_year, fm.genres, fm.average_rating, fm.num_votes
  HAVING
    shared_genres >= 1
),

-- Step 5: Compute actor matches using BNL join strategy
ActorMatches AS (
  SELECT
    fm.tconst,
    COUNT(DISTINCT ah.nconst) AS shared_actors
  FROM
    FilteredMovies fm
  JOIN
    \`bigquery-public-data.imdb.title_principals\` tp
  ON
    fm.tconst = tp.tconst
  JOIN
    ActorHash ah
  ON
    tp.nconst = ah.nconst
  WHERE
    tp.category IN ('actor', 'actress')
  GROUP BY
    fm.tconst
  HAVING
    shared_actors >= 1
),

-- Step 6: Combine matches and compute similar movies
SimilarMovies AS (
  SELECT
    gm.tconst,
    gm.primary_title,
    gm.start_year,
    gm.genres,
    gm.average_rating,
    gm.num_votes,
    gm.shared_genres,
    COALESCE(am.shared_actors, 0) AS shared_actors
  FROM
    GenreMatches gm
  LEFT JOIN
    ActorMatches am
  ON
    gm.tconst = am.tconst
  WHERE
    gm.shared_genres >= 1 OR COALESCE(am.shared_actors, 0) >= 1
  ORDER BY
    (gm.shared_genres + COALESCE(am.shared_actors, 0)) DESC,
    gm.average_rating DESC,
    gm.num_votes DESC
  LIMIT 5
)

-- Step 7: Final result
SELECT
  tm.primary_title,
  tm.start_year,
  tm.genres,
  tm.runtime_minutes,
  tm.average_rating,
  tm.num_votes,
  tm.directors,
  tm.actors,
  ARRAY_AGG(
    STRUCT(
      sm.tconst AS tconst,
      sm.primary_title,
      sm.start_year,
      sm.genres,
      sm.average_rating
    )
  ) AS similar_movies
FROM
  TargetMovie tm
CROSS JOIN
  SimilarMovies sm
GROUP BY
  tm.primary_title, tm.start_year, tm.genres, tm.runtime_minutes, tm.average_rating, tm.num_votes, tm.directors, tm.actors;
`;

app.post('/api/search', async (req, res) => {
  const { title } = req.body;
  if (!title || title.length < 2) {
    return res.status(400).json({ error: 'Title must be at least 2 characters' });
  }

  const cacheKey = `search:${title.toLowerCase()}`;
  try {
    const cached = await redisClient.get(cacheKey);
    if (cached) {
      return res.json(JSON.parse(cached));
    }

    const queryOptions = {
      query: searchQuery,
      params: { title: `%${title}%` },
    };
    const [rows] = await bigquery.query(queryOptions);
    await redisClient.setEx(cacheKey, 86400, JSON.stringify(rows));
    res.json(rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.post('/api/movie', async (req, res) => {
  const { tconst } = req.body;
  if (!tconst || !tconst.match(/^tt\d+$/)) {
    return res.status(400).json({ error: 'Invalid tconst' });
  }

  const cacheKey = `movie_details:${tconst}`;
  try {
    const cached = await redisClient.get(cacheKey);
    if (cached) {
      return res.json(JSON.parse(cached));
    }

    const queryOptions = {
      query: movieQuery,
      params: { target_tconst: tconst },
    };
    const [rows] = await bigquery.query(queryOptions);
    if (!rows.length) {
      return res.status(404).json({ error: 'Movie not found' });
    }

    await redisClient.setEx(cacheKey, 86400, JSON.stringify(rows));
    res.json(rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Internal server error' });
  }
});
//--------------------------------CALEBS CODE ENDS HERE--------------------------------

//---------------------------------------VENKATS CODE STARTS HERE--------------------------------
app.post('/api/filter', async (req, res) => {
  const { searchTerm, genre, decade, duration, category, includeAdult } = req.body;

  // Base query
  let query = `
    SELECT 
      b.tconst,
      b.primary_title,
      b.start_year,
      b.genres,
      b.runtime_minutes,
      r.average_rating,
      r.num_votes
    FROM \`bigquery-public-data.imdb.title_basics\` AS b
    JOIN \`bigquery-public-data.imdb.title_ratings\` AS r
    ON b.tconst = r.tconst
    WHERE b.title_type = 'movie'
      AND b.start_year IS NOT NULL
  `;

  // Filters
  const params = {};

  if (!includeAdult) {
    query += ` AND b.is_adult = 0`;
  }

  if (searchTerm) {
    query += ` AND b.primary_title LIKE @searchTerm`;
    params.searchTerm = `%${searchTerm}%`;
  }

  if (genre) {
    query += ` AND b.genres LIKE @genre`;
    params.genre = `%${genre}%`;
  }

  if (decade) {
    const startYear = parseInt(decade, 10);
    const endYear = startYear + 9;
    query += ` AND b.start_year BETWEEN @startYear AND @endYear`;
    params.startYear = startYear;
    params.endYear = endYear;
  }

  if (duration) {
    if (duration === 'short') {
      query += ` AND b.runtime_minutes < 60`;
    } else if (duration === 'medium') {
      query += ` AND b.runtime_minutes BETWEEN 60 AND 120`;
    } else if (duration === 'long') {
      query += ` AND b.runtime_minutes > 120`;
    }
  }

  // Apply category sorting and num_votes logic
  if (category === 'popular') {
    query += ` AND r.num_votes > 10000 ORDER BY r.num_votes DESC`;
  } else if (category === 'underrated') {
    query += ` AND r.average_rating > 8.0 AND r.num_votes < 10000 ORDER BY r.average_rating DESC`;
  } else if (category === 'controversial') {
    query += ` AND r.average_rating BETWEEN 5.5 AND 7.0 AND r.num_votes > 10000 ORDER BY r.num_votes DESC`;
  } else {
    // Default ordering if no special category selected
    query += ` AND r.num_votes > 10000 ORDER BY r.num_votes DESC`;
  }

  query += ` LIMIT 10;`;

  try {
    const queryOptions = {
      query,
      params,
    };

    const [rows] = await bigquery.query(queryOptions);
    res.json(rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.post('/api/actors', async (req, res) => {
  const { name } = req.body;

  if (!name || name.length < 2) {
    return res.status(400).json({ error: 'Actor name must be at least 2 characters' });
  }

  const cacheKey = `actor_search:${name.toLowerCase()}`;
  const actorSearchQuery = `
    WITH FilteredNames AS (
      SELECT
        n.nconst,
        n.primary_name,
        n.birth_year,
        n.primary_profession
      FROM
        \`bigquery-public-data.imdb.name_basics\` n
      WHERE
        LOWER(n.primary_name) LIKE LOWER(@name)
        AND IFNULL(SPLIT(n.primary_profession, ',')[SAFE_OFFSET(0)], '') IN ('actor', 'actress')
    ),
    FilteredPrincipals AS (
      SELECT
        tp.nconst,
        tp.tconst,
        tp.characters
      FROM
        \`bigquery-public-data.imdb.title_principals\` tp
      JOIN
        FilteredNames fn
      ON
        tp.nconst = fn.nconst
      WHERE
        tp.category IN ('actor', 'actress')
    ),
    TopMovies AS (
      SELECT
        fp.nconst,
        t.primary_title,
        REGEXP_REPLACE(JSON_EXTRACT_ARRAY(fp.characters)[SAFE_OFFSET(0)], r'^"(.*)"$', r'\\1') AS character,
        t.start_year
      FROM
        FilteredPrincipals fp
      JOIN
        \`bigquery-public-data.imdb.title_basics\` t
      ON
        fp.tconst = t.tconst
      WHERE
        t.title_type = 'movie'
        AND t.start_year BETWEEN 1980 AND 2025
      QUALIFY
        ROW_NUMBER() OVER (PARTITION BY fp.nconst ORDER BY t.start_year DESC) <= 5
    )
    SELECT DISTINCT
      fn.nconst,
      IFNULL(fn.primary_name, 'Unknown') AS actor_name,
      fn.birth_year AS birth_year,
      IFNULL(
        CAST(
          CASE 
            WHEN fn.birth_year IS NOT NULL THEN 2025 - fn.birth_year
            ELSE NULL
          END AS STRING
        ),
        'Unknown'
      ) AS age,
      IFNULL(fn.primary_profession, 'Unknown') AS profession,
      ARRAY_AGG(DISTINCT tm.primary_title IGNORE NULLS) AS known_for_movies,
      ARRAY_AGG(
        DISTINCT TO_JSON_STRING(
          STRUCT(
            IFNULL(tm.primary_title, 'Unknown') AS title,
            IFNULL(tm.character, 'Unknown') AS character
          )
        ) IGNORE NULLS
      ) AS movies_and_characters
    FROM
      FilteredNames fn
    LEFT JOIN
      TopMovies tm
    ON
      fn.nconst = tm.nconst
    GROUP BY
      fn.nconst, fn.primary_name, fn.birth_year, fn.primary_profession
    ORDER BY
      actor_name ASC
    LIMIT 10;
  `;

  const params = { name: `%${name}%` };

  try {
    // Check Redis cache first
    const cached = await redisClient.get(cacheKey);
    if (cached) {
      return res.json(JSON.parse(cached));
    }

    // Log the query and parameters for debugging
    console.log('Actor Search Query:', actorSearchQuery);
    console.log('Parameters:', params);

    const [rows] = await bigquery.query({ query: actorSearchQuery, params });
    console.log('Actor Search Results:', rows);

    // Parse the movies_and_characters JSON strings into objects
    rows.forEach(row => {
      if (row.movies_and_characters) {
        row.movies_and_characters = row.movies_and_characters.map(str => JSON.parse(str));
      }
    });

    // Cache the results in Redis for 24 hours (86400 seconds)
    await redisClient.setEx(cacheKey, 86400, JSON.stringify(rows));
    res.json(rows);
  } catch (err) {
    console.error('Error in /api/actors:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});
//---------------------------------------VENKATS CODE ENDS HERE--------------------------------

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
