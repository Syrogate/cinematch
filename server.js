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
  ORDER BY
    start_year DESC
  LIMIT 10;
`;

const movieQuery = `
WITH MovieDetails AS (
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
    ARRAY_AGG(DISTINCT genre) AS genre_list
  FROM
    \`bigquery-public-data.imdb.title_basics\` tb
  LEFT JOIN
    \`bigquery-public-data.imdb.title_ratings\` tr
  ON
    tb.tconst = tr.tconst
  JOIN
    \`bigquery-public-data.imdb.title_principals\` tp
  ON
    tb.tconst = tp.tconst
  JOIN
    \`bigquery-public-data.imdb.name_basics\` nb
  ON
    tp.nconst = nb.nconst
  CROSS JOIN
    UNNEST(SPLIT(COALESCE(tb.genres, ''), ',')) AS genre
  WHERE
    tb.tconst = @tconst
    AND tb.title_type = 'movie'
    AND tb.genres IS NOT NULL
  GROUP BY
    tb.tconst, tb.primary_title, tb.start_year, tb.genres, tb.runtime_minutes, tr.average_rating, tr.num_votes
),
SimilarMovies AS (
  SELECT
    tb.tconst,
    tb.primary_title,
    tb.start_year,
    tb.genres,
    tr.average_rating,
    tr.num_votes,
    COUNT(DISTINCT genre) AS shared_genres,
    COUNT(DISTINCT tp.nconst) AS shared_actors
  FROM
    \`bigquery-public-data.imdb.title_basics\` tb
  LEFT JOIN
    \`bigquery-public-data.imdb.title_ratings\` tr
  ON
    tb.tconst = tr.tconst
  JOIN
    \`bigquery-public-data.imdb.title_principals\` tp
  ON
    tb.tconst = tp.tconst
  CROSS JOIN
    UNNEST(SPLIT(COALESCE(tb.genres, ''), ',')) AS genre
  WHERE
    tb.tconst != @tconst
    AND tb.title_type = 'movie'
    AND tb.genres IS NOT NULL
    AND (
      genre IN (SELECT genre FROM MovieDetails, UNNEST(genre_list) AS genre)
      OR tp.nconst IN (SELECT nconst FROM MovieDetails, UNNEST(actor_nconsts) AS nconst)
    )
  GROUP BY
    tb.tconst, tb.primary_title, tb.start_year, tb.genres, tr.average_rating, tr.num_votes
  HAVING
    shared_genres >= 1 OR shared_actors >= 1
  ORDER BY
    (shared_genres + shared_actors) DESC, tr.average_rating DESC, tr.num_votes DESC
  LIMIT 5
)
SELECT
  md.primary_title,
  md.start_year,
  md.genres,
  md.runtime_minutes,
  md.average_rating,
  md.num_votes,
  md.directors,
  md.actors,
  ARRAY_AGG(
    STRUCT(
      sm.primary_title,
      sm.start_year,
      sm.genres,
      sm.average_rating
    )
  ) AS similar_movies
FROM
  MovieDetails md
CROSS JOIN
  SimilarMovies sm
GROUP BY
  md.primary_title, md.start_year, md.genres, md.runtime_minutes, md.average_rating, md.num_votes, md.directors, md.actors;
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
      params: { tconst },
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
  const { genre, decade, duration, category, includeAdult } = req.body;

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
//---------------------------------------VENKATS CODE ENDS HERE--------------------------------








//----------------------------MAHALAKSHMI CODE STARTS HERE----------------------------
app.post('/api/actor', async (req, res) => {
  const { nconst } = req.body;

  if (!nconst) {
    return res.status(400).json({ error: 'Actor ID is required' });
  }

  const actor_query = `
    SELECT
      n.nconst,
      IFNULL(n.primary_name, 'Unknown') AS actor_name,
      IFNULL(n.birth_year, 0) AS actor_birth_year,
      IFNULL(
        CAST(
          CASE 
            WHEN n.birth_year IS NOT NULL AND n.birth_year != 0 THEN 2025 - n.birth_year
            ELSE NULL
          END AS STRING
        ),
        'Unknown'
      ) AS actor_age,
      IFNULL(n.primary_profession, 'Unknown') AS primary_profession,
      ARRAY_AGG(DISTINCT t.primary_title IGNORE NULLS) AS known_for_movies,
      ARRAY_AGG(
        DISTINCT TO_JSON_STRING(
          STRUCT(
            IFNULL(t.primary_title, 'Unknown') AS title,
            IFNULL(
              REGEXP_REPLACE(
                JSON_EXTRACT_ARRAY(tp.characters)[SAFE_OFFSET(0)],
                r'^"(.*)"$', r'\\1'
              ),
              'Unknown'
            ) AS character
          )
        )
        IGNORE NULLS
      ) AS title_and_character
    FROM \`bigquery-public-data.imdb.name_basics\` n
    LEFT JOIN UNNEST(SPLIT(n.known_for_titles, ',')) kft ON TRUE
    LEFT JOIN \`bigquery-public-data.imdb.title_basics\` t
      ON t.tconst = kft AND t.title_type = 'movie'
    LEFT JOIN \`bigquery-public-data.imdb.title_principals\` tp
      ON tp.tconst = t.tconst AND tp.nconst = n.nconst AND tp.category = 'actor'
    WHERE
      n.nconst = @nconst
      AND IFNULL(SPLIT(n.primary_profession, ',')[SAFE_OFFSET(0)], '') = 'actor'
    GROUP BY
      n.nconst, n.primary_name, n.birth_year, n.primary_profession
    LIMIT 10
  `;

  const params = { nconst };

  try {
    const [rows] = await bigquery.query({ query: actor_query, params, useLegacySql: false });
    // Parse the title_and_character JSON strings into objects
    if (rows.length > 0 && rows[0].title_and_character) {
      rows[0].title_and_character = rows[0].title_and_character.map(str => JSON.parse(str));
    }
    res.json(rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Internal server error' });
  }
});


//----------------------------MAHALAKSHMI CODE ENDS HERE----------------------------

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});