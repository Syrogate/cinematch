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

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});