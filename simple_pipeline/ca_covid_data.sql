CREATE TABLE IF NOT EXISTS "bitdotio/simple_pipeline"."ca_covid_data_join" (
  date date,
  county TEXT,
  fips TEXT,
  cases INTEGER,
  deaths INTEGER,
  pct_vaccinations_complete REAL,
  num_vaccinations_complete INTEGER,
  total_population INTEGER,
  population_16plus INTEGER,
  population_65plus INTEGER,
  cases_per_total_population REAL,
  deaths_per_total_population REAL
);

TRUNCATE TABLE "bitdotio/simple_pipeline"."ca_covid_data_join";

INSERT INTO "bitdotio/simple_pipeline"."ca_covid_data_join"
  SELECT
    CD.date,
    CD.county,
    CD.fips,
    CD.cases,
    CD.deaths,
    VACCS.series_complete_pop_pct AS pct_vaccinations_complete,
    VACCS.series_complete_yes AS num_vaccinations_complete,
    POP.total_population,
    POP.population_16plus,
    POP.population_65plus,
    1.0 * CD.cases / POP.total_population AS cases_per_total_population,
    1.0 * CD.deaths / POP.total_population AS deaths_per_total_population
  FROM "bitdotio/simple_pipeline"."cases_counties" AS CD
  LEFT JOIN "bitdotio/simple_pipeline"."vaccinations_counties" AS VACCS
  ON CD.fips = VACCS.fips
  LEFT JOIN "bitdotio/simple_pipeline"."population_counties" AS POP
  ON CD.fips = POP.fips
  WHERE CD.date >= '2021-05-01' AND CD.fips ~ '^06'
  ORDER BY CD.fips ASC, CD.date DESC;