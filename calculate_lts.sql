-- Calculate Level of Traffic Stress (LTS) for model network
-- Replicates the exact methodology from old_lts.py

-- This methodology uses lookup tables based on:
-- 1. Number of lanes (numlanes)
-- 2. Speed limit (posted_speed or vcur_prt~2)
-- 3. Link type (typeno) - residential (72, 79) vs other
-- 4. Bike facility type (bike_fac~1)

-- Create view with parsed speed
DROP VIEW IF EXISTS output.model_network_with_parsed_speed CASCADE;
CREATE VIEW output.model_network_with_parsed_speed AS
SELECT
  *,
  -- Combine posted_speed with parsed vcur_prt~2 as fallback
  COALESCE(
    posted_speed,
    -- Parse speed from vcur_prt~2 field (format: "25mph" or "25")
    CASE
      WHEN "vcur_prt~2" IS NOT NULL THEN
        (regexp_match("vcur_prt~2", '(\d+)'))[1]::numeric
      ELSE NULL
    END
  ) as speed_mph
FROM output.model_network_with_speed;

-- Calculate LTS using the exact lookup table methodology
DROP TABLE IF EXISTS output.model_network_with_lts CASCADE;
CREATE TABLE output.model_network_with_lts as
WITH lts_calc AS (
  SELECT
    gid,
    no,
    fromnodeno,
    tonodeno,
    typeno,
    length,
    "bike_fac~1" as bike_facility,
    rise_run as slope,
    "county_c~3" as county_code,
    numlanes+"reversel~4" as totnumlanes,
    speed_mph as vehiclespeed,
    -- First, determine the row index based on lanes, speed, and link type
    -- Residential roads (typeno 72 or 79) with 1-2 lanes use residential_index
    -- All others use road_index
    CASE
      -- Residential index (for link types 72, 79 with 1-2 lanes)
      WHEN typeno IN ('72', '79') AND numlanes IN (1, 2) THEN
        CASE
          WHEN numlanes = 0 THEN 0                                    -- Row 0: no lanes
          WHEN numlanes BETWEEN 1 AND 2 AND speed_mph BETWEEN 0 AND 25 THEN 1   -- Row 1
          WHEN numlanes BETWEEN 1 AND 2 AND speed_mph BETWEEN 26 AND 65 THEN 2  -- Row 2
          ELSE 0
        END
      -- Road index (for all other roads)
      ELSE
        CASE
          WHEN numlanes = 0 THEN 0                                              -- Row 0: no lanes
          WHEN numlanes = -2 AND speed_mph BETWEEN 0 AND 25 THEN 1              -- Row 1 (residential fallback)
          WHEN numlanes = -2 AND speed_mph BETWEEN 26 AND 36 THEN 2             -- Row 2 (residential fallback)
          WHEN numlanes BETWEEN 1 AND 3 AND speed_mph BETWEEN 0 AND 25 THEN 3   -- Row 3
          WHEN numlanes BETWEEN 4 AND 5 AND speed_mph BETWEEN 0 AND 25 THEN 4   -- Row 4
          WHEN numlanes BETWEEN 1 AND 3 AND speed_mph BETWEEN 26 AND 34 THEN 5  -- Row 5
          WHEN numlanes >= 6 AND speed_mph BETWEEN 0 AND 25 THEN 6              -- Row 6
          WHEN numlanes BETWEEN 4 AND 5 AND speed_mph BETWEEN 26 AND 34 THEN 7  -- Row 7
          WHEN numlanes >= 6 AND speed_mph BETWEEN 26 AND 34 THEN 8             -- Row 8
          WHEN numlanes BETWEEN 1 AND 3 AND speed_mph >= 35 THEN 9              -- Row 9
          WHEN numlanes BETWEEN 4 AND 5 AND speed_mph >= 35 THEN 10             -- Row 10
          WHEN numlanes >= 6 AND speed_mph >= 35 THEN 11                        -- Row 11
          ELSE 0
        END
    END as row_index,
    -- Second, determine column index based on bike facility
    -- bikeFac_index = [0, 5, 1, 2, 3, 4, 6, 9]
    CASE
      WHEN "bike_fac~1" = 0 THEN 0
      WHEN "bike_fac~1" = 5 THEN 1
      WHEN "bike_fac~1" = 1 THEN 2
      WHEN "bike_fac~1" = 2 THEN 3
      WHEN "bike_fac~1" = 3 THEN 4
      WHEN "bike_fac~1" = 4 THEN 5
      WHEN "bike_fac~1" = 6 THEN 6
      WHEN "bike_fac~1" = 9 THEN 7
      ELSE 0
    END as col_index,
    geom
  FROM output.model_network_with_parsed_speed
),
lts_calc_final as (
SELECT
  gid,
  no,
  fromnodeno,
  tonodeno,
  typeno,
  length,
  CASE WHEN bike_facility = 0 THEN 'No facility'
    WHEN bike_facility = 1 THEN 'Sharrows'
    WHEN bike_facility = 2 THEN 'Bike Lane'
    WHEN bike_facility = 3 THEN 'Buffered Bike Lane'
    WHEN bike_facility = 4 THEN 'Multi-use Trail'
    WHEN bike_facility = 5 THEN 'Signed Bike Route'
    WHEN bike_facility = 6 THEN 'Protected Bike Lane'
    WHEN bike_facility = 9 THEN 'Opposite direction of one way street'
    ELSE NULL 
  END as bike_facility,
  slope,
  county_code,
  totnumlanes,
  vehiclespeed,
  -- Third, lookup LTS from StressLevels matrix
  -- StressLevels[row_index][col_index]
  CASE
    -- Row 0 (no lanes)
    WHEN row_index = 0 THEN -1
    -- Row 1
    WHEN row_index = 1 AND col_index = 0 THEN 1
    WHEN row_index = 1 AND col_index = 1 THEN 1
    WHEN row_index = 1 AND col_index = 2 THEN 1
    WHEN row_index = 1 AND col_index = 3 THEN 1
    WHEN row_index = 1 AND col_index = 4 THEN 1
    WHEN row_index = 1 AND col_index = 5 THEN 1
    WHEN row_index = 1 AND col_index = 6 THEN 1
    WHEN row_index = 1 AND col_index = 7 THEN -2
    -- Row 2
    WHEN row_index = 2 AND col_index = 0 THEN 2
    WHEN row_index = 2 AND col_index = 1 THEN 2
    WHEN row_index = 2 AND col_index = 2 THEN 2
    WHEN row_index = 2 AND col_index = 3 THEN 1
    WHEN row_index = 2 AND col_index = 4 THEN 1
    WHEN row_index = 2 AND col_index = 5 THEN 1
    WHEN row_index = 2 AND col_index = 6 THEN 1
    WHEN row_index = 2 AND col_index = 7 THEN -2
    -- Row 3
    WHEN row_index = 3 AND col_index = 0 THEN 2
    WHEN row_index = 3 AND col_index = 1 THEN 2
    WHEN row_index = 3 AND col_index = 2 THEN 2
    WHEN row_index = 3 AND col_index = 3 THEN 1
    WHEN row_index = 3 AND col_index = 4 THEN 1
    WHEN row_index = 3 AND col_index = 5 THEN 1
    WHEN row_index = 3 AND col_index = 6 THEN 1
    WHEN row_index = 3 AND col_index = 7 THEN -2
    -- Row 4
    WHEN row_index = 4 AND col_index = 0 THEN 3
    WHEN row_index = 4 AND col_index = 1 THEN 3
    WHEN row_index = 4 AND col_index = 2 THEN 3
    WHEN row_index = 4 AND col_index = 3 THEN 2
    WHEN row_index = 4 AND col_index = 4 THEN 2
    WHEN row_index = 4 AND col_index = 5 THEN 1
    WHEN row_index = 4 AND col_index = 6 THEN 1
    WHEN row_index = 4 AND col_index = 7 THEN -2
    -- Row 5
    WHEN row_index = 5 AND col_index = 0 THEN 3
    WHEN row_index = 5 AND col_index = 1 THEN 3
    WHEN row_index = 5 AND col_index = 2 THEN 3
    WHEN row_index = 5 AND col_index = 3 THEN 2
    WHEN row_index = 5 AND col_index = 4 THEN 2
    WHEN row_index = 5 AND col_index = 5 THEN 1
    WHEN row_index = 5 AND col_index = 6 THEN 1
    WHEN row_index = 5 AND col_index = 7 THEN -2
    -- Row 6
    WHEN row_index = 6 AND col_index = 0 THEN 4
    WHEN row_index = 6 AND col_index = 1 THEN 4
    WHEN row_index = 6 AND col_index = 2 THEN 4
    WHEN row_index = 6 AND col_index = 3 THEN 3
    WHEN row_index = 6 AND col_index = 4 THEN 2
    WHEN row_index = 6 AND col_index = 5 THEN 2
    WHEN row_index = 6 AND col_index = 6 THEN 1
    WHEN row_index = 6 AND col_index = 7 THEN -2
    -- Row 7
    WHEN row_index = 7 AND col_index = 0 THEN 4
    WHEN row_index = 7 AND col_index = 1 THEN 4
    WHEN row_index = 7 AND col_index = 2 THEN 4
    WHEN row_index = 7 AND col_index = 3 THEN 3
    WHEN row_index = 7 AND col_index = 4 THEN 2
    WHEN row_index = 7 AND col_index = 5 THEN 2
    WHEN row_index = 7 AND col_index = 6 THEN 1
    WHEN row_index = 7 AND col_index = 7 THEN -2
    -- Row 8
    WHEN row_index = 8 AND col_index = 0 THEN 4
    WHEN row_index = 8 AND col_index = 1 THEN 4
    WHEN row_index = 8 AND col_index = 2 THEN 4
    WHEN row_index = 8 AND col_index = 3 THEN 3
    WHEN row_index = 8 AND col_index = 4 THEN 2
    WHEN row_index = 8 AND col_index = 5 THEN 2
    WHEN row_index = 8 AND col_index = 6 THEN 1
    WHEN row_index = 8 AND col_index = 7 THEN -2
    -- Row 9
    WHEN row_index = 9 AND col_index = 0 THEN 4
    WHEN row_index = 9 AND col_index = 1 THEN 4
    WHEN row_index = 9 AND col_index = 2 THEN 4
    WHEN row_index = 9 AND col_index = 3 THEN 3
    WHEN row_index = 9 AND col_index = 4 THEN 3
    WHEN row_index = 9 AND col_index = 5 THEN 2
    WHEN row_index = 9 AND col_index = 6 THEN 1
    WHEN row_index = 9 AND col_index = 7 THEN -2
    -- Row 10
    WHEN row_index = 10 AND col_index = 0 THEN 4
    WHEN row_index = 10 AND col_index = 1 THEN 4
    WHEN row_index = 10 AND col_index = 2 THEN 4
    WHEN row_index = 10 AND col_index = 3 THEN 3
    WHEN row_index = 10 AND col_index = 4 THEN 3
    WHEN row_index = 10 AND col_index = 5 THEN 2
    WHEN row_index = 10 AND col_index = 6 THEN 1
    WHEN row_index = 10 AND col_index = 7 THEN -2
    -- Row 11
    WHEN row_index = 11 AND col_index = 0 THEN 4
    WHEN row_index = 11 AND col_index = 1 THEN 4
    WHEN row_index = 11 AND col_index = 2 THEN 4
    WHEN row_index = 11 AND col_index = 3 THEN 4
    WHEN row_index = 11 AND col_index = 4 THEN 3
    WHEN row_index = 11 AND col_index = 5 THEN 3
    WHEN row_index = 11 AND col_index = 6 THEN 1
    WHEN row_index = 11 AND col_index = 7 THEN -2
    ELSE -1  -- Default for unmatched cases
  END as lts,
  geom
FROM lts_calc
WHERE NOT (typeno::int BETWEEN 0 AND 6 
    OR typeno::int BETWEEN 10 AND 19 
    OR typeno::int BETWEEN 80 AND 99))
SELECT
  *
FROM
  lts_calc_final
WHERE lts NOT IN (-1,-2);

-- Create spatial index
CREATE INDEX model_network_with_lts_geom_idx ON output.model_network_with_lts USING GIST (geom);

-- Create index on LTS for filtering
CREATE INDEX model_network_with_lts_lts_idx ON output.model_network_with_lts (lts);

-- Display summary statistics
SELECT 'LTS Calculation Summary (using exact old_lts.py methodology)' as summary;

SELECT
  lts,
  CASE
    WHEN lts = -2 THEN 'Opposite direction one-way'
    WHEN lts = -1 THEN 'No lanes / unmatched'
    WHEN lts = 1 THEN 'LTS 1 (Low stress)'
    WHEN lts = 2 THEN 'LTS 2 (Low-moderate stress)'
    WHEN lts = 3 THEN 'LTS 3 (Moderate stress)'
    WHEN lts = 4 THEN 'LTS 4 (High stress)'
    ELSE 'Unknown'
  END as description,
  COUNT(*) as segment_count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as percentage,
  ROUND(AVG(speed_mph), 1) as avg_speed_mph,
  ROUND(AVG(numlanes), 1) as avg_lanes
FROM output.model_network_with_lts
GROUP BY lts
ORDER BY lts;

SELECT 'Speed Data Source' as source;

SELECT
  CASE
    WHEN posted_speed IS NOT NULL THEN 'posted_speed (Overture)'
    WHEN "vcur_prt~2" IS NOT NULL THEN 'vcur_prt~2 (Original)'
    ELSE 'No speed data'
  END as speed_source,
  COUNT(*) as count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as percentage
FROM output.model_network_with_lts
GROUP BY
  CASE
    WHEN posted_speed IS NOT NULL THEN 'posted_speed (Overture)'
    WHEN "vcur_prt~2" IS NOT NULL THEN 'vcur_prt~2 (Original)'
    ELSE 'No speed data'
  END
ORDER BY count DESC;
