-- Conflate Overture speed data to network

-- Create schema for conflation work
CREATE SCHEMA IF NOT EXISTS conflation;

-- Extract speed from Overture JSON and calculate bearing
DROP VIEW IF EXISTS conflation.overture_speeds CASCADE;
CREATE VIEW conflation.overture_speeds AS
WITH speed_extract AS (
  SELECT
    id,
    class,
    -- Extract speed limit value using regex (more robust than JSON parsing)
    -- Looking for pattern: 'value': <number>
    CASE
      WHEN speed_limits IS NOT NULL
        AND speed_limits != '[]'
        AND speed_limits ~ '''value'':\s*\d+' THEN
        (regexp_match(speed_limits, '''value'':\s*(\d+)'))[1]::numeric
      ELSE NULL
    END as max_speed_mph,
    geometry as geom
  FROM overture_roads
  WHERE speed_limits IS NOT NULL
    AND speed_limits != '[]'
    AND speed_limits ~ '''value'':\s*\d+'
)
SELECT
  id,
  class,
  max_speed_mph,
  degrees(ST_Azimuth(ST_StartPoint(geom), ST_EndPoint(geom))) as bearing,
  CASE
    WHEN degrees(ST_Azimuth(ST_StartPoint(geom), ST_EndPoint(geom))) >= 315
      OR degrees(ST_Azimuth(ST_StartPoint(geom), ST_EndPoint(geom))) <= 45 THEN 'N'
    WHEN degrees(ST_Azimuth(ST_StartPoint(geom), ST_EndPoint(geom))) >= 45
      AND degrees(ST_Azimuth(ST_StartPoint(geom), ST_EndPoint(geom))) <= 135 THEN 'E'
    WHEN degrees(ST_Azimuth(ST_StartPoint(geom), ST_EndPoint(geom))) >= 135
      AND degrees(ST_Azimuth(ST_StartPoint(geom), ST_EndPoint(geom))) <= 225 THEN 'S'
    WHEN degrees(ST_Azimuth(ST_StartPoint(geom), ST_EndPoint(geom))) >= 225
      AND degrees(ST_Azimuth(ST_StartPoint(geom), ST_EndPoint(geom))) <= 315 THEN 'W'
  END as dir,
  geom
FROM speed_extract
WHERE max_speed_mph IS NOT NULL;

-- Split Overture segments at connector points for better curve matching
DROP VIEW IF EXISTS conflation.overture_speeds_split CASCADE;
CREATE VIEW conflation.overture_speeds_split AS
WITH speed_extract AS (
  SELECT
    id,
    class,
    CASE
      WHEN speed_limits IS NOT NULL
        AND speed_limits != '[]'
        AND speed_limits ~ '''value'':\s*\d+' THEN
        (regexp_match(speed_limits, '''value'':\s*(\d+)'))[1]::numeric
      ELSE NULL
    END as max_speed_mph,
    connectors,
    geometry as geom
  FROM overture_roads
  WHERE speed_limits IS NOT NULL
    AND speed_limits != '[]'
    AND speed_limits ~ '''value'':\s*\d+'
    AND connectors IS NOT NULL
    AND connectors != '[]'
),
connector_positions AS (
  SELECT
    id,
    class,
    max_speed_mph,
    geom,
    -- Parse connector positions from the text array format
    -- Format: [{'connector_id': 'xxx', 'at': 0.0} ...]
    regexp_matches(connectors, '''at'':\s*([0-9.]+)', 'g') as position_match,
    row_number() OVER (PARTITION BY id ORDER BY connectors) as rn
  FROM speed_extract
),
positions_expanded AS (
  SELECT
    id,
    class,
    max_speed_mph,
    geom,
    position_match[1]::float as at_position,
    row_number() OVER (PARTITION BY id ORDER BY position_match[1]::float) as position_order
  FROM connector_positions
),
segments_to_split AS (
  SELECT
    p1.id,
    p1.class,
    p1.max_speed_mph,
    p1.geom,
    p1.at_position as start_pos,
    p2.at_position as end_pos,
    p1.position_order
  FROM positions_expanded p1
  LEFT JOIN positions_expanded p2
    ON p1.id = p2.id
    AND p2.position_order = p1.position_order + 1
  WHERE p2.at_position IS NOT NULL  -- Only keep pairs
)
SELECT
  id || '_' || position_order as segment_id,
  id as original_id,
  class,
  max_speed_mph,
  ST_LineSubstring(geom, start_pos, end_pos) as geom,
  degrees(ST_Azimuth(
    ST_StartPoint(ST_LineSubstring(geom, start_pos, end_pos)),
    ST_EndPoint(ST_LineSubstring(geom, start_pos, end_pos))
  )) as bearing,
  CASE
    WHEN degrees(ST_Azimuth(
      ST_StartPoint(ST_LineSubstring(geom, start_pos, end_pos)),
      ST_EndPoint(ST_LineSubstring(geom, start_pos, end_pos))
    )) >= 315
      OR degrees(ST_Azimuth(
        ST_StartPoint(ST_LineSubstring(geom, start_pos, end_pos)),
        ST_EndPoint(ST_LineSubstring(geom, start_pos, end_pos))
      )) <= 45 THEN 'N'
    WHEN degrees(ST_Azimuth(
      ST_StartPoint(ST_LineSubstring(geom, start_pos, end_pos)),
      ST_EndPoint(ST_LineSubstring(geom, start_pos, end_pos))
    )) >= 45
      AND degrees(ST_Azimuth(
        ST_StartPoint(ST_LineSubstring(geom, start_pos, end_pos)),
        ST_EndPoint(ST_LineSubstring(geom, start_pos, end_pos))
      )) <= 135 THEN 'E'
    WHEN degrees(ST_Azimuth(
      ST_StartPoint(ST_LineSubstring(geom, start_pos, end_pos)),
      ST_EndPoint(ST_LineSubstring(geom, start_pos, end_pos))
    )) >= 135
      AND degrees(ST_Azimuth(
        ST_StartPoint(ST_LineSubstring(geom, start_pos, end_pos)),
        ST_EndPoint(ST_LineSubstring(geom, start_pos, end_pos))
      )) <= 225 THEN 'S'
    WHEN degrees(ST_Azimuth(
      ST_StartPoint(ST_LineSubstring(geom, start_pos, end_pos)),
      ST_EndPoint(ST_LineSubstring(geom, start_pos, end_pos))
    )) >= 225
      AND degrees(ST_Azimuth(
        ST_StartPoint(ST_LineSubstring(geom, start_pos, end_pos)),
        ST_EndPoint(ST_LineSubstring(geom, start_pos, end_pos))
      )) <= 315 THEN 'W'
  END as dir
FROM segments_to_split
WHERE ST_Length(ST_LineSubstring(geom, start_pos, end_pos)) > 0;  -- Skip zero-length segments

-- Combine split segments with non-split segments
DROP VIEW IF EXISTS conflation.overture_speeds_combined CASCADE;
CREATE VIEW conflation.overture_speeds_combined AS
-- Split segments (where connectors exist)
SELECT
  segment_id as id,
  original_id,
  class,
  max_speed_mph,
  bearing,
  dir,
  geom
FROM conflation.overture_speeds_split
UNION ALL
-- Original segments (where no connectors or single segment roads)
SELECT
  id,
  id as original_id,
  class,
  max_speed_mph,
  bearing,
  dir,
  geom
FROM conflation.overture_speeds
WHERE id NOT IN (SELECT DISTINCT original_id FROM conflation.overture_speeds_split);

-- Calculate bearing for model network segments
DROP VIEW IF EXISTS conflation.model_segs CASCADE;
CREATE VIEW conflation.model_segs AS
SELECT
  gid,
  no,
  degrees(ST_Azimuth(ST_StartPoint(geom), ST_EndPoint(geom))) as bearing,
  CASE
    WHEN degrees(ST_Azimuth(ST_StartPoint(geom), ST_EndPoint(geom))) >= 315
      OR degrees(ST_Azimuth(ST_StartPoint(geom), ST_EndPoint(geom))) <= 45 THEN 'N'
    WHEN degrees(ST_Azimuth(ST_StartPoint(geom), ST_EndPoint(geom))) >= 45
      AND degrees(ST_Azimuth(ST_StartPoint(geom), ST_EndPoint(geom))) <= 135 THEN 'E'
    WHEN degrees(ST_Azimuth(ST_StartPoint(geom), ST_EndPoint(geom))) >= 135
      AND degrees(ST_Azimuth(ST_StartPoint(geom), ST_EndPoint(geom))) <= 225 THEN 'S'
    WHEN degrees(ST_Azimuth(ST_StartPoint(geom), ST_EndPoint(geom))) >= 225
      AND degrees(ST_Azimuth(ST_StartPoint(geom), ST_EndPoint(geom))) <= 315 THEN 'W'
  END as dir,
  geom
FROM model_network;

-- Create points every 4 meters along Overture segments (using split segments)
DROP MATERIALIZED VIEW IF EXISTS conflation.overture_pts CASCADE;
CREATE MATERIALIZED VIEW conflation.overture_pts AS
SELECT
  row_number() OVER (ORDER BY id) as num,
  n,
  id,
  original_id,
  class,
  max_speed_mph,
  bearing,
  dir,
  ST_LineInterpolatePoint(
    ST_LineMerge((ST_Dump(geom)).geom),
    LEAST(n * (4 / ST_Length(ST_Transform(geom, 3857))), 1.0)
  )::GEOMETRY(Point, 4326) as geom
FROM conflation.overture_speeds_combined
CROSS JOIN generate_series(0, CEIL(ST_Length(ST_Transform(geom, 3857)) / 4)::int) as n;

CREATE INDEX idx_overture_pts ON conflation.overture_pts USING GIST (geom);

-- Match Overture points to model segments within 10 meters and check direction
DROP VIEW IF EXISTS conflation.overture_pt_to_model CASCADE;
CREATE VIEW conflation.overture_pt_to_model AS
WITH matches AS (
  SELECT
    a.id as overture_id,
    a.original_id as overture_original_id,
    a.num,
    a.class,
    a.max_speed_mph,
    a.dir as odir,
    a.bearing as obearing,
    b.gid as model_gid,
    b.dir as bdir,
    b.bearing as bbearing,
    ST_Distance(ST_Transform(a.geom, 3857), ST_Transform(b.geom, 3857)) as dist_meters,
    a.geom
  FROM conflation.overture_pts a,
       conflation.model_segs b
  WHERE ST_DWithin(a.geom, b.geom, 0.0001)  -- roughly 10 meters in degrees at this latitude
  ORDER BY a.id, ST_Distance(a.geom, b.geom)
)
SELECT
  *,
  -- Match bearing +/- 20 degrees in EITHER direction (same or opposite)
  -- This handles model network having paired segments (one for each direction)
  -- while Overture has single centerline segments
  CASE
    -- Same direction: within 20 degrees
    WHEN ABS(bbearing - obearing) <= 20
      OR ABS(bbearing - obearing) >= 340 THEN 1
    -- Opposite direction: within 20 degrees of opposite bearing (obearing + 180)
    WHEN ABS(bbearing - MOD((obearing + 180)::numeric, 360)) <= 20
      OR ABS(bbearing - MOD((obearing + 180)::numeric, 360)) >= 340 THEN 1
    ELSE 0
  END as match
FROM matches;

-- Count Overture points that match each model segment
DROP VIEW IF EXISTS conflation.point_count CASCADE;
CREATE VIEW conflation.point_count AS
SELECT
  model_gid,
  overture_id,
  class,
  max_speed_mph,
  COUNT(*) as pnt_count,
  odir,
  obearing,
  bdir,
  bbearing,
  match
FROM conflation.overture_pt_to_model
WHERE match > 0
GROUP BY model_gid, overture_id, class, max_speed_mph,
         odir, obearing, bdir, bbearing, match;

-- Count total Overture points per segment
DROP VIEW IF EXISTS conflation.total_point_count CASCADE;
CREATE VIEW conflation.total_point_count AS
SELECT
  overture_id,
  COUNT(*) as total_point_count
FROM conflation.overture_pt_to_model
WHERE match > 0
GROUP BY overture_id;

-- Find best matching Overture segment for each model segment
DROP VIEW IF EXISTS conflation.most_occurring CASCADE;
CREATE VIEW conflation.most_occurring AS
SELECT DISTINCT ON (a.model_gid)
  a.model_gid,
  a.pnt_count,
  b.total_point_count,
  ROUND(((a.pnt_count::numeric / b.total_point_count::numeric) * 100), 0) as pct_match,
  a.overture_id,
  a.class as overture_class,
  a.max_speed_mph,
  a.bdir,
  a.bbearing,
  a.odir,
  a.obearing,
  a.match
FROM conflation.point_count a
LEFT JOIN conflation.total_point_count b ON (a.overture_id = b.overture_id)
ORDER BY a.model_gid, a.pnt_count DESC;

-- Final output - model network with speed data
DROP TABLE IF EXISTS conflation.model_with_speed CASCADE;
CREATE TABLE conflation.model_with_speed AS
SELECT DISTINCT ON (a.gid)
  a.gid,
  a.no,
  a.geom,
  b.overture_id,
  b.overture_class,
  b.max_speed_mph,
  b.pnt_count,
  b.total_point_count,
  b.pct_match,
  ROUND((ST_Length(ST_Transform(a.geom, 3857)) / 4)::numeric, 0) as total_possible_pnts,
  ROUND(((b.total_point_count / (ST_Length(ST_Transform(a.geom, 3857)) / 4)) * 100)::numeric, 0) as possible_coverage,
  b.bdir,
  b.bbearing,
  b.odir,
  b.obearing,
  b.match
FROM model_network a
LEFT JOIN conflation.most_occurring b ON a.gid = b.model_gid;

CREATE SCHEMA IF NOT EXISTS output;

-- Final production table - all model network fields + quality filtered speed data
DROP TABLE IF EXISTS output.model_network_with_speed CASCADE;
CREATE TABLE output.model_network_with_speed AS
SELECT
  a.*,
  b.max_speed_mph as posted_speed
FROM model_network a
LEFT JOIN conflation.model_with_speed b
  ON a.gid = b.gid
  AND b.pct_match >= 25;  -- Only include quality matches (25%+ point match)

-- Create spatial index
CREATE INDEX model_network_with_speed_geom_idx ON output.model_network_with_speed USING GIST (geom);
