/* Baseline query. Optimised version below.
 */
WITH attrs AS (
    SELECT ogc_fid pk,
           identificatie
    FROM public.building b2),
     extent AS (
         SELECT st_union(geom) geom
         FROM tile_index.tile_index_sub tis
         WHERE id IN ('ic1')),
     sub AS (
         SELECT a.*
         FROM public.building a,
              extent t
         WHERE st_intersects(t.geom,
                             a.wkb_geometry)),
     polygons AS (
         SELECT ogc_fid       pk,
                (ST_Dump(wkb_geometry)).geom,
                identificatie coid
         FROM sub),
     boundary AS (
         SELECT pk,
                ARRAY_AGG(ST_ASTEXT(geom)) geom,
                coid
         FROM polygons
         GROUP BY pk,
                  coid)
SELECT b.pk,
       b.geom,
       b.coid,
       a.*
FROM boundary b
         INNER JOIN attrs a ON
    b.pk = a.pk;


/* The goal is to parse the PostGIS geometry representation into 
 * a CityJSON-like geometry array representation. Here I use 
 * several subqueries for sequentially aggregating the vertices, 
 * rings and surfaces. I also tested the aggregation with window
 * function calls, but this approach tends to be at least twice 
 * as expensive then the subquery-aggregation. 
 * In the expand_point subquery, the first vertex is skipped, 
 * because PostGIS uses Simple Features so the first vertex is 
 * duplicated at the end.
 */
WITH sub AS (
    SELECT *
    FROM public.building b2
    LIMIT 1),
     polygons AS (
         SELECT ogc_fid                     pk,
                ST_DumpPoints(wkb_geometry) geom,
                identificatie               coid
         FROM sub b),
     expand_points AS (
         SELECT pk,
                (geom).PATH[1]                                                 exterior,
                (geom).PATH[2]                                                 interior,
                (geom).PATH[3]                                                 vertex,
                ARRAY [ST_X((geom).geom), ST_Y((geom).geom),ST_Z((geom).geom)] point
         FROM polygons
         WHERE (geom).PATH[3] > 1
               -- skip the first vertex because the first and last are the same
         ORDER BY pk,
                  exterior,
                  interior,
                  vertex),
     rings AS (
         SELECT pk,
                exterior,
                interior,
                ARRAY_AGG(point) geom
         FROM expand_points
         GROUP BY interior,
                  exterior,
                  pk
         ORDER BY exterior,
                  interior),
     surfaces AS (
         SELECT pk,
                ARRAY_AGG(geom) geom
         FROM rings
         GROUP BY exterior,
                  pk
         ORDER BY exterior),
     multisurfaces AS (
         SELECT pk,
                ARRAY_AGG(geom) geom
         FROM surfaces
         GROUP BY pk)
SELECT *
FROM multisurfaces;

/* Putting it together with the attribute and tile selection.
*/
WITH extent AS (
         SELECT st_union(geom) geom
         FROM tile_index.tile_index_sub tis
         WHERE id IN ('ic1')),
     attr_in_extent AS (
         SELECT ogc_fid pk,
                identificatie
         FROM public.building a,
              extent t
         WHERE ST_Intersects(t.geom,
                             a.wkb_geometry)),
     geom_in_extent AS (
         SELECT a.*
         FROM public.building a,
              extent t
         WHERE ST_Intersects(t.geom,
                             a.wkb_geometry)),
     polygons AS (
         SELECT ogc_fid                     pk,
                ST_DumpPoints(wkb_geometry) geom
         FROM geom_in_extent b),
     expand_points AS (
         SELECT pk,
                (geom).PATH[1]         exterior,
                (geom).PATH[2]         interior,
                (geom).PATH[3]         vertex,
                ARRAY [ST_X((geom).geom),
                    ST_Y((geom).geom),
                    ST_Z((geom).geom)] point
         FROM polygons
         WHERE (geom).PATH[3] > 1
         ORDER BY pk,
                  exterior,
                  interior,
                  vertex),
     rings AS (
         SELECT pk,
                exterior,
                interior,
                ARRAY_AGG(point) geom
         FROM expand_points
         GROUP BY interior,
                  exterior,
                  pk
         ORDER BY exterior,
                  interior),
     surfaces AS (
         SELECT pk,
                ARRAY_AGG(geom) geom
         FROM rings
         GROUP BY exterior,
                  pk
         ORDER BY exterior),
     multisurfaces AS (
         SELECT pk,
                ARRAY_AGG(geom) geom
         FROM surfaces
         GROUP BY pk)
SELECT b.pk,
       b.geom,
       a.*
FROM multisurfaces b
         INNER JOIN attr_in_extent a ON
    a.pk = b.pk;

