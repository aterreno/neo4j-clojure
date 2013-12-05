(ns neo4j-clojure.core
  (:require [clojurewerkz.neocons.rest :as nr]
            [clojurewerkz.neocons.rest.cypher :as cy]))

(nr/connect! "http://localhost:7474/db/data/")

(defmacro run-query [query]
  `(cy/tquery ~query))

(defn relationships []
  (run-query "MATCH (a)-[ACTED_IN] -> ()
              RETURN a.name, type(ACTED_IN);"))

(defn foo []
  (run-query "MATCH (a) - [:ACTED_IN] -> (m) <- [:DIRECTED] - (d)
              RETURN a.name, m.title, d.name;") )

(defn paths []
  (run-query "MATCH p=(a) - [:ACTED_IN] -> (m) <- [:DIRECTED] - (d)
              RETURN p;") )

(defn paths2 []
  (run-query "MATCH p=(a) - [:ACTED_IN] -> (m) <- [:DIRECTED] - (d)
              RETURN nodes(p);") )

(defn aggregation []
  (run-query "MATCH (a) - [:ACTED_IN] -> (m) <- [:DIRECTED] - (d)
              RETURN a.name, d.name, count(*)"))

(defn directors-who-acted-in-theirmovies []
  (run-query "MATCH (a) - [:ACTED_IN] -> (m) <- [:DIRECTED] - (a)
              RETURN a.name, m.title"))

(defn unique-relationships-in-paths []
  (run-query "MATCH (a) - [:ACTED_IN] -> (m) <- [:ACTED_IN] - (a)
              RETURN a.name, m.title"))

(defn sort-and-limit []
  (run-query "MATCH (a)-[:ACTED_IN]->(m)<-[:DIRECTED]-(d)
              RETURN a.name, d.name, count(*) AS count
              ORDER BY count DESC
              LIMIT 5"))

(defn aggregation-collect []
  (run-query "MATCH (a) - [:ACTED_IN] -> (m) <- [:DIRECTED] - (d)
              RETURN a.name, d.name, collect(m.title)"))

(defn all []
  (run-query "MATCH (n)
              RETURN n"))

(defn filter-by-name [name]
  (run-query (format "MATCH (n)
              WHERE has(n.name) AND n.name = '%s'
              RETURN n" name)))

(defn node-with-label [name]
  (run-query (format "MATCH (p:Person)
             WHERE p.name = '%s'
             RETURN tom" name)))

(defn create-index-on-person []
  (run-query "CREATE INDEX ON :Person(name)"))

(defn create-index-on-movie []
  (run-query "CREATE INDEX ON :Movie(title)"))

(defn query-from-labeled-node [name]
  (run-query (format "MATCH (p:Person)-[:ACTED_IN] -> (movie:Movie)
              WHERE p.name='%s'
              RETURN movie.title" name)))

(defn query-distinct [name]
  (run-query (format "MATCH (p:Person) - [:ACTED_IN] -> (movie:Movie),
                      (director:Person)-[:DIRECTED] -> (movie:Movie)
                      WHERE p.name='%s'
                      RETURN DISTINCT director.name" name)))

(defn both-acted [name othername]
  (run-query (format "MATCH (p1:Person)-[:ACTED_IN] -> (movie),
                      (p2:Person)-[:ACTED_IN] -> (movie)
                      WHERE p1.name='%s' AND p2.name='%s'
                      RETURN movie.title" name othername)))

(defn acted-before-year [name year]
  (run-query (format "MATCH (p1:Person)-[:ACTED_IN] -> (movie)
                      WHERE p1.name='%s' AND movie.released < %s
                      RETURN movie.title" name year)))

(defn constraints-on-properties [name role]
  (run-query (format "MATCH (p1:Person)-[r:ACTED_IN] -> (movie)
                      WHERE p1.name='%s' AND '%s' IN (r.roles)
                      RETURN DISTINCT movie.title" name role)))

(defn younger-than-with-diff-sorted [name]
  (run-query (format "MATCH (p1:Person)-[r:ACTED_IN] -> (movie),
                      (a:Person)-[:ACTED_IN] -> (movie)
                      WHERE p1.name='%s'
                      AND a.born < p1.born
                      RETURN DISTINCT a.name, (p1.born - a.born) AS diff
                      ORDER BY diff DESC" name)))

(defn constraints-based-on-patterns [name]
  (run-query (format "MATCH (p1:Person)-[:ACTED_IN]->movie,
                      (n)-[:ACTED_IN]-(movie)
                      WHERE p1.name='%s'
                      AND (n)-[:DIRECTED]->()
                      RETURN DISTINCT n.name" name)))

(defn busiest-actors []
  (run-query "MATCH (p1:Person)-[:ACTED_IN]->movie
              RETURN p1.name, count(p1.name) AS count
              ORDER BY count DESC
              LIMIT 5"))


(defn should-work-with [name]
  (run-query (format "MATCH (p1:Person)-[:ACTED_IN]->(m1)<-[:ACTED_IN]-(c),
                      (c)-[:ACTED_IN]->(m2)<-[:ACTED_IN]-(coc)
                      WHERE p1.name = '%s'
                      AND NOT (p1)-[:ACTED_IN] -> () <- [:ACTED_IN]- (coc)
                      AND p1 <> coc
                      RETURN c, coc, p1, count(coc), m1, m2" name)))

(defn find-movies-by-actor
  [name]
  (run-query (format
              "MATCH (p:Person) - [:ACTED_IN] -> (movie)
               WHERE p.name='%s'
               RETURN DISTINCT movie" name)))

(defn create-movie-node [name year]
  (run-query (format "CREATE (m:Movie {title:'%s', released:%s})" name year)))

(defn set-tagline-to-movie
  [name tagline]
  (run-query (format
              "MATCH (movie:Movie)
               WHERE movie.title='%s'
               SET movie.tagline = '%s'
               RETURN movie" name tagline)))

(defn create-relationship
  [actor movie]
  (run-query (format "
               MATCH (movie:Movie),(person:Person)
               WHERE movie.title='%s'
               AND person.name='%s'
               CREATE UNIQUE (person)-[:ACTED_IN {roles:['Sean']}]->(movie)" movie actor)))

(defn update-relationship
  [actor]
  (run-query (format "
               MATCH (actor:Person)-[r:ACTED_IN]->(movie:Movie)
               WHERE actor.name = '%s'
               SET r.roles = [x in r.roles WHERE x <> 'Sean'] + 'Sean Divine'
               RETURN actor, r
               " actor)))

(defn match-or-create
  [name]
  (run-query (format "MERGE (p:Person {name:'%s'})
                      ON CREATE SET p.created = timestamp()
                      ON MATCH SET p.accessed = coalesce(p.accessed, 0) + 1
                      RETURN p.created, p.accessed" name)))
