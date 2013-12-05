(ns neo4j-clojure.core
  (:require [clojurewerkz.neocons.rest :as nr]
            [clojurewerkz.neocons.rest.cypher :as cy]))

(nr/connect! "http://localhost:7474/db/data/")

(defn relationships []
  (cy/tquery "MATCH (a)-[ACTED_IN] -> ()
              RETURN a.name, type(ACTED_IN);"))

(defn foo []
  (cy/tquery "MATCH (a) - [:ACTED_IN] -> (m) <- [:DIRECTED] - (d)
              RETURN a.name, m.title, d.name;") )

(defn paths []
  (cy/tquery "MATCH p=(a) - [:ACTED_IN] -> (m) <- [:DIRECTED] - (d)
              RETURN p;") )

(defn paths2 []
  (cy/tquery "MATCH p=(a) - [:ACTED_IN] -> (m) <- [:DIRECTED] - (d)
              RETURN nodes(p);") )

(defn aggregation []
  (cy/tquery "MATCH (a) - [:ACTED_IN] -> (m) <- [:DIRECTED] - (d)
              RETURN a.name, d.name, count(*)"))

(defn directors-who-acted-in-theirmovies []
  (cy/tquery "MATCH (a) - [:ACTED_IN] -> (m) <- [:DIRECTED] - (a)
              RETURN a.name, m.title"))

(defn unique-relationships-in-paths []
  (cy/tquery "MATCH (a) - [:ACTED_IN] -> (m) <- [:ACTED_IN] - (a)
              RETURN a.name, m.title"))

(defn sort-and-limit []
  (cy/tquery "MATCH (a)-[:ACTED_IN]->(m)<-[:DIRECTED]-(d)
              RETURN a.name, d.name, count(*) AS count
              ORDER BY count DESC
              LIMIT 5"))

(defn aggregation-collect []
  (cy/tquery "MATCH (a) - [:ACTED_IN] -> (m) <- [:DIRECTED] - (d)
              RETURN a.name, d.name, collect(m.title)"))

(defn all []
  (cy/tquery "MATCH (n)
              RETURN n"))

(defn filter-by-name [name]
  (cy/tquery (format "MATCH (n)
              WHERE has(n.name) AND n.name = '%s'
              RETURN n" name)))

(defn node-with-label [name]
  (cy/tquery (format "MATCH (p:Person)
             WHERE p.name = '%s'
             RETURN tom" name)))

(defn create-index-on-person []
  (cy/tquery "CREATE INDEX ON :Person(name)"))

(defn create-index-on-movie []
  (cy/tquery "CREATE INDEX ON :Movie(title)"))

(defn query-from-labeled-node [name]
  (cy/tquery (format "MATCH (p:Person)-[:ACTED_IN] -> (movie:Movie)
              WHERE p.name='%s'
              RETURN movie.title" name)))

(defn query-distinct [name]
  (cy/tquery (format "MATCH (p:Person) - [:ACTED_IN] -> (movie:Movie),
                      (director:Person)-[:DIRECTED] -> (movie:Movie)
                      WHERE p.name='%s'
                      RETURN DISTINCT director.name" name)))

(defn both-acted [name othername]
  (cy/tquery (format "MATCH (p1:Person)-[:ACTED_IN] -> (movie),
                      (p2:Person)-[:ACTED_IN] -> (movie)
                      WHERE p1.name='%s' AND p2.name='%s'
                      RETURN movie.title" name othername)))

(defn acted-before-year [name year]
  (cy/tquery (format "MATCH (p1:Person)-[:ACTED_IN] -> (movie)
                      WHERE p1.name='%s' AND movie.released < %s
                      RETURN movie.title" name year)))
