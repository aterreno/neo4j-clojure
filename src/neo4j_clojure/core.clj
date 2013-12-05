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

(defn filter-by-name []
  (cy/tquery "MATCH (n)
              WHERE has(n.name) AND n.name = 'Tom Hanks'
              RETURN n"))

(defn node-with-label []
  (cy/query "MATCH (tom:Person)
             WHERE tom.name = 'Tom Hanks'
             RETURN tom"))
