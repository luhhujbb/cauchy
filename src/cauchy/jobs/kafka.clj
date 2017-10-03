(ns cauchy.jobs.kafka
  (:use [clojure.java.shell :only [sh]])
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]))

(defn exec-kafka-consumer-stats
  [{:keys [bootstrap-server]
    :or {bootstrap-server "kafka-10-a.prod.aws.rtgi.eu:9092"}
    :as conf}
   group]
  (:out (sh "bash" "-c" (format (str "/rtgi/ext/kafka/bin/kafka-consumer-groups.sh --new-consumer --bootstrap-server " bootstrap-server " --group %s --describe|tr -s ' '|cut -d ' ' -f 6,7,3|tail -n +2|sed 's/ /,/g'") group))))

(defn exec-kafka-offset-checker
  [{:keys [zookeeper]
    :or {zookeeper "zookeeper-a.prod.aws.rtgi.eu,zookeeper-b.prod.aws.rtgi.eu,zookeeper-c.prod.aws.rtgi.eu:2181/kafka-10"}
    :as conf}
  topic
  group]
  (:out (sh "bash" "-c" (format (str "/rtgi/ext/kafka/bin/kafka-consumer-offset-checker.sh --topic %s --group %s --zookeeper " zookeeper "|tr -s ' '|cut -d ' ' -f 6,7,3|tail -n +2|sed 's/ /,/g'") topic group))))

(defn keywordize-consumer-stats
  [parsed-stats]
  (map #(zipmap [:id :lag :owner] %) parsed-stats))

(defn parse-consumer-stats
  [stats]
  (keywordize-consumer-stats
   (map #(str/split % #",") (str/split-lines stats))))

(defn get-consumer-lags
  ([conf group]
   (parse-consumer-stats (exec-kafka-consumer-stats conf group)))
  ([conf topic group]
   (parse-consumer-stats (exec-kafka-offset-checker conf topic group))))

(defn kafka-consumer-lags
  [{:keys [group] :as conf}]
  (try
    (vec (map #(let [ id (:id %)
                      lag (if (= "unknown" (:lag %))
                          -1
                          (read-string (:lag %)))
                     ]
                 {:service (str group "." id ".lag")
                  :metric lag})
              (get-consumer-lags conf group)))
    (catch Exception e
      (log/error "unexpected exception happened in job kafka-consumer-lags (" group ") " e))))

;; Offset-checker is deprecated and we should use kafka-consumer-groups
;; with the new-consumer api. However to get the lags of a
;; disconnected consumer group we have to use offset-checker.
(defn kafka-consumer-offset-checker
  [{:keys [topic group] :as conf}]
  (try
    (vec (map #(let [ id (:id %)
                      lag (if (= "unknown" (:lag %))
                          -1
                          (read-string (:lag %)))
                     ]
                 {:service (str topic "." group "." id ".lag")
                  :metric lag})
              (get-consumer-lags conf topic group)))
    (catch Exception e
      (log/error "unexpected exception happened in job kafka-consumer-offset-checker (" group ") " e))))
