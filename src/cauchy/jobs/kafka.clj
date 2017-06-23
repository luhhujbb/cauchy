(ns cauchy.jobs.kafka
  (:use [clojure.java.shell :only [sh]])
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]))

(defn exec-kafka-consumer-stats
  [group]
  (:out (sh "bash" "-c" (format "/rtgi/ext/kafka/bin/kafka-consumer-groups.sh --new-consumer --bootstrap-server localhost:9092 --group %s --describe|tr -s ' '|cut -d ' ' -f 6,7,3|tail -n +2|sed 's/ /,/g'" group))))

(defn exec-kafka-offset-checker
  [topic group]
  (:out (sh "bash" "-c" (format "/rtgi/ext/kafka/bin/kafka-consumer-offset-checker.sh --topic %s --group %s --zookeeper zookeeper-a.prod.aws.rtgi.eu,zookeeper-b.prod.aws.rtgi.eu,zookeeper-c.prod.aws.rtgi.eu:2181/kafka-10|tr -s ' '|cut -d ' ' -f 6,7,3|tail -n +2|sed 's/ /,/g'" topic group))))

(defn keywordize-consumer-stats
  [parsed-stats]
  (map #(zipmap [:id :lag :owner] %) parsed-stats))

(defn parse-consumer-stats
  [stats]
  (keywordize-consumer-stats
   (map #(str/split % #",") (str/split-lines stats))))

(defn get-consumer-lags
  ([group]
   (parse-consumer-stats (exec-kafka-consumer-stats group)))
  ([topic group]
   (parse-consumer-stats (exec-kafka-offset-checker topic group))))

(defn kafka-consumer-lags
  [{:keys [group] :as conf}]
  (try
    (vec (map #(let [m %]
                 {:service (str group "." (:id m) ".lag")
                  :metric (read-string (:lag m))})
              (get-consumer-lags group)))
    (catch Exception e
      (log/error "unexpected exception happened in job kafka-consumer-lags (" group ") " e))))

;; Offset-checker is deprecated and we should use kafka-consumer-groups
;; with the new-consumer api. However to get the lags of a
;; disconnected consumer group we have to use offset-checker.
(defn kafka-consumer-offset-checker
  [{:keys [topic group] :as conf}]
  (try
    (vec (map #(let [m %]
                 {:service (str topic "." group "." (:id m) ".lag")
                  :metric (read-string (:lag m))})
              (get-consumer-lags topic group)))
    (catch Exception e
      (log/error "unexpected exception happened in job kafka-consumer-offset-checker (" group ") " e))))
