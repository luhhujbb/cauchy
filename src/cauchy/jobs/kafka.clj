(ns cauchy.jobs.kafka
  (:use [clojure.java.shell :only [sh]])
  (:require [clojure.string :as str]))

(defn exec-kafka-consumer-stats
  [group]
  (:out (sh "bash" "-c" "/rtgi/ext/kafka/bin/kafka-consumer-groups.sh --new-consumer --bootstrap-server kafka-10-a.prod.aws.rtgi.eu:9092 --group group --describe|tr -s ' '|cut -d ' ' -f 6,7,3|tail -n +2|sed 's/ /,/g'")))

(defn parse-consumer-stats
  [stats]
  (keywordize-consumer-stats
   (map #(str/split % #",") (str/split-lines stats))))

(defn keywordize-consumer-stats
  [parsed-stats]
  (map #(zipmap [:id :lag :owner] %) parsed-stats))

(defn get-consumer-lags
  [group]
  (parse-consumer-stats (exec-kafka-consumer-stats group)))
