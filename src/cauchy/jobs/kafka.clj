(ns cauchy.jobs.kafka
  (:use [clojure.java.shell :only [sh]])
  (:require [clojure.string :as str]
            [com.climate.claypoole :as cp]
            [clojure.tools.logging :as log]))

(defn list-consumer-groups
  [{:keys [bootstrap-server kafka-path]
    :or {bootstrap-server "localhost:9092"
         kafka-path "/rtgi/ext/kafka"}}]
   (let[cmd (format "%s/bin/kafka-consumer-groups.sh --list --bootstrap-server %s" kafka-path bootstrap-server)]
     (str/split 
      (:out (sh "bash" "-c" cmd)) 
     #"\n" )))
   

(defn exec-kafka-consumer-stats
  [{:keys [bootstrap-server kafka-path]
    :or {bootstrap-server "localhost:9092"
         kafka-path "/rtgi/ext/kafka"}
    :as conf}
   group]
   (let[cmd (format (str kafka-path "/bin/kafka-consumer-groups.sh --new-consumer --bootstrap-server " bootstrap-server " --group %s --describe|tr -s ' '|sed -r 's/ /,/g'") group)]
      (log/debug cmd)
      (:out (sh "bash" "-c" cmd))))

(defn exec-kafka-offset-checker
  [{:keys [zookeeper kafka-path]
    :or {zookeeper "zookeeper-a.prod.aws.rtgi.eu,zookeeper-b.prod.aws.rtgi.eu,zookeeper-c.prod.aws.rtgi.eu:2181/kafka-10"
         kafka-path "/rtgi/ext/kafka"}
    :as conf}
  topic
  group]
  (let[cmd (format (str kafka-path "/bin/kafka-consumer-offset-checker.sh --topic %s --group %s --zookeeper " zookeeper "|tr -s ' '|sed -r 's/ /,/g'") topic group)]
    (log/debug cmd)
    (:out (sh "bash" "-c" cmd))))

(defn keywordize-consumer-stats
  [parsed-stats]
  (map #(zipmap [:topic :id :curoffset :lag] %) parsed-stats))

(defn parse-consumer-stats
  [stats topic-header-name partition-header-name current-offset-header-name lag-header-name]
  (let[stats (map #(str/split % #",") (filter #(not= "" %) (str/split-lines stats)))

      headers (first stats)
      _ (log/debug "headers" headers)

      partition-position (.indexOf headers partition-header-name)
      topic-position (.indexOf headers topic-header-name)
      lag-position (.indexOf headers lag-header-name)
      offset-position (.indexOf headers current-offset-header-name)
      _ (log/debug "position" partition-header-name partition-position offset-position lag-header-name lag-position)]

    (keywordize-consumer-stats
      (map #(let[_ (log/debug "parse-consumer-stats select on this line :" %)]
            [(nth % topic-position) (nth % partition-position) (nth % offset-position) (nth % lag-position)]) (rest stats))) ))

(defn get-consumer-lags
  ([conf group]
   (parse-consumer-stats (exec-kafka-consumer-stats conf group) "TOPIC" "PARTITION" "CURRENT-OFFSET" "LAG"))
  ([conf topic group]
   (parse-consumer-stats (exec-kafka-offset-checker conf topic group) "Pid" "Lag")))

(defn kafka-consumer-lags
  [{:keys [group] :as conf}]
  (try
   (vec
    (apply concat (map #(let [ id (:id %)
                      topic (:topic %)
                      currentoffset (if (= "-" (:curoffset %) ) 
                                     0 
                                     (read-string (:curoffset %)))
                      lag (if (or (= "unknown" (:lag %)) (= "-" (:lag %)))
                          -1
                          (read-string (:lag %)))
                     ]
              (when-not (= "-" topic)
                 [{:service (str topic "." group "." id ".lag")
                  :metric lag}
                  {:service (str topic "." group "." id ".current-offset")
                  :metric currentoffset}]))
             (get-consumer-lags conf group))))
    (catch Exception e
      (log/error "unexpected exception happened in job kafka-consumer-lags (" group ") " e))))

(defn kafka-stats []
 (apply concat
  (cp/pmap (/ (.. Runtime getRuntime availableProcessors) 2)  (fn [group-id]
   (kafka-consumer-lags {:group group-id})
   )
  (list-consumer-groups {}))))

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
