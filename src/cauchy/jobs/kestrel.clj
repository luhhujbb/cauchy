(ns cauchy.jobs.kestrel
  (:require [clj-http.client :as http]))

(defn threshold
  [{:keys [warn crit comp] :as conf} metric]
  (cond
   (comp metric crit) "critical"
   (comp metric warn) "warning"
   :else "ok"))

(defn fetch-stats
  [{:keys [host port period] :or {host "localhost" port 3334 period 3600}}]
  (let [url (str "http://" host ":" port "/stats")]
    (:body (http/get url {:as :json}))))

(defn fetch-stats-period
  [{:keys [host port period] :or {host "localhost" port 3334 period 3600}}]
  (let [url (str "http://" host ":" port "/stats?period=" period)]
    (:body (http/get url {:as :json}))))

(defn all-queues
  [stats]
  (->> stats
      (:gauges)
      (keys)
      (map (fn [^clojure.lang.Keyword kw]
             (let [n (.toString kw)]
               (re-find #"q\/([a-zA-Z.+-_]+)\/" n))))
      (map second)
      (into #{})
      (remove nil?)))

(defn mk-key
  [queue metric]
  (keyword (str "q/" queue "/" metric)))

(defn get-queue-info
  [stats stats-p queue period]
  (let [items (get-in stats [:gauges (mk-key queue "items")])
        age (get-in stats [:gauges (mk-key queue "age_msec")])
        ;; default to 0 when there is no stats-period
        ;; because queue has just been created
        put (get-in stats-p [:counters (mk-key queue "put_items")] 0)
        get (get-in stats-p [:counters (mk-key queue "get_items_hit")] 0)
        put-rate (double (/ put period))
        get-rate (double (/ get period)) ]
    {"items" items "age" age
     "put_rate" put-rate "get_rate" get-rate}))

(def default-thresholds
  {"age" {:warn 300000 :crit 900000 :comp >}
   "items" {:warn 10000 :crit 100000 :comp >}
   "put_rate" {:warn 0.5 :crit 0.01 :comp <}
   "get_rate" {:warn 0.5 :crit 0.01 :comp <}})

(defn kestrel-stats
  ([{:keys [thresholds host port period]
     :or {period 3600}
     :as conf}]
   (let [thresholds (merge-with merge default-thresholds thresholds)
         stats (fetch-stats conf)
         stats-p (fetch-stats-period conf)
         infos (->> (all-queues stats)
                    (map (fn [queue]
                           [queue
                            (get-queue-info stats stats-p queue period)]))
                    (into {}))]
     (for [[queue data] infos
           [sname value] data]
       {:service (str queue "." sname)
        :metric value
        :state (threshold (get thresholds sname) value)})))
  ([] (kestrel-stats {})))
