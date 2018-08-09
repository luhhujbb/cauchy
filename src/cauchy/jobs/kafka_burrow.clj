(ns cauchy.jobs.kafka-burrow
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clj-http.client :as http]))

(defn mark-as-whitelisted
    [consumers whitelist]
    (loop [patterns (map re-pattern whitelist)
           consumers-whitelisted consumers]
           (if-let [pattern (first patterns)]
                (recur (rest patterns)
                        (map
                            (fn [x] (if-not (nil? (re-find pattern (:consumer x)))
                                        (assoc x :whitelisted true)
                                        x))
                            consumers-whitelisted))
                consumers-whitelisted)))

(defn remove-blacklisted
    [consumers blacklist]
    (let [consumers-blacklisted (loop [patterns (map re-pattern blacklist)
                                       consumers-blacklisted consumers]
                                    (if-let [pattern (first patterns)]
                                        (recur (rest patterns)
                                            (remove
                                                (fn [x]
                                                    (not (or (nil? (re-find pattern (:consumer x)))
                                                             (:whitelisted x))))
                                                consumers-blacklisted))
                                        consumers-blacklisted))]
        (map (fn [x] (:consumer x)) consumers-blacklisted)))

(defn fetch-cluster-consumer [burrow cluster blacklist whitelist]
    ;;retrieve consumer list
    (let [response (http/get (str "http://" burrow "/v3/kafka/" cluster "/consumer") {:as :json :throw-exceptions false})
          consumers (get-in response [:body :consumers] [])]
          (remove-blacklisted
              (mark-as-whitelisted
                  (map (fn [x] {:whitelisted false :consumer x}) consumers)
                  whitelist)
              blacklist)))

(defn fetch-consumer-lags [burrow cluster consumer]
    (let [response (http/get (str "http://" burrow "/v3/kafka/" cluster "/consumer/" consumer "/lag") {:as :json :throw-exceptions false})
          {:keys [status body]} response
          content (if (= 404 status)
                    nil
                    body)]
         (if content
             (reduce
                 (fn [acc part]
                     (if-let [topic (get acc (:topic part))]
                        (assoc acc (:topic part) (conj topic part))
                        (assoc acc (:topic part) [part])))
                 {}
                 (get-in content [:status :partitions]))
             [])))

(defn extract-topic-aggregated-metrics [cluster consumer topic data]
    (let [base-metric (str topic "." consumer)]
    (try
    [(reduce
        (fn [acc x] (update acc :metric + (get-in x [:current_lag])))
        {:service (str base-metric ".totalLag") :metric 0} data)
        (reduce
        (fn [acc x] (update acc :metric + (get-in x [:end :offset])))
        {:service (str base-metric ".totalOffset") :metric 0} data)]
        (catch Exception e
            (log/error "[" cluster "][" topic "][" consumer"] error aggregating data")
            []))))

(defn extract-consumer-aggregated-metrics [cluster consumer data]
    (mapcat
        (fn [[topic v]]
            (extract-topic-aggregated-metrics cluster consumer topic v))
            data))

(defn extract-topic-partition-metrics [cluster consumer topic data]
    (let [base-metric (str topic "." consumer ".partition" )]
    (remove
        (fn [x] (nil? (:metric x)))
        (concat
            (map (fn [x] {:service (str base-metric "." (:partition x) ".lag") :metric (get-in x [:current_lag])}) data)
            (map (fn [x] {:service (str base-metric "." (:partition x) ".current-offset") :metric (get-in x [:end :offset])}) data)
            (map (fn [x] {:service (str base-metric "." (:partition x) ".timestamp") :metric (get-in x [:end :timestamp])}) data)))))

(defn extract-consumer-partition-metrics [cluster consumer data]
    (mapcat
        (fn [[topic v]]
            (extract-topic-partition-metrics cluster consumer topic v))
            data))

(defn extract-consumer-metrics [cluster consumer data]
    (concat
        (extract-consumer-aggregated-metrics cluster consumer data)
        (extract-consumer-partition-metrics cluster consumer data)))

(defn kafka-metrics [{:keys [burrow cluster blacklist whitelist]
                      :or {blacklist [] whitelist []}
                      :as conf}]
        (let [consumers (fetch-cluster-consumer burrow cluster blacklist whitelist)]
            (mapcat
                (fn [consumer]
                    (let [consumer-data (fetch-consumer-lags burrow cluster consumer)]
                        (extract-consumer-metrics cluster consumer consumer-data)))
                consumers)))
