(ns cauchy.jobs.http
    (:require [clj-http.client :as http]))

(def state (atom {:state "ok" :warn 0 :crit 0 :ok 0}))

(defn get-state-with-threshold
  "threshold comparator"
  [{:keys [warn crit comp] :as conf} metric]
  (cond
   (comp metric crit) "critical"
   (comp metric warn) "warning"
   :else "ok"))

(defn get-metric-with-threshold
 "threshold comparator"
 [{:keys [warn crit comp] :as conf} metric]
 (cond
  (comp metric crit) -1
  (comp metric warn) 0
  :else 1))


(def default-code-threshold
  { "status-code" { :warn 400 :crit 500 :comp >}
    "request-time" {:warn 1000 :crit 4000 :comp >}})

(defn get-http-data
 "retrieve url status"
  [{:keys [protocol host port path] :or {protocol "http" host "localhost" port "80" path "/"}}]
  (let [url (str protocol "://" host ":" port path)]
    (http/get url {:throw-exceptions false})))

(defn http-health
 "Main http checker"
 ([{:keys [protocol host port path thresholds] :as conf}]
  (let [thresholds (merge-with merge default-code-threshold thresholds)
        resp (get-http-data conf)
        sstate (get-state-with-threshold (get thresholds "status-code") (:status resp))
        lstate (get-state-with-threshold (get thresholds "request-time") (:request-time resp))
        smetric (get-metric-with-threshold (get thresholds "status-code") (:status resp))]
       [{:service "status"
         :state sstate
         :metric smetric}
        {:service "request-time"
         :state lstate
         :metric (:request-time resp)}]))
  ([] (http-health{})))
