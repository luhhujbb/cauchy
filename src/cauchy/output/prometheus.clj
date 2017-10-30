(ns cauchy.output.prometheus
  (:require [prometheus.core :as prometheus]
            [clojure.string :as str]
            [ring.server.standalone :refer [serve]]
            [clojure.tools.logging :as log])
  (:import (java.io IOException)))

;;Store the prometheus registry
(def store (atom nil))

;;Atom to store registered metrics (allow auto metrics registration)
(def metric-registry (atom {}))

(def timer (atom (System/currentTimeMillis)))

(def pg (atom "localhost:9091"))

(def namespace "cauchy")

(defn init! [{:keys [pushgateway] :or {pushgateway "localhost:9091"} :as conf}]
  (->> (prometheus/init-defaults)
       (reset! store))
  (reset! pg pushgateway))

(defn send! [msg]
  (let [metric-name (str/replace (:service msg) #"\." "_")
        labels-name (into []
                      (map name
                        (concat
                          (keys (:labels msg))
                          [:host :state])))
        labels-value (into []
                       (concat
                         (vals (:labels msg))
                         [(:host msg) (:state msg)])) ]
        (when-not (get @metric-registry metric-name false)
          (prometheus/register-histogram @store namespace metric-name "" labels-name)
          (swap! metric-registry metric-name true))
        (prometheus/track-observation @store namespace metric-name (:metric msg labels-value))
        (when (> (- (System/currentTimeMillis) @timer) 30000)
          (prometheus/push-metrics! @store "prometheus-cauchy-job")
          (reset! @timer (System/currentTimeMillis)))))
