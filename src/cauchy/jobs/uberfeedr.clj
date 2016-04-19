(ns cauchy.jobs.uberfeedr
  (:require [clj-http.client :as http]))

(defn uberfeedr-fetch-metrics
  "Get uberfeedr metrics"
  [{:keys [host port] :or {host "localhost" port 8084}}]
    (let [url (str "http://" host ":" port "/count_feeds")]
        (:body (http/get url {:as :json}))))


(defn uberfeedr-metrics []
   (let [{:keys [:nb :nb_activated :nb_errors]} (uberfeedr-fetch-metrics {})]
       [{:service (str "nb_total") :state "ok" :metric nb}
        {:service (str "nb_error") :state "ok" :metric nb_errors}
        {:service (str "nb_activated") :state "ok" :metric nb_activated}]))
