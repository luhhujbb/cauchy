(ns cauchy.jobs.vostok-pop
    (:require [clj-http.client :as http]
              [clojure.tools.logging :as log]
      ))

(def state (atom {:state "ok" :warn 0 :crit 0 :ok 0}))

(defn get-state-with-threshold
    "threshold comparator"
    [{:keys [warn crit comp] :as conf} metric]
    (cond
         (comp metric crit) "critical"
         (comp metric warn) "warning"
         :else "ok"))

(def default-delay-threshold
    { "tweet-powertrack-delay" { :warn 900000 :crit 1800000 :comp >}})

(defn get-tweet-delay
   "retrieve url status"
    [{:keys [protocol host port] :or {protocol "http" host "localhost" port "8142" }}]
    (let [url (str protocol "://" host ":" port "/tweet-avg-delay")
          resp (http/get url {:as :json :throw-exceptions false})]
          (if (= 200 (:status resp))
            (get (:body resp) :tweet-delay -1)
            -1)))

(defn doc-delay
   "Main vostok-pop checker"
   ([{:keys [protocol host port thresholds] :as conf}]
      (let [thresholds (merge-with merge default-delay-threshold thresholds)
            delay (get-tweet-delay conf)
            sstate (if-not (= -1 delay)
                    (get-state-with-threshold (get thresholds "tweet-powertrack-delay") delay)
                    "critical")]
               [{:service "tweet-delay"
                 :state sstate
                 :metric delay}]))
    ([] (delay{})))
