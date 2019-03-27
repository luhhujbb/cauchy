(ns cauchy.jobs.ping
  (:require [clojure.string :as s])
  (:use [clojure.java.shell :only [sh]]))

(defn execute-ping
    [host]
    (let [raw-res (sh "/bin/ping" "-c" "5" host)]
        (when (= 0 (:exit raw-res))
         (last (s/split-lines (:out raw-res))))))

(defn parse-ping
    [res]
    (let [splitted-res (s/split res #" ")
          metric-names (s/split (get splitted-res 1) #"/")
          metric-values (map (fn [^String x] (Double/parseDouble x)) (s/split (get splitted-res 3) #"/"))]
          (zipmap metric-names metric-values)))


(defn ping
    ([{:keys [host]
       :or {host "8.8.8.8"}}]
      (vec
          (map
              (fn [[k v]] {:service k :metric v})
              (parse-ping (execute-ping host)))))
     ([] (ping {})))
