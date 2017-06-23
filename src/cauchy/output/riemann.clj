(ns cauchy.output.riemann
  (:require [riemann.client :as rc]
            [clojure.tools.logging :as log])
  (:import (java.io IOException)))

(def rc (atom nil))

(defn init!
  [conf]
  (let [conf (:riemann conf)]
      (reset! rc (rc/tcp-client conf))))

(defn send!
  [msg]
  (when-not (rc/connected? @rc)
    (try (rc/reconnect! @rc)
         (catch IOException e
           (rc/flush! @rc))))
  (try
    (rc/send-event @rc msg)
    (catch Exception e
      (log/error "[RIEMANN] connection error, can't send event" e))))

(defn close!
  []
  (rc/close! @rc)
  (reset! rc nil))
