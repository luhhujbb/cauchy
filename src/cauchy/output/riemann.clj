(ns cauchy.output.riemann
  (:require [riemann.client :as rc])
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
  (rc/send-event @rc msg))

(defn close!
  [msg]
  (rc/close! @rc)
  (reset! rc nil))
