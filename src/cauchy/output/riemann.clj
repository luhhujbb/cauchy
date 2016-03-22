(ns cauchy.output.riemann
  (:require [riemann.client :as rc]))

(def rc (atom nil))

(defn init!
  [conf]
  (let [conf (:riemann conf)]
      (reset! rc (rc/tcp-client conf))))

(defn send!
  [msg]
  (rc/send-event @rc msg))
