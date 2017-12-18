(ns cauchy.output.riemann
  (:require [riemann.client :as r]
            [clojure.tools.logging :as log])
  (:import (java.io IOException)))

(def rc (atom nil))

(def r-conf (atom nil))

(defn init!
  [conf]
    (reset! r-conf conf)
    (reset! rc (r/tcp-client conf)))

(defn send!
  [msg]
  (when-not (r/connected? @rc)
    (try (r/reconnect! @rc)
         (catch Exception e
           (log/error "[RIEMANN] fail to reconnect" e))))
    (when (r/connected? @rc)
      (r/send-event @rc msg)))

(defn close!
  []
  (r/close! @rc)
  (reset! rc nil))
