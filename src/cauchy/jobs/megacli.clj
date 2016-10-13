(ns cauchy.jobs.megacli
  (:require [clojure.java.shell :as shell]
            [clojure.string :as str]))

(defn ->byte-size
  [number unit]
  (condp = unit
    "KB" (Math/round (* 1000 number))
    "MB" (Math/round (* 10000000 number))
    "GB" (Math/round (* 1000 1000000 number))
    "TB" (Math/round (* 1000000 1000000 number))))

(defn line-matcher
  [line pattern idx]
  (get (re-matches pattern line) idx))

(defn adapter?
  [line]
  (line-matcher line #"Adapter #([0-9]+)" 1))

(defn virtual-volume-number?
  [line]
  (line-matcher line #"Number of Virtual Disks\: ([0-9]+)" 1))

(defn virtual-volume?
  [line]
  (line-matcher line #"Virtual Disk\: ([0-9]+).*" 1))

(defn virtual-volume-state?
  [line]
  (line-matcher line #"State\: (.*)" 1))

(defn virtual-volume-size?
  [line]
  (if-let [size (line-matcher line #"Size\:([0-9\.]+) (KB|MB|GB|TB)" 1)]
      (->byte-size (Double/parseDouble size) (line-matcher line #"Size\:([0-9\.]+) (KB|MB|GB|TB)" 2))
    nil))

(defn number-of-drive?
  [line]
  (line-matcher line #"Number Of Drives\:([0-9]+)" 1))

(defn physical-drive?
  [line]
  (line-matcher line #"PD\: ([0-9]+).*" 1))

(defn drive-state?
  [line]
  (line-matcher line #"Firmware state: ([A-Za-z]+)" 1))

(defn drive-size?
  [line]
  (if-let [size (line-matcher line #"Raw Size\: ([0-9\.]+) (KB|MB|GB|TB).*" 1)]
      (->byte-size (Double/parseDouble size) (line-matcher line #"Raw Size\: ([0-9\.]+) (KB|MB|GB|TB).*" 2))
    nil))

(defn parse-megacli-data
  [data]
  (if-not (nil? data)
    (let [sp-lines (str/split-lines data)]
            (loop [lines sp-lines
                   mega-state {}
                   mega-adapter nil
                   mega-vol nil
                   mega-drive nil]
            (if-let [line (first lines)]
              (cond
                (adapter? line) (let [adp (adapter? line)
                                      kadp (keyword (str "adapter_" adp))]

                                  (recur
                                    (rest lines)
                                    (assoc mega-state kadp {:virtual_volumes {}})
                                    kadp
                                    nil
                                    nil))
                (virtual-volume-number? line) (let [vvoln (virtual-volume-number? line)]
                                            (recur
                                              (rest lines)
                                              (assoc-in mega-state [mega-adapter :virtual_volume_nb] (Long/parseLong vvoln))
                                              mega-adapter
                                              mega-vol
                                              mega-drive))
                (virtual-volume? line) (let [vvol (virtual-volume? line)
                                             kvvol (keyword (str "virtual_volume_" vvol))]
                                            (recur
                                              (rest lines)
                                              (assoc-in mega-state [mega-adapter :virtual_volumes kvvol] {:drives {}})
                                              mega-adapter
                                              kvvol
                                              nil))
              (virtual-volume-state? line) (let [state (virtual-volume-state? line)]
                                          (recur
                                            (rest lines)
                                            (assoc-in mega-state [mega-adapter :virtual_volumes mega-vol :state] state)
                                            mega-adapter
                                            mega-vol
                                            mega-drive))
              (virtual-volume-size? line) (let [size (virtual-volume-size? line)]
                                          (recur
                                            (rest lines)
                                            (assoc-in mega-state [mega-adapter :virtual_volumes mega-vol :size] size)
                                            mega-adapter
                                            mega-vol
                                            mega-drive))
              (number-of-drive? line) (let [nb_drv (Long/parseLong  (number-of-drive? line))]
                                          (recur
                                            (rest lines)
                                            (assoc-in mega-state [mega-adapter :virtual_volumes mega-vol :drive_nb] nb_drv)
                                            mega-adapter
                                            mega-vol
                                            mega-drive))
              (physical-drive? line) (let [drive (physical-drive? line)
                                           kdrv (keyword (str "drive_" drive))]
                                          (recur
                                            (rest lines)
                                            (assoc-in mega-state [mega-adapter :virtual_volumes mega-vol :drives kdrv] {})
                                            mega-adapter
                                            mega-vol
                                            kdrv))
              (drive-state? line) (let [st (drive-state? line)]
                                          (recur
                                            (rest lines)
                                            (assoc-in mega-state [mega-adapter :virtual_volumes mega-vol :drives mega-drive :state] st)
                                            mega-adapter
                                            mega-vol
                                            mega-drive))
              (drive-size? line) (let [size (drive-size? line)]
                                          (recur
                                            (rest lines)
                                            (assoc-in mega-state [mega-adapter :virtual_volumes mega-vol :drives mega-drive :size] size)
                                            mega-adapter
                                            mega-vol
                                            mega-drive))
                :else (recur (rest lines) mega-state mega-adapter mega-vol mega-drive))
                mega-state)))
    nil))

(defn get-megacli-cmd
  []
  (let [res (shell/sh "which" "MegaCli")
        res* (if (= 1 (:exit res))
              (shell/sh "which" "megacli")
              res)
        path (if (= 0 (:exit res))
            (str/replace (:out res) #"\n" "")
            nil  )]
   path))

(defn get-megacli-data
  [sudo]
  (if-let [path (get-megacli-cmd)]
    (:out (if sudo
            (shell/sh "sudo" path "-LdPDinfo" "-aALL" "-NoLog")
            (shell/sh path "-LdPDinfo" "-aALL" "-NoLog")))
    nil))
