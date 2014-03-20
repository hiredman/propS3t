(ns propS3t.log
  (:require [propS3t.core :as s3]
            [clojure.stacktrace :as s])
  (:import (java.util.concurrent ArrayBlockingQueue)))

(defn maintain-loop [logger]
  (try
    (Thread/sleep (* 1000 60))
    (let [data (.take logger)]
      (if (and (:created-at data)
               (> (- (System/currentTimeMillis) (:created-at data))
                  (* 1000 60)))
        (do
          (.put logger (assoc data
                         :current-stream nil
                         :created-at nil
                         :space nil))
          (let [bb (java.nio.ByteBuffer/allocate (:space data))]
            (dotimes [i (:space data)]
              (.put bb (byte (int \n))))
            (.write (:current-stream data) (.array bb)))
          (.close (:current-stream data)))
        (do
          (.put logger data))))
    (catch Throwable t
      (s/print-stack-trace t))))

(defn make-maintain [logger]
  (future
    (while true
      (maintain-loop logger))))

(defn make-prime [logger creds bucket prefix]
  (.put logger {:creds creds
                :bucket bucket
                :prefix prefix
                :current-stream nil
                :created-at nil
                :space nil}))

(defonce registry (atom {}))

(defn get-stream [data]
  (or (:current-stream data)
      (s3/output-stream (:creds data) (:bucket data)
                        (str (:prefix data) "/"
                             (System/currentTimeMillis)
                             "-"
                             (.replace (format "%4s" (rand-int 1000)) " " "0")
                             ".log")
                        :length (* 5 1024 1024))))

(defn log [name ^bytes message]
  (let [logger (:logger (force (get @registry name)))
        data (.take logger)]
    (try
      (let [data (loop [data data
                        from 0]
                   (let [stream (get-stream data)
                         created-at (or (:created-at data)
                                        (System/currentTimeMillis))
                         space (or (:space data) (* 5 1024 1024))]
                     (cond
                      (>= space (- (count message) from))
                      (do
                        (.write stream message from (- (count message) from))
                        (assoc data
                          :current-stream stream
                          :created-at created-at
                          :space (- space (count message))))
                      (pos? space)
                      (do
                        (.write stream message from space)
                        (.close stream)
                        (recur (assoc data
                                 :current-stream nil
                                 :created-at nil
                                 :space nil)
                               space))
                      :else
                      (do
                        (.close stream)
                        (recur (assoc data
                                 :current-stream nil
                                 :created-at nil
                                 :space nil)
                               from)))))]
        (.put logger data))
      (catch Throwable t
        (.put logger data)
        (throw t)))))

(defn make-logging-fn [name]
  (fn [^bytes message]
    (log name message)))

(defn logger [name creds bucket prefix]
  (swap! registry (fn [r]
                    (if (contains? r name)
                      r
                      (assoc r name
                             (delay
                              (let [logger (ArrayBlockingQueue. 1)]
                                {:logger logger
                                 :prime (make-prime logger creds bucket prefix)
                                 :maintain (make-maintain logger)}))))))
  (make-logging-fn name))
