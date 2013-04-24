(ns propS3t.signing
  (:require [propS3t.urls :as u])
  (:import (javax.crypto Mac)
           (javax.crypto.spec SecretKeySpec)
           (sun.misc BASE64Encoder)))

(defn sign [^String secret-key ^String string-to-sign]
  (let [key (SecretKeySpec. (.getBytes secret-key) "HmacSHA1")
        mac (Mac/getInstance "HmacSHA1")]
    (.init mac key)
    (.encode (BASE64Encoder.) (.doFinal mac (.getBytes string-to-sign)))))

(defn string-to-sign [& args]
  (apply str (interpose \newline (remove nil? args))))

(defn sign-request
  [{:keys [url request-method headers bucket region] :as m}
   aws-key aws-secret-key]
  (let [headers (into {} (for [[k v] headers] [(.toLowerCase (name k)) v]))
        sts
        (string-to-sign (.toUpperCase (name request-method))
                        (if (contains? headers "content-md5")
                          (get headers "content-md5")
                          "")
                        (if (contains? headers "content-type")
                          (get headers "content-type")
                          "")
                        (get headers "date")
                        (let [s (apply str
                                       (interpose \newline
                                                  (for [[n v] headers
                                                        :let [header-name (.toLowerCase (name n))]
                                                        :when (not= header-name "date")
                                                        :when (not= header-name "range")
                                                        :when (not= header-name "content-type")]
                                                    (str header-name ":" v))))]
                          (when-not (empty? s)
                            s))
                        (if-not bucket
                          url
                          (str "/" bucket url)))]
    (-> m
        (update-in [:headers]
                   assoc "Authorization" (format "AWS %s:%s" aws-key
                                                 (sign aws-secret-key sts)))
        (update-in [:url] (fn [path-part host]
                            (str host path-part))
                   (if bucket
                     (format (get u/region-end-points region) bucket)
                     u/bucket-ops-end-point)))))

(defn wrap-aws-signature [client]
  (fn [req aws-key aws-secret-key]
    (client (sign-request req aws-key aws-secret-key))))
