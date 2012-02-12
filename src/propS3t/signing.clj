(ns propS3t.signing
  (:require [propS3t.urls :as u])
  (:import (javax.crypto Mac)
           (javax.crypto.spec SecretKeySpec)
           (sun.misc BASE64Encoder)))

(defn sign [secret-key string-to-sign]
  (let [key (SecretKeySpec. (.getBytes secret-key) "HmacSHA1")
        mac (Mac/getInstance "HmacSHA1")]
    (.init mac key)
    (.encode (BASE64Encoder.) (.doFinal mac (.getBytes string-to-sign)))))

(defn string-to-sign [verb md5 type date url]
  (format "%s\n%s\n%s\n%s\n%s"
          verb
          md5
          type
          date
          url))

(defn sign-request
  [{:keys [url request-method headers bucket region] :as m}
   aws-key aws-secret-key]
  (let [sts
        (string-to-sign (.toUpperCase (name request-method))
                        ""
                        ""
                        (get headers "Date")
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
