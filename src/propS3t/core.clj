(ns propS3t.core
  (:require [clj-http.core :as http]
            [clj-http.client :as c]
            [clojure.xml :as xml]
            [clojure.set :as set])
  (:import (javax.crypto Mac)
           (javax.crypto.spec SecretKeySpec)
           (sun.misc BASE64Encoder)
           (java.util Date)
           (java.text SimpleDateFormat FieldPosition)
           (java.security MessageDigest DigestInputStream)
           (java.io ByteArrayInputStream)
           (org.apache.http.entity InputStreamEntity)))

(def bucket-ops-end-point "http://s3.amazonaws.com")

(def region-end-points
  {:us "http://%s.s3.amazonaws.com"})

(defn date-formater []
  (SimpleDateFormat. "EEE, d MMM yyyy HH:mm:ss Z"))

(def ^{:dynamic true} *date-formater* nil)

(defn date []
  (let [date (Date.)
        sb (StringBuffer.)]
    (.toString (.format (or *date-formater* (date-formater))
                        date sb (FieldPosition. 0)))))

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

(defn sign-request [{:keys [url request-method headers bucket region]
                     :as m}
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
                     (format (get region-end-points region) bucket)
                     bucket-ops-end-point)))))

(defn create-bucket [{:keys [aws-key aws-secret-key region]} bucket-name]
  (-> ((c/wrap-url http/request)
       (sign-request {:request-method :put
                      :url "/"
                      :region (or region :us)
                      :bucket bucket-name
                      :headers {"Date" (date)}}
                     aws-key
                     aws-secret-key))
      :body
      count
      zero?))

(defn list-buckets [{:keys [aws-key aws-secret-key]}]
  (for [[k v] (-> ((c/wrap-url http/request)
                   (sign-request {:request-method :get
                                  :url "/"
                                  :headers {"Date" (date)}}
                                 aws-key
                                 aws-secret-key))
                  :body
                  (ByteArrayInputStream.)
                  xml/parse)
        :when (= k :content)
        item v
        :when (= (:tag item) :Buckets)
        item (:content item)
        :let [bucket-atts (:content item)]]
    {:name (->> (filter #(= :Name (:tag %)) bucket-atts)
                first
                :content
                first)}))

(defn delete-bucket [{:keys [aws-key aws-secret-key region]} bucket-name]
  (-> ((c/wrap-url http/request)
       (sign-request {:request-method :delete
                      :url "/"
                      :region (or region :us)
                      :bucket bucket-name
                      :headers {"Date" (date)}}
                     aws-key
                     aws-secret-key))
      :body
      nil?))

(defn write-stream [{:keys [aws-key aws-secret-key region]} bucket-name
                    object-name stream & {:keys [md5sum length]}]
  (-> ((c/wrap-url http/request)
       (sign-request {:request-method :put
                      :url (str "/" object-name)
                      :region (or region :us)
                      :bucket bucket-name
                      :headers (merge {"Date" (date)}
                                      (when md5sum
                                        {"Content-MD5" md5sum}))
                      :body (InputStreamEntity. stream length)}
                     aws-key
                     aws-secret-key))
      :body
      count
      zero?))

(defn read-stream [{:keys [aws-key aws-secret-key region]} bucket-name
                   object-name & {:keys [length offset]}]
  (-> ((c/wrap-url http/request)
       (sign-request {:request-method :get
                      :url (str "/" object-name)
                      :region (or region :us)
                      :bucket bucket-name
                      :headers {"Date" (date)}}
                     aws-key
                     aws-secret-key))
      :body
      ;; https://github.com/dakrone/clj-http/issues/39
      ByteArrayInputStream.))

(defn extract-key-data [item]
  (set/rename-keys 
   (let [snag {:ETag (comp first :content)
               :Key (comp first :content)
               :LastModified (comp first :content)}]
     (reduce
      (fn [out element]
        (if (contains? snag (:tag element))
          (assoc out (:tag element)
                 ((get snag (:tag element)) element))
          out))
      {} (:content item)))
   {:ETag :etag
    :Key :key
    :LastModified :last-modified}))

(defn list-bucket
  ([creds bucket-name prefix length]
     (list-bucket creds bucket-name prefix length nil))
  ([{:keys [aws-key aws-secret-key region]} bucket-name prefix length start]
     (let [params (merge {"prefix" prefix
                          "max-keys" length}
                         (when start
                           {"marker" start}))]
       (for [[k v] (-> ((c/wrap-url (c/wrap-query-params http/request))
                        (sign-request {:request-method :get
                                       :url "/"
                                       :region (or region :us)
                                       :bucket bucket-name
                                       :headers {"Date" (date)}
                                       :query-params params}
                                      aws-key
                                      aws-secret-key))
                       :body
                       ByteArrayInputStream.
                       xml/parse)
             :when (= k :content)
             item v
             :when (= (:tag item) :Contents)]
         (extract-key-data item)))))
