(ns propS3t.core
  (:require [clj-http.core :as http]
            [clj-http.client :as c]
            [clojure.xml :as xml]
            [clojure.set :as set]
            [propS3t.support :as ps3])
  (:import (java.io ByteArrayInputStream)
           (org.apache.http.entity InputStreamEntity)))

(defn create-bucket [{:keys [aws-key aws-secret-key region]} bucket-name]
  (-> ((c/wrap-url http/request)
       (ps3/sign-request {:request-method :put
                          :url "/"
                          :region (or region :us)
                          :bucket bucket-name
                          :headers {"Date" (ps3/date)}}
                         aws-key
                         aws-secret-key))
      :body
      count
      zero?))

(defn list-buckets [{:keys [aws-key aws-secret-key]}]
  (let [{:keys [body]} ((c/wrap-url http/request)
                        (ps3/sign-request {:request-method :get
                                           :url "/"
                                           :headers {"Date" (ps3/date)}}
                                          aws-key
                                          aws-secret-key))]
    (ps3/xml-extract [(-> body
                          (ByteArrayInputStream.)
                          xml/parse)]
                     [:content :ListAllMyBucketsResult
                      :content :Buckets
                      :content :Bucket]
                     (fn [content]
                       {:name (->> (filter #(= :Name (:tag %)) content)
                                   first
                                   :content
                                   first)}))))

(defn delete-bucket [{:keys [aws-key aws-secret-key region]} bucket-name]
  (-> ((c/wrap-url http/request)
       (ps3/sign-request {:request-method :delete
                          :url "/"
                          :region (or region :us)
                          :bucket bucket-name
                          :headers {"Date" (ps3/date)}}
                         aws-key
                         aws-secret-key))
      :body
      nil?))

(defn write-stream [{:keys [aws-key aws-secret-key region]} bucket-name
                    object-name stream & {:keys [md5sum length]}]
  (-> ((c/wrap-url http/request)
       (ps3/sign-request {:request-method :put
                          :url (str "/" object-name)
                          :region (or region :us)
                          :bucket bucket-name
                          :headers (merge {"Date" (ps3/date)}
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
       (ps3/sign-request {:request-method :get
                          :url (str "/" object-name)
                          :region (or region :us)
                          :bucket bucket-name
                          :headers (merge {"Date" (ps3/date)}
                                          (when (or length offset)
                                            {"Range" (str "bytes="
                                                          (or offset 0)
                                                          "-" length)}))}
                         aws-key
                         aws-secret-key))
      :body
      ;; https://github.com/dakrone/clj-http/issues/39
      ByteArrayInputStream.))

(defn list-bucket
  ([creds bucket-name prefix length]
     (list-bucket creds bucket-name prefix length nil))
  ([{:keys [aws-key aws-secret-key region]} bucket-name prefix length start]
     (let [params (merge {"prefix" prefix
                          "max-keys" length}
                         (when start
                           {"marker" start}))
           {:keys [body]} ((c/wrap-url (c/wrap-query-params http/request))
                           (ps3/sign-request {:request-method :get
                                              :url "/"
                                              :region (or region :us)
                                              :bucket bucket-name
                                              :headers {"Date" (ps3/date)}
                                              :query-params params}
                                             aws-key
                                             aws-secret-key))]
       (ps3/xml-extract [(-> body
                             ByteArrayInputStream.
                             xml/parse)]
                        [:content :ListBucketResult
                         identity :Contents]
                        ps3/extract-key-data))))
