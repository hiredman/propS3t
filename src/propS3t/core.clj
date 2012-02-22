(ns propS3t.core
  (:require [clojure.xml :as xml]
            [propS3t.support :as ps3])
  (:import (java.io FilterOutputStream)))

(defn create-bucket [{:keys [aws-key aws-secret-key region]} bucket-name]
  (-> (ps3/request {:request-method :put
                    :url "/"
                    :region (or region :us)
                    :bucket bucket-name
                    :headers {"Date" (ps3/date)}}
                   aws-key
                   aws-secret-key)
      :body
      count
      zero?))

(defn list-buckets [{:keys [aws-key aws-secret-key]}]
  (let [{:keys [body]} (ps3/request {:request-method :get
                                     :url "/"
                                     :headers {"Date" (ps3/date)}
                                     :as :stream}
                                    aws-key
                                    aws-secret-key)]
    (with-open [body body]
      (ps3/xml-extract [(xml/parse body)]
                       [:content :ListAllMyBucketsResult
                        :content :Buckets
                        :content :Bucket]
                       (fn [content]
                         {:name (->> (filter #(= :Name (:tag %)) content)
                                     first
                                     :content
                                     first)})))))

(defn delete-bucket [{:keys [aws-key aws-secret-key region]} bucket-name]
  (-> (ps3/request {:request-method :delete
                    :url "/"
                    :region (or region :us)
                    :bucket bucket-name
                    :headers {"Date" (ps3/date)}}
                   aws-key
                   aws-secret-key)
      :body
      nil?))

(defn write-stream [{:keys [aws-key aws-secret-key region]} bucket-name
                    object-name stream & {:keys [md5sum length]}]
  (-> (ps3/request {:request-method :put
                    :url (str "/" object-name)
                    :region (or region :us)
                    :bucket bucket-name
                    :headers (merge {"Date" (ps3/date)}
                                    (when md5sum
                                      {"Content-MD5" md5sum}))
                    :length length
                    :body stream}
                   aws-key
                   aws-secret-key)
      :body
      count
      zero?))

(defn output-stream [{:keys [aws-key aws-secret-key region]} bucket-name
                     object-name & {:keys [md5sum length]}]
  (let [the-pipe (ps3/pipe)
        fut (future
              (-> (ps3/request {:request-method :put
                                :url (str "/" object-name)
                                :region (or region :us)
                                :bucket bucket-name
                                :headers (merge {"Date" (ps3/date)}
                                                (when md5sum
                                                  {"Content-MD5" md5sum}))
                                :length length
                                :body (:inputstream the-pipe)}
                               aws-key
                               aws-secret-key)
                  :body
                  count
                  zero?))]
    (proxy [FilterOutputStream] [(:outputstream the-pipe)]
      (close []
        (try
          (proxy-super close)
          (finally
           @fut))))))

(defn read-stream [{:keys [aws-key aws-secret-key region]} bucket-name
                   object-name & {:keys [length offset]}]
  (-> (ps3/request {:request-method :get
                    :url (str "/" object-name)
                    :region (or region :us)
                    :bucket bucket-name
                    :as :stream
                    :headers (merge {"Date" (ps3/date)}
                                    (when (or length offset)
                                      {"Range" (str "bytes="
                                                    (or offset 0)
                                                    "-" length)}))}
                   aws-key
                   aws-secret-key)
      :body))

(defn list-bucket
  ([creds bucket-name prefix length]
     (list-bucket creds bucket-name prefix length nil))
  ([{:keys [aws-key aws-secret-key region]} bucket-name prefix length start]
     (let [params (merge {"prefix" prefix
                          "max-keys" length}
                         (when start
                           {"marker" start}))
           {:keys [body]} (ps3/request {:request-method :get
                                        :url "/"
                                        :region (or region :us)
                                        :bucket bucket-name
                                        :headers {"Date" (ps3/date)}
                                        :as :stream
                                        :query-params params}
                                       aws-key
                                       aws-secret-key)]
       (with-open [body body]
         (ps3/xml-extract [(xml/parse body)]
                          [:content :ListBucketResult
                           identity :Contents]
                          ps3/extract-key-data)))))

(defn start-multipart
  [{:keys [aws-key aws-secret-key region]} bucket-name object-name]
  (let [{:keys [body]} (ps3/request {:request-method :post
                                     :url (str "/" object-name "?uploads")
                                     :bucket bucket-name
                                     :region (or region :us)
                                     :as :stream
                                     :headers {"Date" (ps3/date)}}
                                    aws-key
                                    aws-secret-key)]
    (with-open [body body]
      (first (ps3/xml-extract [(xml/parse body)]
                              [:content :InitiateMultipartUploadResult]
                              ps3/extract-multipart-upload-data)))))

(defn write-part
  [{:keys [aws-key aws-secret-key region]} {:keys [bucket upload-id key]}
   part-number stream & {:keys [md5sum length]}]
  (let [{{:strs [etag]} :headers}
        (ps3/request {:request-method :put
                      :url (str "/" key
                                "?partNumber=" part-number
                                "&uploadId=" upload-id)
                      :region (or region :us)
                      :bucket bucket
                      :headers (merge
                                {"Date" (ps3/date)}
                                (when md5sum
                                  {"Content-MD5" md5sum}))
                      :length length
                      :body stream}
                     aws-key
                     aws-secret-key)]
    {:part part-number :tag (subs etag 1 (dec (count etag)))}))

(defn end-multipart
  [{:keys [aws-key aws-secret-key region]} {:keys [upload-id]} bucket-name
   object-name parts]
  (let [b (.getBytes (ps3/multipart-xml-fragment parts))
        {:keys [body]} (ps3/request {:request-method :post
                                     :url (str "/" object-name "?uploadId="
                                               upload-id)
                                     :bucket bucket-name
                                     :region (or region :us)
                                     :headers {"Date" (ps3/date)}
                                     :body b
                                     :as :stream
                                     :length (count b)}
                                    aws-key
                                    aws-secret-key)]
    (with-open [body body]
      (first (ps3/xml-extract [(xml/parse body)]
                              [:content :CompleteMultipartUploadResult
                               :content :ETag]
                              (fn [[t]]
                                (subs t 1 (dec (count t)))))))))
