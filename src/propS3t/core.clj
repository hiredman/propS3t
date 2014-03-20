(ns propS3t.core
  (:require [clojure.xml :as xml]
            [propS3t.support :as ps3])
  (:import (java.io FilterOutputStream)
           (java.net URLEncoder)))

(defn create-bucket
  "does what it says, creates a an s3 bucket with the given name"
  [{:keys [aws-key aws-secret-key region]} bucket-name]
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

(defn list-buckets
  "lists the available buckets for an account, result is a seq of maps {:name bucket-name}"
  [{:keys [aws-key aws-secret-key]}]
  (let [{:keys [body]} (ps3/request {:request-method :get
                                     :url "/"
                                     :headers {"Date" (ps3/date)}
                                     :as :stream}
                                    aws-key
                                    aws-secret-key)]
    (with-open [^java.io.Closeable body body]
      (ps3/xml-extract [(xml/parse body)]
                       [:content :ListAllMyBucketsResult
                        :content :Buckets
                        :content :Bucket]
                       (fn [content]
                         {:name (->> (filter #(= :Name (:tag %)) content)
                                     first
                                     :content
                                     first)})))))

(defn delete-bucket
  "deletes the named bucket"
  [{:keys [aws-key aws-secret-key region]} bucket-name]
  (-> (ps3/request {:request-method :delete
                    :url "/"
                    :region (or region :us)
                    :bucket bucket-name
                    :headers {"Date" (ps3/date)}}
                   aws-key
                   aws-secret-key)
      :body
      nil?))

(defn delete-object
  "deletes the object with the given key name in the given bucket"
  [{:keys [aws-key aws-secret-key region]} bucket-name object-name]
  (-> (ps3/request {:request-method :delete
                    :url (str "/" (URLEncoder/encode object-name))
                    :region (or region :us)
                    :bucket bucket-name
                    :headers {"Date" (ps3/date)}}
                   aws-key
                   aws-secret-key)
      :body
      count
      zero?))

(defn write-stream
  "writes an inputstream to the named object in the named bucket
  pass additional headers via :headers
  you must pass a :length"
  [{:keys [aws-key aws-secret-key region]} bucket-name object-name stream & {:keys [md5sum length headers]}]
  (-> (ps3/request {:request-method :put
                    :url (str "/" (URLEncoder/encode object-name))
                    :region (or region :us)
                    :bucket bucket-name
                    :headers (merge {"Date" (ps3/date)}
                                    (when md5sum
                                      {"Content-MD5" md5sum})
                                    headers)
                    :length length
                    :body stream}
                   aws-key
                   aws-secret-key)
      :body
      count
      zero?))

(defn output-stream [{:keys [aws-key aws-secret-key region]} bucket-name
                     object-name & {:keys [md5sum length headers]}]
  (let [the-pipe (ps3/pipe)
        fut (future
              (ps3/request {:request-method :put
                            :url (str "/" (URLEncoder/encode object-name))
                            :region (or region :us)
                            :bucket bucket-name
                            :headers (merge {"Date" (ps3/date)}
                                            (when md5sum
                                              {"Content-MD5" md5sum})
                                            headers)
                            :length length
                            :body (:inputstream the-pipe)}
                           aws-key
                           aws-secret-key))]
    (proxy [FilterOutputStream] [^java.io.Closeable (:outputstream the-pipe)]
      (close []
        (try
          (let [^java.io.Closeable this this]
            (proxy-super close))
          (finally
            (when-not (zero? (count (:body @fut)))
              (throw (Exception. (str "Uh Oh " (:body @fut)))))))))))

(defn read-stream [{:keys [aws-key aws-secret-key region]} bucket-name
                   object-name & {:keys [length offset]}]
  (-> (ps3/request {:request-method :get
                    :url (str "/" (URLEncoder/encode object-name))
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
       (with-open [^java.io.Closeable body body]
         (for [item (ps3/xml-extract [(xml/parse body)]
                                     [:content :ListBucketResult
                                      identity :Contents]
                                     ps3/extract-key-data)]
           (assoc item :bucket bucket-name))))))

(defn start-multipart
  [{:keys [aws-key aws-secret-key region]} bucket-name object-name]
  (let [{:keys [body]} (ps3/request {:request-method :post
                                     :url (str "/" (URLEncoder/encode object-name) "?uploads")
                                     :bucket bucket-name
                                     :region (or region :us)
                                     :as :stream
                                     :headers {"Date" (ps3/date)}}
                                    aws-key
                                    aws-secret-key)]
    (with-open [^java.io.Closeable body body]
      (first (ps3/xml-extract [(xml/parse body)]
                              [:content :InitiateMultipartUploadResult]
                              ps3/extract-multipart-upload-data)))))

(defn write-part
  [{:keys [aws-key aws-secret-key region]}
   {:keys [bucket upload-id key]}
   part-number stream
   & {:keys [md5sum length]}]
  (let [{{:strs [etag]} :headers}
        (ps3/request {:request-method :put
                      :url (str "/" (URLEncoder/encode key)
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
  (let [b (.getBytes ^String (ps3/multipart-xml-fragment parts))
        {:keys [body]} (ps3/request {:request-method :post
                                     :url (str "/" (URLEncoder/encode object-name) "?uploadId="
                                               upload-id)
                                     :bucket bucket-name
                                     :region (or region :us)
                                     :headers {"Date" (ps3/date)}
                                     :body b
                                     :as :stream
                                     :length (count b)}
                                    aws-key
                                    aws-secret-key)]
    (with-open [^java.io.Closeable body body]
      (first (ps3/xml-extract [(xml/parse body)]
                              [:content :CompleteMultipartUploadResult
                               :content :ETag]
                              (fn [[t]]
                                (subs t 1 (dec (count t)))))))))
