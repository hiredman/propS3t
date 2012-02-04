(ns propS3t.support
  (:require [clojure.set :as set])
  (:import (javax.crypto Mac)
           (javax.crypto.spec SecretKeySpec)
           (sun.misc BASE64Encoder)
           (java.util Date)
           (java.text SimpleDateFormat FieldPosition)))

(def bucket-ops-end-point "http://s3.amazonaws.com")

(def region-end-points
  {:us "http://%s.s3.amazonaws.com"})

;; if you have your own date-formater bind this so you are not making
;; requests all the time
(def ^{:dynamic true} *date-formater* nil)

(def date-format "EEE, d MMM yyyy HH:mm:ss Z")

(defn date-formater []
  (SimpleDateFormat. date-format))

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

(defmacro xml-extract [roots path-spec fun]
  (let [root (gensym 'root)
        {:keys [out prev]}
        (reduce
         (fn [{:keys [out prev]} fun]
           (let [[prev form] (fun prev)]
             {:prev prev
              :out (into out form)}))
         {:prev root
          :out []}
         (for [[fun p] (partition-all 2 path-spec)]
           (fn [pitem]
             (let [item (gensym 'item)]
               [item `[:when (= (:tag ~pitem) ~p)
                       ~item (~fun ~pitem)]]))))
        last-v (peek out)
        out (pop out)
        last-b (peek out)
        out (pop out)]
    `(for ~(into [root roots] out)
       (~fun ~last-v))))

(defn extract-multipart-upload-data [item]
  (set/rename-keys
   (let [snag {:Bucket (comp first :content)
               :Key (comp first :content)
               :UploadId (comp first :content)}]
     (reduce
      (fn [out element]
        (if (contains? snag (:tag element))
          (assoc out (:tag element)
                 ((get snag (:tag element)) element))
          out))
      {} item))
   {:Bucket :bucket
    :Key :key
    :UploadId :upload-id}))

(defn multipart-xml-fragment [parts]
  (let [result (StringBuffer.)]
    (.append result "<CompleteMultipartUpload>")
    (doseq [{:keys [part tag]} parts]
      (doto result
        (.append "<Part>")
        (.append "<PartNumber>")
        (.append part)
        (.append "</PartNumber>")
        (.append "<ETag>")
        (.append tag)
        (.append "</ETag>")
        (.append "</Part>")))
    (.append result "</CompleteMultipartUpload>")
    (.toString result)))
