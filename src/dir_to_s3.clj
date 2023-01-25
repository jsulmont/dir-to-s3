(ns dir-to-s3
  (:require
   [clojure.instant :as instant]
   [clojure.edn :as edn]
   [archive.directory :as dir]
   [clojure.datafy :as d]
   [chunked.output-stream :as c]
   [chunked.multipart-upload :as mpu])
  (:import
   (java.nio.file Paths)
   (java.util Date)
   (java.text SimpleDateFormat)
   (software.amazon.awssdk.regions Region)
   (software.amazon.awssdk.services.s3 S3AsyncClient)
   (software.amazon.awssdk.auth.credentials ProfileCredentialsProvider)))

(defn dir-to-s3 [dir s3 metadata]
  (let [os (c/output-stream (mpu/create-buffer-sink s3 metadata))]
    (dir/archive-directory dir dir/simple-path-pred os)))

(def tx-number (atom 0))

(defn format-rfc3339-date [^Date d]
  (when d
    (.format ^SimpleDateFormat (.get ^ThreadLocal @#'instant/thread-local-utc-date-format) d)))

(defn gen-metadata
  [base]
  (let [now (Date.)
        file-name (format "x/%s-%s.tgz" base (format-rfc3339-date now))]
    [file-name
     {:xtdb.checkpoint/cp-format {:index-version 20,
                                  :xtdb.rocksdb/version "6"},
      :tx {:xtdb.api/tx-time now
           :xtdb.api/tx-id (swap! tx-number inc)},
      :xtdb.checkpoint/checkpoint-at now}]))



(comment

  (do

    (def s3-client (-> (S3AsyncClient/builder)
                       (.credentialsProvider (. ProfileCredentialsProvider create "jan-juxt"))
                       (.region (Region/EU-WEST-1))
                       (.build)))

    (def test-dir "/Users/jan/tmp/aws-doc-sdk-examples")

    (def test-bucket-name "jan-bucket1") ;; should exist

    (def s3  {:client s3-client
              :bucket test-bucket-name
              :part-upload-timeout (java.time.Duration/parse "PT10M")}))

  (let [[k m] (gen-metadata "aws-doc")]
    (printf "uploading %s with %s\n " k m)
    (dir-to-s3 (Paths/get test-dir  (make-array String 0))
               (assoc s3 :key k) m))




  ;; tidy up failed uploads - if you don't do this you have space
  ;; allocated in S3 that is not 'obvious' and you still have to pay
  ;; for.
  (doseq [upload-id (->> s3 mpu/list-multipart-uploads (map (comp :upload-id d/datafy)))]
    (.println *err* (str "aborting " upload-id))
    (mpu/abort-multipart-upload s3 upload-id))

  (let [uploads (-> "/Users/jan/uploads.edn"
                    slurp
                    (edn/read-string)
                    :Uploads)]
    (doseq [{:keys [UploadId Key]} uploads
            :let [s (format  "aws s3api abort-multipart-upload --bucket jan-bucket1 --upload-id \"%s\" --key \"%s\"\n"
                             UploadId Key)]]
      (spit "/tmp/doit.sh" s :append true)))

  :ok)
