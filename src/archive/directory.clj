(ns archive.directory
  (:require
   [clojure.java.io :as io])
  (:import
   (org.apache.commons.compress.archivers.tar
     TarArchiveOutputStream
     TarArchiveInputStream)
   (org.apache.commons.compress.compressors.gzip
     GzipCompressorOutputStream
     GzipCompressorInputStream)
   (java.io File)
   (java.nio.file Files Path FileVisitOption LinkOption OpenOption)))

(defn add-entry! [os path entry-name]
  (let [entry (.createArchiveEntry os path entry-name (make-array LinkOption 0))]
    (.putArchiveEntry os entry)
    (with-open [is (Files/newInputStream path (make-array OpenOption 0))]
      (io/copy is os))
    (.closeArchiveEntry os)))

(defn simple-path-pred [^Path p]
  (not (-> p .toFile .isHidden)))

(defn archive-directory
  ([^Path dir dest-os]
   (archive-directory dir simple-path-pred dest-os))
  ([^Path dir path-pred dest-os]
   (with-open [tgz (doto (-> dest-os
                             GzipCompressorOutputStream.
                             TarArchiveOutputStream.)
                     (.setLongFileMode (. TarArchiveOutputStream LONGFILE_POSIX))
                     (.setBigNumberMode (. TarArchiveOutputStream BIGNUMBER_POSIX)))]
     (doseq [^Path path (-> (Files/walk dir Integer/MAX_VALUE (make-array FileVisitOption 0))
                            .iterator
                            iterator-seq)
             :when (and (path-pred path)
                        (-> path .toFile .isFile))
             :let [entry-name (str (.relativize dir path))]]
       (add-entry! tgz path entry-name)))))

(defn restore-directory
  [src-is out-folder]
  (with-open [ais (-> src-is
                      GzipCompressorInputStream.
                      TarArchiveInputStream.)]
    (loop [entry (.getNextEntry ais)
           cnt   0]
      (if entry
        (let [save-path (str out-folder File/separatorChar (.getName entry))
              out-file  (File. save-path)]
          (if (.isDirectory entry)
            (when-not (.exists out-file)
              (.mkdirs out-file))
            (let [parent-dir
                  (File.
                    (.substring save-path 0
                                (.lastIndexOf save-path
                                              (int File/separatorChar))))]
              (when-not (.exists parent-dir)
                (.mkdirs parent-dir))
              (io/copy ais out-file :buffer-size 8192)))
          (recur (.getNextEntry ais) (inc cnt)))
        cnt))))
