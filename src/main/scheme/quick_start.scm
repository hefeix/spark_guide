(module-compile-options main: #f)

;; (require org.apache.spark.SparkConf)
;; (require org.apache.spark.api.java.JavaSparkContext)
;; (require org.apache.spark.api.java.function.Function)

(define-alias SparkConf org.apache.spark.SparkConf)
(define-alias JavaSparkContext org.apache.spark.api.java.JavaSparkContext)
(define-alias Function org.apache.spark.api.java.function.Function)

;; (define-simple-class StringFilter (Function)
;;   (pattern ::String init-keyword: pattern:)
;;   ((*init* (p ::String))
;;    (set! pattern p))
;;   ((call (text ::String)) ::java.lang.Boolean
;;    (text:contains pattern)))

;; (define-simple-class SparkFunction (Function java.io.Serializable)
;;   (fn init-keyword: fn:)
;;   ((*init* f)
;;    (set! fn f))
;;   ((call t)
;;    (fn t)))

;; (define (make-filter-o pattern ::String)
;;    (object (Function)
;;            ((call (text ::String)) ::java.lang.Boolean
;;             (text:contains pattern))))

(define (make-filter pattern ::String) ::Function
  (lambda (text ::String) ::java.lang.Boolean
          (text:contains pattern)))

(define (quick-start file)
  (let* ((conf (SparkConf))
         (sc (JavaSparkContext conf))
         (data (sc:textFile file)))
    (format #t "number of a: ~A, number of b: ~A~%"
      ((data:filter (make-filter "a")):count)
      ((data:filter (make-filter "b")):count))))

(define (run)
  (quick-start "README.md"))

(define-simple-class QuickStart ()
  ((main (args ::String[])) ::void
   allocation: 'static
   (run)))
