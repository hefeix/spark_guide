(module-compile-options main: #f)

(define-alias SparkConf org.apache.spark.SparkConf)
(define-alias JavaSparkContext org.apache.spark.api.java.JavaSparkContext)
(define-alias Function org.apache.spark.api.java.function.Function)

(define-alias Tuple2 scala.Tuple2)

(define (parallelized-collections sc ::JavaSparkContext)
  (let ((data (sc:parallelize (java.util.ArrayList 1 2 3 4 5))))
    (let ((value (data:reduce (lambda (x y) (+ x y)))))
      (format #t "parallelized-colletions result = ~A~%" value))))

(define (basics sc ::JavaSparkContext)
  (let* ((lines (sc:textFile "README.md"))
         (line-lengths (lines:map (lambda (s ::String) (s:length))))
         (total-length (line-lengths:reduce (lambda (x y) (+ x y)))))
    (format #t "basics result = ~A~%" total-length)))

(define (key-value-pairs sc ::JavaSparkContext)
  (let* ((lines (sc:textFile "README.md"))
         (pairs (lines:mapToPair (lambda (s ::String) (Tuple2 s 1))))
         (counts (pairs:reduceByKey (lambda (x y) (+ x y))))
         (results (counts:collect)))
    (format #t "key values results = ~A~%" results)))

(define (word-counts sc ::JavaSparkContext)
  (let* ((lines (sc:textFile "README.md"))
         (words (lines:flatMap (lambda (s ::String)
                                 (java.util.Arrays:asList (s:split " ")))))
         (pairs (words:mapToPair (lambda (s ::String) (Tuple2 s 1))))
         (counts (pairs:reduceByKey (lambda (x y) (+ x y))))
         (results (counts:collect)))
    (format #t "word counts results = ~A~%" results)))

(define (run)
  (let* ((conf (SparkConf))
         (sc (JavaSparkContext conf)))
    (parallelized-collections sc)
    (basics sc)
    (key-value-pairs sc)
    (word-counts sc)
    ))

(define-simple-class SparkGuide ()
  ((main (args ::String[])) ::void
   allocation: 'static
   (run)))
