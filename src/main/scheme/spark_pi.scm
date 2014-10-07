(module-compile-options main: #f)

(require 'list-lib)

(define-alias SparkConf org.apache.spark.SparkConf)
(define-alias JavaSparkContext org.apache.spark.api.java.JavaSparkContext)
(define-alias Function org.apache.spark.api.java.function.Function)

(define-alias Tuple2 scala.Tuple2)

(define rand (java.util.Random))

(define (random)
  (rand:nextFloat))

(define (make-list a b)
  (iota (- b a) a))

(define (sum-of-square x ::float y ::float) ::float
  (+ (* x x) (* y y)))

(define (pi sc ::JavaSparkContext)
  (let* ((slices ::int 2)
         (n (* 100000 slices))
         (xn (make-list 1 n))
         (data (sc:parallelize xn slices))
         (sample (data:map (lambda (i)
                             (let ((x (- (* 2.0 (random)) 1.0))
                                   (y (- (* 2.0 (random)) 1.0)))
                               (if (< (sum-of-square x y) 1)
                                   1
                                   0)))))
         (count (sample:reduce (lambda (x y) (+ x y)))))
    (format #t "Pi is roughly ~A~%" (/ (* 4.0 count) n))))

(define (local-pi)
  (let* ((slices 2)
         (n (* 100000 slices))
         (xn (make-list 1 n))
         (sample (map (lambda (i)
                        (let ((x (- (* 2 (random)) 1))
                              (y (- (* 2 (random)) 1)))
                          (if (< (sum-of-square x y) 1)
                              1
                              0)))
                      xn))
         (count (reduce (lambda (x y) (+ x y))
                        0
                        sample)))
    (format #t "Pi is roughly ~A~%" (/ (* 4.0 count) n))))

(define (run)
  (let* ((conf (SparkConf)))
    (let ((sc (JavaSparkContext conf)))
      (pi sc))))

(define-simple-class SparkPi ()
  ((main (args ::String[])) ::void
   allocation: 'static
   (run)))
