

'''

Testcases derived from: http://norvig.com/lispytest.py

'''

import decimal
import datetime
import unittest

from thirdparty import lispy

def fact(n):
    i = 1
    while n > 1:
        i *= n
        n -= 1
    return i

lis_tests = [
    ("(quote (testing 1 (2.0) -3.14e159))", ['testing', 1, [2.0], -3.14e159]),
    ("(+ 2 3 2)", 7),
    ("(/ 5.0 2 1.25)", 2),
    ("(* 4 2 3)", 24),
    ("(+ (* 2 100) (* 1 10))", 210),
    ("(if (> 6 5) (+ 1 1) (+ 2 2))", 2),
    ("(if (< 6 5) (+ 1 1) (+ 2 2))", 4),
    ("(define x 3)", None), ("x", 3), ("(+ x x)", 6),
    ("(begin (define x 1) (set! x (+ x 1)) (+ x 1))", 3),
    ("((lambda (x) (+ x x)) 5)", 10),
    ("(define twice (lambda (x) (* 2 x)))", None), ("(twice 5)", 10),
    ("(define compose (lambda (f g) (lambda (x) (f (g x)))))", None),
    ("((compose list twice) 5)", [10]),
    ("(define repeat (lambda (f) (compose f f)))", None),
    ("((repeat twice) 5)", 20), ("((repeat (repeat twice)) 5)", 80),
    ("(define fact (lambda (n) (if (<= n 1) 1 (* n (fact (- n 1))))))", None),
    ("(fact 3)", 6),
    ("(fact 50)", 30414093201713378043612608166064768844377641568960512000000000000),
    ("(define abs (lambda (n) ((if (> n 0) + -) 0 n)))", None),
    ("(list)", []),
    ('(list (abs -3) (abs 0) (abs 3) "hello")', [3, 0, 3, "hello"]),
    ("""(define combine (lambda (f)
    (lambda (x y)
      (if (null? x) (quote ())
          (f (list (car x) (car y))
             ((combine f) (cdr x) (cdr y)))))))""", None),
    ("(define zip (combine cons))", None),
    ("(zip (list 1 2 3 4) (list 5 6 7 8))", [[1, 5], [2, 6], [3, 7], [4, 8]]),
    ("""(define riff-shuffle (lambda (deck) (begin
    (define take (lambda (n seq) (if (<= n 0) (quote ()) (cons (car seq) (take (- n 1) (cdr seq))))))
    (define drop (lambda (n seq) (if (<= n 0) seq (drop (- n 1) (cdr seq)))))
    (define mid (lambda (seq) (/ (length seq) 2)))
    ((combine append) (take (mid deck) deck) (drop (mid deck) deck)))))""", None),
    ("(riff-shuffle (list 1 2 3 4 5 6 7 8))", [1, 5, 2, 6, 3, 7, 4, 8]),
    ("((repeat riff-shuffle) (list 1 2 3 4 5 6 7 8))",  [1, 3, 5, 7, 2, 4, 6, 8]),
    ("(riff-shuffle (riff-shuffle (riff-shuffle (list 1 2 3 4 5 6 7 8))))", [1,2,3,4,5,6,7,8]),
    # test module loading and types
    ("(begin (load math) (sin 1))", 0.8414709848078965),
    ("(begin (load ops) (xor 1 2))", 3),
    ("(begin (load types) (datetime 2010 10 9 13 25 12 987))", datetime.datetime(2010, 10, 9, 13, 25, 12, 987)),
    ('(begin (load types) (set 1 2 3 "hello"))', set([1,2,3,"hello"])),
    ('(begin (load types) (+ (decimal "1.234") 1))', decimal.Decimal("2.234")),
    ('(begin (load types) (+ (decimal "1.234") 1 2))', decimal.Decimal("4.234")),
    # test the tail recursion elimination
    ("(define facti (lambda (n acc) (if (<= n 1) acc (facti (- n 1) (* n acc)))))", None),
    ("(facti 10000 1)", fact(10000)),
]

lispy_tests = [
    ("()", SyntaxError), ("(set! x)", SyntaxError),
    ("(define 3 4)", SyntaxError),
    ("(quote 1 2)", SyntaxError), ("(if 1 2 3 4)", SyntaxError),
    ("(lambda 3 3)", SyntaxError), ("(lambda (x))", SyntaxError),
    ("(define (twice x) (* 2 x))", None), ("(twice 2)", 4),
    ("(twice 2 2)", TypeError),
    ("(define lyst (lambda items items))", None),
    ("(lyst 1 2 3 (+ 2 2))", [1,2,3,4]),
    ("(if 1 2)", 2),
    ("(if (= 3 4) 2)", None),
    ("(define ((account bal) amt) (set! bal (+ bal amt)) bal)", None),
    ("(define a1 (account 100))", None),
    ("(a1 0)", 100), ("(a1 10)", 110), ("(a1 10)", 120),
    ("""(define (newton guess function derivative epsilon)
    (define guess2 (- guess (/ (function guess) (derivative guess))))
    (if (< (abs (- guess guess2)) epsilon) guess2
        (newton guess2 function derivative epsilon)))""", None),
    ("""(define (square-root a)
    (newton 1 (lambda (x) (- (* x x) a)) (lambda (x) (* 2 x)) 1e-8))""", None),
    ("(> (square-root 200.) 14.14213)", True),
    ("(< (square-root 200.) 14.14215)", True),
    ("(load math)", None),
    ("(= (square-root 200.) (sqrt 200.))", True),
    ("""(define (sum-squares-range start end)
         (define (sumsq-acc start end acc)
            (if (> start end) acc (sumsq-acc (+ start 1) end (+ (* start start) acc))))
         (sumsq-acc start end 0))""", None),
    ("(sum-squares-range 1 10000)", 333383335000), ## Tests tail recursion
    ("(and 1 2 3)", 3), ("(and (> 2 1) 2 3)", 3), ("(and)", True),
    ("(and (> 2 1) (> 2 3))", False),
    ("(quote x)", 'x'),
    ("(equal? 'x (quote x))", True), # test string literal quoting
    ("(begin '3)", "3"),
    ("(quote (1 2 three))", [1, 2, 'three']),
    ("(define L (list 1 2 3))", None),
    ("(let ((a 1) (b 2)) (+ a b))", 3),
    ("(let ((a 1) (b 2 3)) (+ a b))", SyntaxError),
    ("(let ((a 1) (b 2)) (+ a b) (- a b))", SyntaxError),
    ('''(let ((a 1))
          (let ((b 2))
            (+ a b)))''', 3), # test recursive lets
    ("(letrec ((a 1) (b 2) (c (+ a b))) c)", 3),
    ("(letrec ((a 1) (b 2 3)) (+ a b))", SyntaxError),
    ("(letrec ((a 1) (b 2)) (+ a b) (- a b))", SyntaxError),
    # We don't support define-macro, call/cc, real quasiquotes, etc.
    ## ("""(if (= 1 2) (define-macro a 'a)
     ## (define-macro a 'b))""", SyntaxError),
    ## ("(call/cc (lambda (throw) (+ 5 (* 10 (throw 1))))) ;; throw", 1),
    ## ("(call/cc (lambda (throw) (+ 5 (* 10 1)))) ;; do not throw", 15),
    ## ("""(call/cc (lambda (throw)
         ## (+ 5 (* 10 (call/cc (lambda (escape) (* 100 (escape 3)))))))) ; 1 level""", 35),
    ## ("""(call/cc (lambda (throw)
         ## (+ 5 (* 10 (call/cc (lambda (escape) (* 100 (throw 3)))))))) ; 2 levels""", 3),
    ## ("""(call/cc (lambda (throw)
         ## (+ 5 (* 10 (call/cc (lambda (escape) (* 100 1))))))) ; 0 levels""", 1005),
    ## ("(* 1i 1i)", -1), ("(sqrt -1)", 1j),
    ## ("(define-macro unless (lambda args `(if (not ,(car args)) (begin ,@(cdr args))))) ; test `", None),
    ## ("(unless (= 2 (+ 1 1)) (display 2) 3 4)", None),
    ## (r'(unless (= 4 (+ 1 1)) (display 2) (display "\n") 3 4)', 4),
    ## ("'(one 2 3)", ['one', 2, 3]),
    ## ("`(testing ,@L testing)", ['testing',1,2,3,'testing']),
    ## ("`(testing ,L testing)", ['testing',[1,2,3],'testing']),
    ## ("`,@L", SyntaxError),
    ## ("""(list 1 ;test comments '
     ## ;skip this line
     ## 2 ; more ; comments ; ) )
     ## 3) ; final comment""", [1,2,3]),
]

class TestLispInterpreter(unittest.TestCase):
    def test_lispy(self):
        for x, expected in lis_tests+lispy_tests:
            try:
                returned = lispy.eval(lispy.parse(x))
                self.assertEquals(returned, expected, (
                    "%r -> %r expected: %r"%(x, returned, expected)))
            except Exception as e:
                if type(e) is expected:
                    continue
                print x, '=raises=>', type(e).__name__, e
                raise

    def test_lispy_script(self):
        s = """
        (define add-stats (lambda (v)
            (begin
                (setv 'shared 'count
                    (+ 1 (getv 'shared 'count 0.0)))
                (setv 'shared 'sum
                    (+ v (getv 'shared 'sum 0.0)))
                (setv 'shared 'sumsq
                    (+ (* v v) (getv 'shared 'sumsq 0.0)))
                (/ (getv 'shared 'sum) (getv 'shared 'count)))))
        (define get-stats (lambda()
            (letrec
                ((n (or (getv 'shared 'count 0.0) 1))
                 (n1 (max (- n 1) 1))
                 (s (getv 'shared 'sum))
                 (sq (getv 'shared 'sumsq 0.0))
                 (avg (/ s n)))
                (begin
                    (load math)
                    (setv 'doc 'n n)
                    (setv 'doc 'avg avg)
                    (setv 'doc 'stddev (sqrt (/ (- sq (* avg avg n)) n1)))))))
        (add-stats 5)
        (add-stats 7)
        (add-stats 2)
        (get-stats)
        """
        doc, shared = {}, {}
        lispy.run_script(s, doc, shared)
        self.assertEquals(shared, {'count':3, 'sum':14, 'sumsq': 78})
        self.assertEquals(doc, {u'avg': 4.666666666666667, u'stddev': 2.5166114784235822, u'n': 3.0})
        self.assertRaises(NameError, lambda:lispy.run_script("(setv 'shared '.foo 1)", {}, {}))
        self.assertRaises(NameError, lambda:lispy.run_script("(setv 'shared '3.foo 1)", {}, {}))

