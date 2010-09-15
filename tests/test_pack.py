
import datetime
import decimal
import os
import random
import sys
import unittest

from .lib import pack

class TestPacking(unittest.TestCase):
    def _verify(self, d, cs=True, neg=False):
        # this is a helper function that compares two sequences for
        # equivalence after encoding
        random.shuffle(d)
        f = lambda x: pack.pack(x, case_sensitive=cs, neg=neg)
        e = map(f, d)
        if not cs:
            d = [di.lower() for di in d]
        d.sort(reverse=neg)
        e.sort()
        ## t = time.time()
        ed = map(f, d)
        ## print >>sys.stderr, '\n', (time.time()-t) / len(d), " ",
        if ed != e:
            print >>sys.stderr, ''
            for i, (j,k) in enumerate(zip(ed, e)):
                if j == k:
                    continue
                print >>sys.stderr, (d[i], j, k)
            raise AssertionError

    def test_packing_int(self, t=int):
        # this tests integer packing
        d = range(-1000, 1000)
        for i in xrange(1000):
            d.append(random.randrange(-2**30, 2**30))
        d = map(t, d)
        self._verify(d)
        self._verify(d, neg=True)

    def test_packing_long(self):
        # this tests long packing
        self.test_packing_int(t=long)

    def test_packing_float(self, t=float):
        # this tests float and decimal packing
        d = []
        for i in xrange(1000):
            d.append((1,-1)[random.randrange(2)]*random.random() * 10.0**random.randrange(-20,21))
        d = map(t, map(str, d))
        self._verify(d)
        self._verify(d, neg=True)

    def test_packing_decimal(self):
        # This tests decimal packing with and without binary-coded-decimal
        # mantissas.
        self.test_packing_float(t=decimal.Decimal)
        pack.USE_BCD = False
        self.test_packing_float(t=decimal.Decimal)

    def test_packing_str(self):
        # this tests string packing
        d = []
        for i in xrange(1000):
            d.append(os.urandom(random.randrange(10,50)))
        self._verify(d)
        self._verify(d, cs=False)
        self._verify(d, neg=True)
        self._verify(d, cs=False, neg=True)

    def test_packing_unicode(self, cs=True):
        # this tests unicode packing
        d = []
        for i in xrange(1000):
            # We aren't interested in comparisons with surrogates, but if it
            # works for code points below surrogates, we'll consider it good
            # for all non-surrogates.
            d.append(u''.join(unichr(random.randrange(0xd7b0)) for i in xrange(random.randrange(10,50))))
        self._verify(d)
        self._verify(d, cs=False)
        self._verify(d, neg=True)
        self._verify(d, cs=False, neg=True)

    def test_pack_sequence(self):
        data = (1, 40L, 1.4, decimal.Decimal('4.2'), 'hello', u'hello', None)
        seqa = pack.pack(data)
        seqb = tuple(map(pack.pack, data))
        self.assertEquals(seqa, seqb)

    def test_packing_datetime(self):
        d = []
        for i in xrange(1000):
            # 719163 is January 1, 1970
            # 744018 is January 19, 2038
            # the maximum ordinal for dates is 3652059
            dt = datetime.datetime.fromordinal(random.randrange(3652060)) + \
                datetime.timedelta(seconds=random.randrange(86400),
                                   microseconds=random.randrange(1000000))
            d.append(dt)
        self._verify(d)
        self._verify(d, neg=True)

    def test_packing_date(self):
        d = []
        for i in xrange(1000):
            d.append(datetime.datetime.fromordinal(random.randrange(3652060)))
        self._verify(d)
        self._verify(d, neg=True)

    def test_packing_time(self):
        d = []
        for i in xrange(1000):
            d.append(datetime.time(random.randrange(24),
                                   random.randrange(60),
                                   random.randrange(60),
                                   random.randrange(1000000)))
        self._verify(d)
        self._verify(d, neg=True)
