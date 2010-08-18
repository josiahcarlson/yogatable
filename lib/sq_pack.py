
import array
from datetime import datetime, date, time
from decimal import Decimal as decimal
import itertools
import struct

import sq_exceptions

def _pack_int(v, case_sensitive=True, neg=False):
    '''
    This will pack a positive or negative integer of at most 128 base-256
    digits long such that prefix-comparisons of these values will match the
    unpacked comparisons with the same semantics.
    '''
    d = array.array('B')
    if neg:
        v = -v
    xor = 0
    if v < 0:
        v = -v-1
        xor = 0xff

    # first byte:
    # <pos><more><6 bits>

    while v > 0xff:
        subv = v & 0xff
        d.append(subv ^ xor)
        v >>= 8
    d.append(v ^ xor)
    if xor:
        d.append(128-len(d))
    else:
        d.append(127+len(d))
    d.append(105) # character 'i'
    d.reverse()
    ret = d.tostring()
    return ret

def _pack_float(val, case_sensitive=True, neg=False):
    '''
    This will pack an IEEE 754 FP double such that prefix-comparisons of these
    values will match the unpacked comparisons with the same semantics.
    '''
    if neg:
        val = -val
    data = map(ord, struct.pack('>d', abs(val)))
    # Sign bit on is 0 on positive, 1 on negative.  This is opposite of how we
    # would compare it byte-wise.
    data[0] ^= 0x80
    if val < 0:
        # it's negative, flip all the bits
        for i in xrange(8):
            data[i] ^= 0xff
    return 'f' + ''.join(map(chr, data))

USE_BCD = True
def _pack_decimal(d, case_sensitive=True, neg=False):
    '''
    This supports decimals on the order of +/- 10**(-32768) to 10**32767 .
    This is over 100 orders of magnitude greater than floats, and can support
    whatever precision your decimals already have.
    '''
    td = decimal(d)
    if neg:
        td._sign ^= 1
    sign = 'n' if td._sign else 'p'
    si = td._int
    lsi = len(si)
    magnitude = lsi + td._exp
    if td._sign:
        # We need to mangle negative numbers into positive numbers for proper
        # sorts.
        si = ("%%0%ii"%(lsi,))%(10**lsi - int(td._int))
        ## si = ''.join(map(str, [10-i for i in map(int, si)]))
    magnitude = struct.pack('>H', 32768+((-magnitude) if sign == 'n' else magnitude))
    mantissa = si
    if USE_BCD:
        # This will use binary-coded decimal to store the value instead of the
        # pre-existing decimal representation.  This will halve the storage
        # requirements for the 'mantissa' portion of the value.
        imantissa = iter(itertools.imap(int, mantissa))
        mantissa = ''.join(itertools.imap(chr,
            (((l+1)<<4) + (r+1)
                for l,r in itertools.izip_longest(imantissa, imantissa, fillvalue=0))))
    return sign + magnitude + mantissa + '\0'

def _pack_data(v, case_sensitive=True, neg=False):
    # Because null is valid in strings, we need to either use base255, or we
    # need to use base 128.  Since base 128 is fast, we'll use that.
    # More specifically, we'll use values 1..127 for data, and \0 for the
    # string terminator.  Because we special-case nulls (see below), we don't
    # need to keep an entry for them :)
    #
    # We're going to squeeze unicode in here because we like to be able to
    # compare equivalent strings to their unicode counterparts.
    # Incidentally, this breaks for surrogates.  We don't really care.
    #
    # Also, because we will have a large number of 0's due to our choice of
    # a maximally-compatible encoding (utf-32-be), we are going to output a
    # bit that tells us whether the subsequent byte is 0 or not, then if it
    # is not 0, output the actual value.
    MASKS = dict((i, (1<<i)-1) for i in xrange(9))
    if not case_sensitive:
        v = v.lower()
    if not isinstance(v, unicode):
        v = v.decode('latin-1')
    v = array.array('B', v.encode('utf-32-be'))
    v.reverse()
    out = array.array('B', 'd')
    bits = 0
    value = 0
    while v:
        sv = v.pop()
        value <<= 1
        bits += 1
        value += bool(sv)
        if sv:
            value <<= 8
            value += sv
            bits += 8
        while bits >= 7:
            out.append(1 + (value >> (bits-7)))
            bits -= 7
            value &= MASKS[bits]
    if bits:
        value <<= (7-bits)
        out.append(1 + value)
    out.append(0)
    if neg:
        for i in xrange(1, len(out)):
            out[i] ^= 0xff
    return out.tostring()

def _pack_datetime(v, case_sensitive=True, neg=False):
    assert v.tzinfo is None
    # the zero day is jan 1, 1970
    days = (v.toordinal()-719163)*86400*1000000
    seconds = (v.hour*3600 + v.minute*60)*1000000
    microseconds = v.microsecond
    return 't' + _pack_int(days + seconds + microseconds, neg=neg)[1:]

def _pack_date(v, case_sensitive=True, neg=False):
    return _pack_datetime(datetime.fromordinal(v.toordinal()), neg=neg)

def _pack_time(v, case_sensitive=True, neg=False):
    assert v.tzinfo is None
    seconds = (v.hour*3600 + v.minute*60 + v.second)*1000000
    microseconds = v.microsecond
    return 's' + _pack_int(seconds + microseconds, neg=neg)[1:]

def _pack_none(v, case_sensitive=True, neg=False):
    # We will differentiate None from the lack of a value.
    return 'z' if neg else 'a'

def _pack_sequence(seq, case_sensitive=True, neg=False):
    out = []
    for v in seq:
        out.append(PACK_TABLE[type(v)](v, case_sensitive=case_sensitive, neg=neg))
    ts = type(seq)
    if ts is set:
        return list(ts)
    return ts(out)

PACK_TABLE = {
    int: _pack_int,
    long: _pack_int,
    float: _pack_float,
    decimal: _pack_decimal,
    str: _pack_data,
    unicode: _pack_data,
    datetime:_pack_datetime,
    date:_pack_date,
    time:_pack_time,
    type(None): _pack_none,
    tuple: _pack_sequence,
    list: _pack_sequence,
    set: _pack_sequence,
}

def pack(v, case_sensitive=True, neg=False, _type=type, _table=PACK_TABLE):
    return _table[_type(v)](v, case_sensitive=case_sensitive, neg=neg)

DISCARD = object()
TRUNCATE = object()
FAIL = object()

def generate_index_rows(data, index_dict, _pack=pack, **kwargs):
    # We need to generate the index rows for the given set of indexes and the
    # data provided.
    max_row_count = min(kwargs.pop('max_row_count', 100), 10000)
    max_row_len = min(kwargs.pop('max_row_len', 256), 4096)
    row_over_count = kwargs.pop('row_over_count', FAIL)
    row_over_size = kwargs.pop('row_over_size', TRUNCATE)

    _seqs = (tuple, list)
    cache = {}
    index_rows = []
    index_row_count = 0
    usable_indexes = []

    for cols, iid in index_dict.iteritems():
        index_cols = [(pack(iid)[1:],)]
        for col in cols.rstrip(',').split(','):
            cname = col.strip('-')
            if col not in cache:
                cache[col] = _pack(data.get(cname), case_sensitive=col.endswith('-'), neg=col.startswith('-'))
                cc = cache[col]
                # We convert everything into a sequence, so we can let
                # itertools.product() do the cartesian product of all of them,
                # which is necessary for proper list indexing.
                if cc is None:
                    cache[col] = ()
                elif not isinstance(cc, _seqs):
                    cache[col] = (cc,)
                else:
                    cache[col] = [i for i in cc if i]
            if not cache[col]:
                break
            index_cols.append(cache[col])
        else:
            # save the references for actual creation later
            usable_indexes.append(index_cols)
            cnt = 1
            for col_data in index_cols:
                cnt *= len(col_data)
            index_row_count += cnt

    if index_row_count > max_row_count and row_over_count is FAIL:
        raise sq_exceptions.TooManyIndexRows("Index row count %i exceeds maximum count %i"%(index_row_count, max_row_count))

    for data in usable_indexes:
        # we have all non-nulls and all non-empty lists
        for irow in itertools.product(*data):
            if len(index_rows) == max_row_count:
                break
            row = ''.join(irow)
            lir = len(irow[0])
            rowlen = len(row) - lir
            if rowlen > max_row_len:
                if row_over_size is DISCARD:
                    continue
                elif row_over_size is TRUNCATE:
                    row = row[:max_row_len+lir]
                else:
                    raise sq_exceptions.IndexRowTooLong("Index row with length %i > maximum length %i"%(rowlen, max_row_len))

            index_rows.append(buffer(row))

        else:
            # can still generate rows
            continue
        # we can no longer generate any more index rows
        break
    return index_row_count, index_rows
