
from __future__ import division

'''
This lisp interpreter started out as the version that Peter Norvig called
"lis.py".  Changes include but are not limited to...

* Using a dictionary instead of if/elif to improve eval() performance.
* List -> deque in tokenize (useful for long scripts).
* Added the tail-recursion, expand, and Procedure stuff from lispy.py .
* Added def? .
* Added some loadable modules.
* Added the ability to run a script and interact with outside values via
  dotted names.
* The basic math operations can now support more than 2 arguments.
* Fixed a bug with expansion when using let.
* Added letrec (which is really what "let" should have been, semantically).
* Added a fake quasiquote for strings (everything else can use 'quote').


'''


################ Lispy: Scheme Interpreter in Python

## (c) Peter Norvig, 2010; See http://norvig.com/lispy.html
## (c) Peter Norvig, 2010; See http://norvig.com/lispy2.html

################ Symbol, Procedure, Env classes

import datetime as dt
from decimal import Decimal as decimal
from collections import defaultdict, deque
import json
import math
import operator as op
import re
import time

default = object()
isa = isinstance

class Symbol(str):
    pass

class Env(dict):
    "An environment: a dict of {'var':val} pairs, with an outer Env."
    def __init__(self, parms=(), args=(), outer=None):
        # Bind parm list to corresponding args, or single parm to list of args
        self.outer = outer
        if isa(parms, Symbol):
            self.update({parms:list(args)})
        else:
            if len(args) != len(parms):
                raise TypeError('expected %r, given %r, '
                                % (parms, args))
            self.update(zip(parms,args))
    def find(self, var):
        "Find the innermost Env where var appears."
        if var in self:
            return self
        elif self.outer is None:
            raise LookupError(var)
        else:
            return self.outer.find(var)
    def defined(self, var):
        try:
            self.find(var)
        except LookupError:
            return False
        return True
    def set(self, var, val):
        self[var] = val

class Procedure(object):
    "A user-defined Scheme procedure."
    def __init__(self, parms, exp, env):
        self.parms, self.exp, self.env = parms, exp, env
    def __call__(self, *args):
        if len(args) != len(self.parms):
            raise TypeError
        return eval(self.exp, Env(self.parms, args, self.env))

def tc(cls):
    def check(value):
        return isinstance(value, cls)
    return check

types = {
    'set':lambda *x: set(x), 'dict':lambda *x: dict(x),'time':dt.time,
    'timedelta':dt.timedelta, 'datetime':dt.datetime, 'date':dt.date,
    'decimal':decimal, 'set?':tc(set), 'dict?':tc(dict), 'time?':tc(dt.time),
    'timedelta?':tc(dt.timedelta), 'datetime?':tc(dt.datetime),
    'date?':tc(dt.date), 'decimal?':tc(decimal),

}
LOADABLE = {
    'types': types,
    'math': dict((k,v) for k,v in vars(math).iteritems() if k[:1] != '_'),
    'ops': dict((k,v) for k,v in vars(op).iteritems() if k[:1] != '_'),
}

def load(env, *names):
    for name in names:
        if name in LOADABLE:
            env.update(LOADABLE[name])

def compose(op, initial=default):
    def fcn(*args):
        if initial is not default:
            return reduce(op, args, initial)
        return reduce(op, args)
    return fcn

def _and(*args):
    a = True
    for a in args:
        if not a:
            return False
    return a

def _or(*args):
    a = False
    for a in args:
        if a:
            return a
    return a

def add_globals(env):
    "Add some Scheme standard procedures to an environment."
    c = compose
    env.update({'#t':True, '#f':False, '+':c(op.add), '-':c(op.sub),
        '*':c(op.mul), '/':c(op.div), 'not':op.not_, '>':op.gt, '<':op.lt,
        '%':op.mod, '^':c(op.xor), '<<':op.lshift, '>>':op.rshift,
        '>=':op.ge, '<=':op.le, '=':op.eq, 'equal?':op.eq, 'eq?':op.is_,
        'length':len, 'cons':lambda x,y:[x]+y, 'car':lambda x:x[0],
        'cdr':lambda x:x[1:], 'cadr':lambda x:x[1], 'append':op.add,
        'list':lambda *x:list(x), 'list?': tc(list), 'null?':lambda x:x==[],
        'symbol?':tc(Symbol), 'bool?':tc(bool), 'int?':tc(int),
        'float?':tc(float), 'abs':abs, 'and': _and, 'or':_or, 'max':c(max),
        'min':c(min), 'now':time.time})
    return env

global_env = add_globals(Env())

################ eval
ops = {
    'load': load,
    'quote': lambda env, exp: exp,
    'if': lambda env, test, conseq, alt: (env, (conseq if eval(test, env) else alt)),
    'set!': lambda env, var, exp: env.find(var).set(var, eval(exp, env)),
    'define': lambda env, var, exp: env.set(var, eval(exp, env)),
    'def?': lambda env, var: env.defined(var),
    'undef': lambda env, var: env.find(var).pop(var),
    'lambda': lambda env, vars, exp: Procedure(vars, exp, env),
    'begin': lambda env, *exp: (env, ([eval(ex, env) for ex in exp[:-1]], exp[-1])[-1]),
    '\0env': lambda env, exp: (Env(outer=env), exp),
    'method': (lambda env, var, name, *exp:
        getattr(env.find(var)[var], name)(*[eval(ex, env) for ex in exp])),
}
tail_call = set(['if', 'begin', '\0env'])

def eval(x, env=global_env):
    "Evaluate an expression in an environment."
    while True:
        tx = type(x)
        if tx is Symbol:             # variable reference
            return env.find(x)[x]
        elif tx is not list:         # constant literal
            return x
        opname = x[0]
        try:
            op = ops.get(opname, None)
        except TypeError:
            op = None
        if op:
            x = op(env, *x[1:])
            if opname in tail_call:
                env, x = x
                continue
            return x
        else:                          # (proc exp*)
            exps = [eval(exp, env) for exp in x]
            proc = exps.pop(0)
            if type(proc) is Procedure:
                x = proc.exp
                env = Env(proc.parms, exps, proc.env)
                continue
            return proc(*exps)

################ parse, read, and user interaction
_SAW = defaultdict(int)
def expand(x, toplevel=False):
    "Walk tree of x, making optimizations/fixes, and signaling SyntaxError."
    isa = isinstance
    require(x, x!=[])                    # () => Error
    if not isa(x, list):                 # constant => unchanged
        return x
    x0 = x[0]
    _lambda, _set, _define, _begin = map(Symbol, 'lambda set! define begin'.split())
    try:
        _SAW[x0] += 1
    except:
        pass
    # reorganized based on expected code structures.
    if x0 == 'define':
        require(x, len(x)>=3)
        _def, v, body = x0, x[1], x[2:]
        if isa(v, list) and v:           # (define (f args) body)
            f, args = v[0], v[1:]        #  => (define f (lambda (args) body))
            return expand([_def, f, [_lambda, args]+body])
        else:
            require(x, len(x)==3)        # (define non-var/list exp) => Error
            require(x, isa(v, Symbol), "can define only a symbol")
            exp = expand(x[2])
            return [_define, v, exp]
    elif x0 == 'lambda':                # (lambda (x) e1 e2)
        require(x, len(x)>=3)            #  => (lambda (x) (begin e1 e2))
        vars, body = x[1], x[2:]
        require(x, (isa(vars, list) and all(isa(v, Symbol) for v in vars))
                or isa(vars, Symbol), "illegal lambda argument list")
        exp = body[0] if len(body) == 1 else [_begin] + body
        return [_lambda, vars, expand(exp)]
    elif x0 == 'if':
        if len(x)==3: x = x + [None]     # (if t c) => (if t c None)
        require(x, len(x)==4)
        return map(expand, x)
    elif x0 == 'quote':                 # (quote exp)
        require(x, len(x)==2)
        return x
    elif x0 == 'set!':
        require(x, len(x)==3);
        var = x[1]                       # (set! non-var exp) => Error
        require(x, isa(var, Symbol), "can set! only a symbol")
        return [_set, var, expand(x[2])]
    elif x0 == 'begin':
        if len(x) == 1: return None        # (begin) => None
        else: return [expand(xi, toplevel) for xi in x]
    elif x0 == 'load':
        require(x, len(x) > 1)
        for name in x[1:]:
            require(x, name in LOADABLE, "bad module %s"%(name,))
        return x
    elif x0 == 'let':
        require(x, len(x) == 3)
        args = x[1:]
        bindings, body = args[0], args[1:]
        require(x, all(isa(b, list) and len(b)==2 and isa(b[0], Symbol)
                       for b in bindings), "illegal binding list")
        vars, vals = zip(*bindings)
        return [[_lambda, list(vars)] + map(expand, body)] + map(expand, list(vals))
    elif x[0] == 'letrec':
        require(x, len(x) == 3)
        bindings = x[1]
        body = x[2:]
        for b in bindings:
            b.insert(0, 'define')
        return ['\0env', map(expand, ['begin'] + bindings + body)]
    else:
        try:
            _SAW.pop(x0, None)
        except:
            pass
        return map(expand, x)            # (f arg...) => expand each

def require(x, predicate, msg="wrong length"):
    "Signal a syntax error if predicate is false."
    if not predicate:
        raise SyntaxError(str(x)+': '+msg)

def tokenize(s):
    "Convert a string into a deque of tokens."
    return deque(s.replace('(',' ( ').replace(')',' ) ').split())

def read_from(tokens):
    "Read an expression from a sequence of tokens."
    if not tokens:
        raise SyntaxError('unexpected EOF while reading')
    token = tokens.popleft()
    if '(' == token:
        L = []
        while tokens[0] != ')':
            L.append(read_from(tokens))
        tokens.popleft() # pop off ')'
        return L
    elif ')' == token:
        raise SyntaxError('unexpected )')
    else:
        return atom(token)

def parse(s):
    "Read a Scheme expression from a string."
    return expand(read_from(tokenize(s)), True)

def quote_literal(s):
    if s.startswith("'") or s.startswith('`'):
        return s[1:]
    raise ValueError

def string_literal(s):
    try:
        s = json.loads(s)
    except:
        raise ValueError
    if not isinstance(s, (str, unicode)):
        raise ValueError
    return s

def atom(token):
    "Numbers become numbers; some strings are strings, all else are symbols."
    for typ in (int, float, string_literal, quote_literal, Symbol):
        try:
            r = typ(token)
            return r
        except ValueError:
            continue

name_re = re.compile('^[a-z_][a-z0-9_]*(?:\.[a-z_][a-z0-9_]*)*$')
def _splitname(name):
    if not name_re.match(name):
        raise NameError("Bad name: %r"%name,)
    return name.split('.')

def _get(doc, name, default=None):
    for subn in _splitname(name):
        if subn in doc:
            doc = doc[subn]
        else:
            return default
    return doc

def _set(doc, name, value):
    name = _splitname(name)
    for subn in name[:-1]:
        if subn not in doc:
            doc[subn] = {}
        doc = doc[subn]
    doc[name[-1]] = value

def _del(doc, name):
    name = _splitname(name)
    for subn in name[:-1]:
        if subn not in doc:
            return
        doc = doc[subn]
    doc.pop(name[-1], None)

def run_script(script, doc, shared, timeit=False, exc_env=False):
    tokens = tokenize(script)
    env = Env(outer=global_env)
    d = {'doc':doc, 'shared':shared}
    ops['getv'] = lambda env, s, name, default=None: _get(d[eval(s,env)], eval(name,env), eval(default, env) if default is not None else default)
    ops['setv'] = lambda env, s, name, value: _set(d[eval(s,env)], eval(name,env), eval(value, env))
    ops['delv'] = lambda env, s, name: _del(d[eval(s,env)], eval(name,env))
    while tokens:
        expression = expand(read_from(tokens), True)
        try:
            eval(expression, env)
        except:
            if exc_env:
                print env
            raise
    if timeit:
        passes = 2
        dt = 0
        while dt < 1:
            passes = int(passes * 1.5)
            t = time.time()
            for i in xrange(passes):
                eval(expression, env)
            dt = time.time()-t
        print "%i passes in %.3fs"%(passes, dt)
    # any desired changes should have used setv/store
