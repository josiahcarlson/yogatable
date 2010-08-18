#!/usr/bin/env python

import os
import sys
import unittest

class TestSuite(unittest.TestSuite):
    '''
    There has to be a simpler way of doing this.
    '''
    def __init__(self, *args, **kwargs):
        super(TestSuite, self).__init__(*args, **kwargs)
        # find tests
        self.load_tests(os.path.dirname(__file__), 'tests', names=sys.argv[1:])

    def run(self, result):
        for testcase in self._tests:
            if result.shouldStop:
                break
            tests = self.get_tests(testcase)
            for test in tests:
                test(result)
        return result

    def get_tests(self, testcase):
        if hasattr(testcase, '_tests'):
            return testcase._tests
        return [testcase]

    def load_tests(self, path, tpath, names):
        for root, dirs, files in os.walk(os.path.join(path, tpath)):
            if '__init__.py' not in files:
                del dirs[:]
                continue
            name = root[len(path):].strip('/\\').replace('\\', '/').replace('/', '.')
            for fname in files:
                if not fname.endswith('.py') or not fname.startswith('test_') or fname.count('.') != 1:
                    continue
                mname = name + '.' + fname[:-3]
                module = __import__(mname, fromlist=['*'])
                if names:
                    for test_name in names:
                        try:
                            self.addTest(
                                unittest.defaultTestLoader.loadTestsFromName(test_name, module))
                        except AttributeError:
                            pass
                else:
                    tests = unittest.defaultTestLoader.loadTestsFromModule(module)
                    self.addTests(tests._tests)

if __name__ == '__main__':
    suite = TestSuite()
    unittest.TextTestRunner(verbosity=2).run(suite)
