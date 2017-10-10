# -*- coding: utf-8 -*-
#
# lhq
#


import StringIO
import logging
import traceback


def trace_log():
    fp = StringIO.StringIO()
    traceback.print_exc(file=fp)
    logging.error(fp.getvalue())


def trace_msg():
    fp = StringIO.StringIO()
    traceback.print_exc(file=fp)
    return fp.getvalue()


if __name__ == '__main__':
    import unittest
    from testException import TestException


    class MyTest(unittest.TestCase):
        def test_trace_log(self):
            try:
                raise TestException("test")
            except TestException:
                self.assertIsNone(trace_log())

        def test_trace_msg(self):
            try:
                raise TestException("test")
            except TestException:
                self.assertIsNotNone(trace_msg())

    unittest.main()
