# -*- coding: utf-8 -*-
#
# lhq
#


import urllib
import urllib2
from retry import retry


class HttpRequest:
    def __init__(self, url):
        self._url = url

    @retry(tries=5, delay=1)
    def post(self, data={}):
        req = urllib2.Request(
            url=self._url,
            data=urllib.urlencode(data)
        )
        return urllib2.urlopen(req).read()

    @retry(tries=5, delay=1)
    def get(self):
        req = urllib2.Request(url=self._url)
        return urllib2.urlopen(req).read()


if __name__ == '__main__':
    import unittest

    class MyTest(unittest.TestCase):
        def test_get(self):
            self.assertIsNotNone(HttpRequest("http://api.finance.ifeng.com/akdaily/?code=sh601633&type=last").get())

        def test_post_without_data(self):
            self.assertIsNotNone(HttpRequest("http://api.finance.ifeng.com/akdaily/?code=sh601633&type=last").post())

        def test_post_with_data(self):
            self.assertIsNotNone(
                HttpRequest("http://api.finance.ifeng.com/akdaily/?code=sh601633&type=last").post({"unittest": "haha"})
            )

    unittest.main()
