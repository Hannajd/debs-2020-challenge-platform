from controller import app
import unittest
import json
from flask import jsonify
import datetime

class TestMyAPI(unittest.TestCase):
    def test_unknown_route(self):
        with app.test_client() as c:
            response = c.get('/some/path/that/exists')
            self.assertEqual(response.status_code, 404)
    def test_result_route(self):
        with app.test_client() as c:
            response = c.get('/result')
            self.assertEqual(response.status_code, 405 or 404)

    def test_result_route_with_empty_post(self):
        with app.test_client() as c:
            response = c.post('/result',
                        data=json.dumps(dict(foo='bar')),
                        content_type='application/json')
            self.assertEqual(response.status_code, 400)
            #print("response", response)

    def test_schedule_route_with_empty_post(self):
        with app.test_client() as c:
            response = c.post('/schedule',
                        data=json.dumps(dict()),
                        content_type='application/json')
            self.assertEqual(response.status_code, 400)

    def test_schedule_route(self):

        #json.JSONEncoder.default = lambda self,obj: (obj.isoformat() if isinstance(obj, datetime.datetime) else None)

        with app.test_client() as c:
            response = c.get('/schedule')
            self.assertEqual(response.status_code, 200)
            print("response", response)
            body = json.loads(response.data)
            self.assertNotEqual(body, None)
            for k, v in body.items():
                self.assertEqual(v, 'updated' or 'old')


def main():
    unittest.main()

if __name__ == "__main__":
    main()
