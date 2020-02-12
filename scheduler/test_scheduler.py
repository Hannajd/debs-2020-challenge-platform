import unittest
from scheduler import *


class SchedulerTest(unittest.TestCase):

    def setUp(self):
        self.scheduler = Scheduler()

    def test_initialze_scheduler(self):
        self.assertTrue(self.scheduler.schedule)
        self.assertFalse(self.scheduler.last_updated_images)

    def test_reguest_all_images(self):
        _, status = self.scheduler.reguest_all_images()
        self.assertTrue(status, 200)

    def test_post(self):
        status = post_result({'test':1})
        self.assertTrue(status, 200 or 201)

    def test_run(self):
        self.scheduler.run()
        for i in self.scheduler.schedule.values():
            self.assertTrue(i, 'updated')


def main():
    unittest.main()

if __name__ == "__main__":
    main()
