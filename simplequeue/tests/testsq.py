import unittest
import sys
sys.path.append("/Users/abhishek.p/code/personal/job-queue/lib/")
from simplequeue import sq
from unittest.mock import MagicMock
from unittest.mock import patch
from unittest.mock import create_autospec


class TestSQ(unittest.TestCase):

    def test_spawn_workers(self):
        with patch.object(sq.SQ, '__spawn_workers_in_new_process', return_value=None) as mock_method:
                testqueue = sq.SQ("test", "redis")
                # testqueue.__spawn_workers_in_new_process = MagicMock(return_value=None)
                # testqueue.__spawn_workers_in_same_process = MagicMock(return_value=None)
                testqueue.spawn_workers()
                mock_method.assert_called_once_with(testqueue)

