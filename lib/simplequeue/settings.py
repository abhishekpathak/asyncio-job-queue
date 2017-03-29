backend = "redis"
name = "test"
queue = "%s.%s" % (backend, name)
LOG_LEVEL = "DEBUG"
LOG_FILE = "/tmp/simplequeue.log"
