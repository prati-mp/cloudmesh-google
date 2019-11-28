class Manager(object):
    """
        storage [--storage=SERVICE] create dir DIRECTORY
        storage [--storage=SERVICE] get SOURCE DESTINATION [--recursive]
        storage [--storage=SERVICE] put SOURCE DESTINATION [--recursive]
        storage [--storage=SERVICE] list SOURCE [--recursive] [--output=OUTPUT]
        storage [--storage=SERVICE] delete SOURCE
        storage [--storage=SERVICE] search  DIRECTORY FILENAME [--recursive] [--output=OUTPUT]
        storage [--storage=SERVICE] sync SOURCE DESTINATION [--name=NAME] [--async]
        storage [--storage=SERVICE] sync status [--name=NAME]
        storage config list [--output=OUTPUT]
    """

    def __init__(self):
        print("init {name}".format(name=self.__class__.__name__))

    def list(self, parameter):
        print("list", parameter)
