class Config(object):
    db_type = None
    db_connection = None


class DevConfigWinLocal(Config):
    db_type = 'postgres'
    db_connection = 'localhost'


config = DevConfigWinLocal()
