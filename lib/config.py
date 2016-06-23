from os.path import abspath, realpath, dirname, join
import ConfigParser

here = realpath(dirname(__file__))
config = ConfigParser.ConfigParser()
config.read(abspath(join(here, '../config/attribution.ini')))
