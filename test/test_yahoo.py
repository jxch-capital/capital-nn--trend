import datadownload.yahoo as yahoo
import utils.socket_util as socket_util


socket_util.set_proxy()
yahoo.download()

