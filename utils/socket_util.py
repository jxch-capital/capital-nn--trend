import socket
import socks


def set_proxy(host="localhost", port=10808):
    socks.setdefaultproxy(socks.SOCKS5, host, port)
    socket.socket = socks.socksocket
