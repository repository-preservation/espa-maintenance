import socket

#directory under which tm, etm and nlaps directories can be found
base_dir = '/data1/standard_l1t'

#ip address that the server should be bound to.  Hostname works too
ip_address = socket.getfqdn()

#port to listen on
port = 50000

#this may be either 'thread', 'fork' or 'single', default is 'thread'
multiprocess_model = 'thread'

#replace this with the clients that should be allowed to access the cache
authorized_clients = [
'152.61.43.84',
'152.61.128.117',
'152.61.84.12'
]

#True/False
run_as_daemon = True

#controls the name of this server per the HTTP spec (will show up in security scans)
server_name = "Yeah, Magnets! v148, 3-to-the-3-to-the-6-to-the-9"

#log message on server start
startup_message = 'Starting LSRD Scene Cache RPC Server on %s:%s...' % (ip_address, port)

