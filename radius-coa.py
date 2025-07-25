#! /usr/bin/env python3

from pyrad.client import Client
from pyrad import dictionary
from pyrad import packet
import sys

if len(sys.argv) != 3:
  print ("usage: coa.py {coa|dis} daemon-1234")
  sys.exit(1)

#ADDRESS = "10.10.15.212"
ADDRESS = "10.10.1.12"
SECRET = b"123456"
ATTRIBUTES = {
    "Acct-Session-Id": "39566303"
}

ATTRIBUTES["NAS-Identifier"] = sys.argv[2]

ATTRIBUTES["ERX-Service-Deactivate"] = 'INET-FREEDOM'
ATTRIBUTES["ERX-Service-Activate:1"] = 'NOINET-NOMONEY()'

#ATTRIBUTES["ERX-Service-Deactivate"] = 'NOINET-NOMONEY'
#ATTRIBUTES["ERX-Service-Activate:1"] = 'INET-FREEDOM(10000)'

#ATTRIBUTES["ERX-Service-Activate:1"] = 'NOINET-NOVLAN()'
#ATTRIBUTES["ERX-Service-Deactivate"] = 'NOINET-NOVLAN'

# create coa client
client = Client(server=ADDRESS, secret=SECRET, dict=dictionary.Dictionary("dictionary.pyrad"))

# set coa timeout
client.timeout = 30

# create coa request packet
attributes = {k.replace("-", "_"): ATTRIBUTES[k] for k in ATTRIBUTES}

if sys.argv[1] == "coa":
    # create coa request
    print(attributes)
    request = client.CreateCoAPacket(**attributes)
elif sys.argv[1] == "dis":
    # create disconnect request
    request = client.CreateCoAPacket(code=packet.DisconnectRequest, **attributes)
else:
  sys.exit(1)

# send request
result = client.SendPacket(request)
print(result)
print(result.code)
