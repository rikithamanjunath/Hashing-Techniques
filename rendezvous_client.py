# there is a csv file
# Given by client (CSV parser.py)
# from each row -->  key : split[0] +split[1] +split[2]
#                    value : info (each  row line)

import requests


import json
import hashlib
import requests
import sys
import re
from CSVparser import CSVparser
import pprint

nodesList = ['localhost:5000' ,'localhost:5001','localhost:5002','localhost:5003']
# nodesList = ['http://localhost:5000','http://localhost:5001','http://localhost:5002','http://localhost:5003']

class Rendezvous():

    def getmdHash(self, value: str):
        #  not to consider comma inside " "
        value = re.sub(r'(?!(([^"]*"){2})*[^"]*$),', '', value)
        split = value.split(',')
        res = split[0] + split[1] + split[3]
        return res

    # get highest weight node
    def getNode(self,key):
        highest_node = None
        highestValue = 0
        for each in nodesList:
            x = each + key
            hash_int = int((hashlib.md5(x.encode())).hexdigest(), 16)
            if (hash_int > highestValue):
                highestValue = hash_int
                highest_node = each
        return highest_node

    def getServer(self,node):
        return 'http://'+node

    # send post call to the server having highest random weight
    def sendPostCall(self, input_str: str):
        key = self.getmdHash(row)
        node = self.getNode(key)
        server = self.getServer(node)
        post_url = server + '/api/v1/entries'
        putKey = int(hashlib.md5(key.encode()).hexdigest(), 16)
        payload = {'key': putKey, 'value': input_str}
        r = requests.post(post_url, data=json.dumps(payload), headers={"Content-Type": "application/json"})
        return r.status_code;

    def getEntries(self):
        for node in nodesList:
            print("\n")
            server = self.getServer(node)
            res = requests.get(server + '/api/v1/entries')
            print("GET " + server)
            output = res.text
            pp = pprint.PrettyPrinter(indent=4)
            pp.pprint(output)


if __name__ == '__main__':

    if len(sys.argv) != 2:
        print("Give correct arguments")
        sys.exit(1)

    filename = sys.argv[1]
    obj = CSVparser(filename)
    list = obj.process(filename)
    c = Rendezvous()
    count = 0
    for row in list:
        new_row = row[:-1]
        c.sendPostCall(new_row)
        count = count+1
    print("Uploaded all " + str(count) + " entries")
    print("Verifying the data.")
    c.getEntries()
