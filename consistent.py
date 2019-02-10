import json
import hashlib
import requests
import sys
import re
from CSVparser import CSVparser
# import pprint
# from collections import OrderedDict

serverDict = {
                0 : 'http://localhost:5000',
                1 : 'http://localhost:5001',
                2 : 'http://localhost:5002',
                3 : 'http://localhost:5003'
     }
noOfServers = 4

class Consistent():


    def getmdHash(self, value: str):
        #  not to consider comma inside " "
        value = re.sub(r'(?!(([^"]*"){2})*[^"]*$),', '', value)
        split = value.split(',')
        res = split[0] + split[1] + split[3]
        hashKey = hashlib.md5(res.encode())
        # print(res + "  ---> " + hashKey.hexdigest() + "  ---> " + value)
        return hashKey


    def getBucketId(self, hash):
        bucketId = int(hash.hexdigest(),16)  % 360
        return (bucketId)

    def getNode(self, hashID):
        # considered as a ring (circle) of 360
        eachpart = 360 / noOfServers
        if(hashID  > 0 and hashID <= eachpart):
            node = 0
        elif(hashID  > eachpart and hashID <= eachpart*2):
            node =1
        elif(hashID  > eachpart*3 and hashID <= eachpart*4):
            node = 2
        else:
            node = 3
        return node

    def getServer(self, node):
        return serverDict.get(node)

    # post call to the correct server
    def sendPostCall(self, input_str: str):
        hashmd5 = self.getmdHash(row)
        bucketId = self.getBucketId(hashmd5)
        node = self.getNode(bucketId)
        server = self.getServer(node)
        post_url = server + '/api/v1/entries'
        payload = {'key': int(hashmd5.hexdigest(),16), 'value':input_str}
        r = requests.post(post_url, data=json.dumps(payload), headers={"Content-Type": "application/json"})
        return r.status_code;


    def getEntries(self ):
        i = 0
        for each in serverDict:
            print("\n")
            server = serverDict.get(i)
            res = requests.get(server+'/api/v1/entries')
            print("GET "+ server)
            output = res.text
            my_dict = json.loads(output)
            print(json.dumps(my_dict, indent=4))
            # pp = pprint.PrettyPrinter(indent=2,width=240)
            # pp.pprint(my_dict)

            i = i + 1

if __name__ == '__main__':

    if len(sys.argv) != 2:
        print("Give correct arguments")
        sys.exit(1)

    filename = sys.argv[1]
    obj = CSVparser(filename)
    list = obj.process(filename)
    c = Consistent()
    count =0
    for row in list:
        new_row = row[:-1]
        c.sendPostCall(new_row)
        count = count +1

    print("Uploaded all "+ str(count)+" entries")
    print("Verifying the data.")
    c.getEntries()




