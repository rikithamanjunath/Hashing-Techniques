from flask import Flask
from flask_restful import Resource, Api
from flask import request,jsonify
import hashlib
import re
import sys

app = Flask(__name__)
api = Api(app)

# dont sort the json reply
app.config['JSON_SORT_KEYS'] = False


datastore = {}

class DataStoreAPI(Resource):

    def post(self):
        json_data = request.get_json(force=True)
        # if key given as xxxx (part1)
        if 'xxxx' in json_data:
            info = json_data['xxxx']
            hashKey = self.getHash(info)
            datastore[hashKey] = info
            return 201
        else :
            info = json_data['value']
            hashKey = json_data['key']
            datastore[hashKey] = info
            return 201

    def getHash(self, value: str):
        #  not to consider comma inside " "
        value = re.sub(r'(?!(([^"]*"){2})*[^"]*$),', '', value)
        split = value.split(',')
        res = split[0] + split[1] + split[3]
        print(res)
        hash = hashlib.md5(res.encode())
        hashID = int(hash.hexdigest(),16)
        return (hashID)

    def get(self):
        return jsonify({'numofentries': len(datastore),'entries':datastore})

api.add_resource(DataStoreAPI, '/api/v1/entries')


if __name__ == '__main__':
    print (sys.path)
    inputPort = sys.argv[1]
    app.run(host='0.0.0.0', port=inputPort)
    app.run(debug=True)




