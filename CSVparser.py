
import requests
import json


class CSVparser:

    def __init__(self,url):
        self.url = url

    def readFile(self,fileName:str):
        f = open(fileName)
        next(f)
        yourList = f.readlines()
        return yourList

    def process(self,fileName:str):
        list = self.readFile(fileName)
        return list
        print("Finshed processing")


# if __name__ == '__main__':
#     url = 'http://127.0.0.1:5000'
#     fileName = 'causes-of-death.csv'
#
#     csvparser =  CSVparser(url)
#     csvparser.process(fileName)

# with open('causes-of-death.csv', mode='r') as csv_file:
#     csv_reader = csv.reader(csv_file)
#     line_count = 0
#     for row in csv_reader:
#             #row.replace('"', '')
#             print(row)
#             line_count += 1
#     print(f'Processed {line_count} lines.')