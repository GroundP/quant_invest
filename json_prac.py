import json

with open('telepot.json') as file:
    data = json.load(file)
    
print(data["api_key"])    
print(data["id"])    