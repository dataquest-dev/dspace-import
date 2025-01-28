import os
import json

if __name__ == '__main__':
    # Load the JSON data from a file
    with open(os.path.join('data', 'metadata.json'), 'r', encoding='utf-8') as file:
        data = json.load(file)

    data_d = {}
    empty_d = {}
    # Filter
    for i in data['data']:
        for b in data['data'][i]['bitstreams']:
            data_d[b['uuid']] = b['sizeBytes']
            if b['sizeBytes'] == 0:
                empty_d[b['uuid']] = b['sizeBytes']

    # Save output
    with open(os.path.join('data', 'output.json'), 'w') as json_file:
        json.dump(data_d, json_file)
    with open(os.path.join('data', 'empty_output.json'), 'w') as json_file:
        json.dump(empty_d, json_file)
