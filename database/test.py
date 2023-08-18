import csv
import json
import os.path

def read_csv(file_name: str) -> tuple:

    '''Reads the content of the specified CSV file.

    Arguments:
     - file_name (str): CSV file to be read

    Returns:
     - 'tuple(headers, file content [without headers])' in case of success
    '''

    file_content: list = []
    
    with open(file_name, newline='') as f:
        file_content = list(csv.reader(f, delimiter='\t'))

        return file_content[0], file_content[1:]


def csv_to_json(file_name: str) -> str:
    
    '''Converts CSV to JSON.

    Arguments:
     - file_name (str): CSV file to be read

    Returns:
     - 'json string' in case of success
    '''

    headers, file_content = read_csv(file_name)

    data: list = []
    d: dict = {}

    for row in file_content:
        for i, v in enumerate(row):
            d[headers[i]] = v

        data.append(d.copy())
        d.clear()

    return json.dumps(data, indent=4)


def import_tags(file_name: str):

    source_file_path, output_file_name = os.path.split(file_name)
    output_file_name = os.path.join(source_file_path, output_file_name.replace('.csv', '.json'))

    json_text = csv_to_json(file_name)
    
    with open(output_file_name, 'x') as o:
        o.write(json_text)

import_tags('./database/import_tags.csv')
