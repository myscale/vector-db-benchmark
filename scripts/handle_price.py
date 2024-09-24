import json
from common import walk_result_file_paths

for file in walk_result_file_paths(root_path="../results"):
    with open(file, 'r') as f:
        data = json.load(f)
    data['meta']['monthly_cost'] = 208.82

    with open(file, 'w') as f:
        json.dump(data, f, indent=2)
