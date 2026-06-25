import json
with open('data/tiki_category.json', 'r', encoding='utf-8') as f:
    data = json.load(f)
with open('cats.txt', 'w', encoding='utf-8') as f:
    for c in data.get('data', []):
        f.write(f"{c['id']}: {c['name']}\n")
