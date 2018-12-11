from dal import get
entries = get('samkeppni_entries', ('id', 'name', 'date'), 'id=1000')
print(entries)
