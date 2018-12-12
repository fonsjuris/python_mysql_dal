from dal import get, update
# entries = get('samkeppni_entries', ('id', 'name', 'date'), 'id=1000')

update('lawyer', ('lawyer_name',), ("Adolf Guðmundsson",), 'id=1')

# print(entries)
