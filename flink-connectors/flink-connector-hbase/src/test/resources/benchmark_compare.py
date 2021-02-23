import happybase
connection = happybase.Connection(host='127.0.0.1', port=9090, autoconnect=True)
table1 = connection.table('latency-in')
table2 = connection.table('latency-out')
table1_dic = {}
table2_dic = {}


result = table1.scan(include_timestamp=True)
for key, data in result:
    table1_dic[key] = data

result = table2.scan(include_timestamp=True)
for key, data in result:
    table2_dic[key] = data


f = open("results_complete.txt", "w")
g = open("only_timestamp_differences.txt", "w")


for key,value in table1_dic.items():
	timestamp1 = list(value.values())[0][1]
	timestamp2 = None
	diff = None

	print(key)
	if key in table2_dic:
		timestamp2 = list(table2_dic[key].values())[0][1]
		diff = timestamp2 - timestamp1
	f.write(str(key) + '\t' + str(timestamp1) + '\t' + str(timestamp2) + '\t' + str(diff) + '\n')
	g.write(str(diff) + '\n')