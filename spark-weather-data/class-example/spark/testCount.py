wc = data.map(lambda line:line.split(" "));
wc.collect()


fm = data.flatMap(lambda line:line.split(" "));
fm.collect()

fm.map(lambda word : (word,1)).collect()

wc.flatMap(lambda word : (word,1)).collect()
