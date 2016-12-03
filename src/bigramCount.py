from pyspark import SparkConf, SparkContext
import re

conf = SparkConf().setAppName("building a warehouse")
sc = SparkContext(conf=conf)

def removePunctuation(text):
    text = re.sub("[^0-9a-zA-Z ]", " ", text)
    return text

textRDD = sc.newAPIHadoopFile('text.txt',
                              'org.apache.hadoop.mapreduce.lib.input.TextInputFormat',
                              'org.apache.hadoop.io.LongWritable',
                              'org.apache.hadoop.io.Text',
                               conf={'textinputformat.record.delimiter': "\r\n\r\n"}) \
            .map(lambda x: x[1])
sentences = textRDD.flatMap(lambda x : x.split(". "))
sentences = sentences.map(removePunctuation).map(lambda x : x.lower())

def bigram(words):
	bigrams = []
	for i in range(len(words) - 1):
		bigrams.append((tuple(words[i:i+2]), 1))
	return bigrams


bigrams = sentences.map(lambda s : s.split()).flatMap(bigram)
freq_bigrams = bigrams.reduceByKey(lambda x, y: x + y).map(lambda x : (x[1], x[0])).sortByKey(False)

freq_bigrams_lst = freq_bigrams.collect()

total_num = 0
for (count, bigram) in freq_bigrams_lst:
	total_num += count

top_ten_percent_count = 0
top_ten_percent_num = total_num / 10.0
for (count, bigram) in freq_bigrams_lst:
	if top_ten_percent_num > 0:
		top_ten_percent_count += 1
		top_ten_percent_num -= count
	else:
		break

with open('result.txt', 'w') as fout:
	fout.write("the total number of bigrams: %d\n" % total_num)
	fout.write("the most common bigram: \"%s\"\n" % " ".join(freq_bigrams_lst[0][1]))
	fout.write("the number of bigrams required to add up to 10%% of all bigrams: %d\n" %top_ten_percent_count)
