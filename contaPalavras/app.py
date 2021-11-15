import sys
from pyspark import SparkContext, SparkConf

if __name__ == '__main__':

	# Criar o SparkContext
	conf = SparkConf().setAppName("Conta Palavras").setMaster("spark://192.168.1.106:7077")
	sc = SparkContext(conf = conf)

	# Carregar o arquivo
	palavras = sc.textFile("/tmp/input.txt").flatMap(lambda line : line.split(" "))

	# Conta ocrrenecia de palavras
	contagem = palavras.map(lambda palavra : (palavra, 1)).reduceByKey(lambda a, b: a + b)

	# Salvar o resultado
	contagem.saveAsTextFile("/tmp/saida")
