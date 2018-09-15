from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark import AccumulatorParam
from collections import OrderedDict

class Last1000Queue(AccumulatorParam):
	def zero(self, value):
		return []
	
	def addInPlace(self, list1, list2):
		list1.extend(list2)
		if len(list1)>1000:
			return list1[-1000:]
		else:
			return list1

def count_words(stream):
	bitstream = [int(i) for i in stream.collect()]
	global most_recent_1000
	global dgim_indices
	most_recent_1000 += bitstream
	number_of_bits_in_current_batch = len(bitstream)

	for key in sorted(dgim_indices.keys()):
		dgim_indices[key] = [i + number_of_bits_in_current_batch for i in dgim_indices[key] if i + number_of_bits_in_current_batch < 1000]

	for bit_position, bit in enumerate(bitstream):
		bit_position = number_of_bits_in_current_batch - 1 - bit_position
		if bit == 1:
			if 1 in dgim_indices.keys():
				dgim_indices[1].append(bit_position)
			else:
				dgim_indices[1] = [bit_position]
			for index in sorted(dgim_indices.keys()):
				current_vals = dgim_indices.get(index)
				if len(current_vals) > 2:
					if not dgim_indices.get(index + 1):
						dgim_indices[index + 1] = [current_vals[1]]
					else:
						dgim_indices[index + 1].append(current_vals[1])
					dgim_indices[index] = [current_vals[2]]

	estimated_count = 0
	if dgim_indices.keys() and len(most_recent_1000.value) == 1000:
		keys = sorted(dgim_indices.keys())
		for key in keys[:-1]:
			estimated_count += 2**(key-1) * len(dgim_indices.get(key))

		key = keys[-1]
		if len(dgim_indices.get(key)) > 1:
			estimated_count += 2**(key-1) + 2**(key-1)/2
		else:
			estimated_count += 2**(key-1)/2

		print("Estimated number of ones in the last 1000 bits: {}".format(estimated_count))
		print("Actual number of ones in the last 1000 bits: {}\n\n".format(most_recent_1000.value.count(1)))

if __name__ == "__main__":
	sc = SparkContext(appName="Vishal_Seshagiri_DGIMAlgorithm")
	sc.setLogLevel(logLevel="OFF")
	ssc = StreamingContext(sc, 10)
	stream = ssc.socketTextStream("localhost", 9999)

	most_recent_1000 = sc.accumulator([], Last1000Queue())

	dgim_indices = {}

	stream.foreachRDD(count_words)

	ssc.start()
	ssc.awaitTermination()