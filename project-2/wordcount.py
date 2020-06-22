import apache_beam as beam
import re
p1 = beam.Pipeline()

wordcount = (
    p1
    | beam.io.ReadFromText('wordcount_data.txt')
    | 'Find words' >> beam.FlatMap(lambda line: re.findall(r"[a-zA-Z']+", line))
    | 'Pair words with 1' >> beam.Map(lambda word: (word, 1))
    | 'Group and sum' >> beam.CombinePerKey(sum)
    | beam.io.WriteToText('output/output')
)
p1.run()
