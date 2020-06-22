import apache_beam as beam
import re
p1 = beam.Pipeline()


INPUT_DIR='data/wordcount_data.txt'
OUTPUT_DIR='output/output'

wordcount = (
    p1
    | beam.io.ReadFromText(INPUT_DIR)
    | 'Find words' >> beam.FlatMap(lambda line: re.findall(r"[a-zA-Z']+", line))
    | 'Pair words with 1' >> beam.Map(lambda word: (word, 1))
    | 'Group and sum' >> beam.CombinePerKey(sum)
    | beam.io.WriteToText(OUTPUT_DIR)
)
p1.run()
