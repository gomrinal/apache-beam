import apache_beam as beam

p1 = beam.Pipeline()

wordcount = (
    p1
    | beam.io.ReadFromText('wordcount_data.txt')
    | beam.Map(lambda record: record.split())
    | beam.Map(lambda record: len(record))
    | beam.CombineGlobally(sum)
    | beam.io.WriteToText('output/output')
)
p1.run()
