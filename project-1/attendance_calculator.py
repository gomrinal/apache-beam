import apache_beam as beam

def SplitRow(element):
    return element.split(',')

def filtering(record):
  return record[3] == 'Accounts'

INPUT_DIR='data/attendance_data.txt'
OUTPUT_DIR='output/output'

p1 = beam.Pipeline()

attendance_count = (
    
   p1
    |beam.io.ReadFromText(INPUT_DIR)
    |beam.Map(SplitRow)
    |beam.Filter(filtering)
    |beam.Map(lambda record: (record[1], 1))
    |beam.CombinePerKey(sum)
    |beam.io.WriteToText(OUTPUT_DIR)
  
)

p1.run()