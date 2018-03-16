import mrjob
from mrjob.job import MRJob
import datetime
import geopy
from geopy.distance import great_circle



class DistanceCalculate(MRJob):
    OUTPUT_PROTOCOL = mrjob.protocol.RawProtocol

    def mapper(self,_,line):

        data = line.split(' ')

        if len(data) != 5 :

            cabid , lat , long , occupancy , timestamp = data

            yield  cabid , [lat , long , occupancy, int(timestamp)]


    def reducer(self, key, values):


        prev_occ , prev_lat , prev_long =0, 0.0 , 0.0

        values = list(values)

        values.sort(key = lambda x : x[3])


        for i , value in enumerate(values):

            curr_lat , curr_long  = float (value[0]) , float(value[1])

            curr_occ = value[2]

            if i != 0 :

                distance = great_circle((prev_lat,prev_long),(curr_lat,curr_long)).miles
            else:
                distance = 0
            prev_lat = curr_lat
            prev_long = curr_long
            output = ' '.join([str(value[0])  , str(value[1]) , str(value[2]) , str(value[3]) ])

            yield key , output

if __name__ == '__main__' :
    DistanceCalculate.run()

