# Alex Kubicek
# Assignment 9

from abc import ABC
from mrjob.job import MRJob


class MRAssignment9(MRJob, ABC):

    def mapper(self, _, line):
        l = line.split(",")
        playerid = str(l[0])
        yearid = l[1]
        w = l[5]
        h = l[13]
        bb = l[16]
        ipouts = l[12]
        if yearid.isnumeric() and int(yearid) > 1900:
            yield playerid, [w, h, bb, ipouts]

    def reducer(self, key, values):
        sum_h = 0
        sum_bb = 0
        sum_ipouts = 0
        sum_w = 0
        for v in values:
            sum_w += int(v[0])
            sum_h += int(v[1])
            sum_bb += int(v[2])
            sum_ipouts += int(v[3])
        if sum_ipouts > 0:
            whip = (3 * (sum_bb + sum_h)) / sum_ipouts
            if sum_w >= 300:
                yield key, whip


MRAssignment9.run()
