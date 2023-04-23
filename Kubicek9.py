from abc import ABC

from mrjob.job import MRJob
from mrjob.step import MRStep


# WHERE clause applied as condition in mapper to emit
#   WHERE yearid > 1900
# GROUP BY is done by the "shuffle" with the attribute as the key
#   GROUP BY playerid
# SELECT clause is handled by the last reducer
#   SELECT playerid, 3*(SUM(p_H)+SUM(p_BB))/SUM(p_IPOUTS) as whip
# HAVING clause handled by reducer that does group by
#   HAVING SUM(p_W) >= 300


class MRAssignment9(MRJob, ABC):

    def mapper(self, _, line):
        l = line.split(",")
        if int(l[1]) > 1900:
            yield l[0], l[1:]

    def reducer(self, key, values):
        sum_h = 0
        sum_bb = 0
        sum_ipouts = 0
        sum_w = 0
        for v in values:
            sum_w += v[5]
            sum_h += v[13]
            sum_bb += v[16]
            sum_ipouts += v[12]
        whip = (3 * (sum_bb + sum_h)) / sum_ipouts
        if sum_w >= 300:
            yield key, whip


if __name__ == '__main__':
    MRAssignment9.run()
