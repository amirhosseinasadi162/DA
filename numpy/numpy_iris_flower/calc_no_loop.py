# coding: utf-8
import numpy as np
def calc_no_loop(new_points, points):
    return np.sum(np.square((np.array(new_points[:, np.newaxis]))-(np.array(points))),axis=2)
