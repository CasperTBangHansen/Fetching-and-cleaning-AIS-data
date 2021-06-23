#!/usr/bin/env python
"""
Finds the centroid of a polygon
"""
from typing import List
import numpy as np                  # type: ignore

def find_centroid(polygon : np.array) -> List[float]:
    """
    Finds the centroid of a polygon
    """
    x : float = 0
    y : float = 0
    length = len(polygon)
    signed_area : float = 0
    # For all vertices
    for i, points in enumerate(polygon):
        x_first = points[0]
        y_first = points[1]
        x_last = polygon[(i + 1) % length][0]
        y_last = polygon[(i + 1) % length][1]

        # Calculate value of A
        # using shoelace formula
        area_polygon = (x_first * y_last) - (x_last * y_first)
        signed_area += area_polygon

        # Calculating coordinates of
        # centroid of polygon
        x += (x_first + x_last) * area_polygon
        y += (y_first + y_last) * area_polygon

    signed_area *= 0.5
    x = x / (6 * signed_area)
    y = y / (6 * signed_area)
    return [x, y]
