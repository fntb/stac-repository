import random
import math
import numpy as np

import shapely


def generate_geometry():
    dx = 0.01
    dy = 0.01

    v0 = np.array([
        random.random() * (360 - dx) - (180 - dx / 2),
        random.random() * (180 - dy) - (90 - dy / 2),
    ])

    rotation_angle = random.random() * math.pi / 2
    rotation = np.array([
        [math.cos(rotation_angle), -math.sin(rotation_angle)],
        [math.sin(rotation_angle), math.cos(rotation_angle)]
    ])

    def to_coords(v: np.array):
        return (v[1], v[0])

    geometry = shapely.Polygon([
        to_coords(v0),
        to_coords(v0 + rotation.dot(np.array([dx, 0]))),
        to_coords(v0 + rotation.dot(np.array([dx, dy]))),
        to_coords(v0 + rotation.dot(np.array([0, dy]))),
        to_coords(v0),
    ])

    return shapely.to_geojson(geometry)
