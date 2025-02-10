from os import PathLike
from os import path
import uuid
import json

import shapely
import pyproj
import numpy
import matplotlib.pyplot as plt
import random


def generate_random_walk_image(dir: PathLike[str]):

    n = 100

    x = numpy.zeros(n)
    y = numpy.zeros(n)

    for i in range(1, n):
        val = random.randint(1, 4)
        if val == 1:
            x[i] = x[i - 1] + 1
            y[i] = y[i - 1]
        elif val == 2:
            x[i] = x[i - 1] - 1
            y[i] = y[i - 1]
        elif val == 3:
            x[i] = x[i - 1]
            y[i] = y[i - 1] + 1
        else:
            x[i] = x[i - 1]
            y[i] = y[i - 1] - 1

    plt.plot(x, y, color="black", linewidth=0.2)
    plt.axis("off")
    plt.savefig(
        path.join(dir, "random_walk_" + uuid.uuid4().hex + ".png"),
        bbox_inches="tight",
        dpi=900
    )
