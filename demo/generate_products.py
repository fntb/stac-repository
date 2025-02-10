from os import path
import os
import tempfile
import shutil

from stac_processor_demo import SimpleProduct


def generate_demo_products():
    dir = tempfile.mkdtemp()

    try:
        for product_dir_name in ["1", "2"]:
            product_dir = path.join(dir, product_dir_name)
            os.makedirs(product_dir)
            SimpleProduct.generate(product_dir)
    except Exception as error:
        shutil.rmtree(dir, ignore_errors=True)
        raise error

    return dir
