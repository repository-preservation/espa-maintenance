import os

''' This is a simple lookup to determine if a Landsat 5 scene is TMA or not '''


class NLAPS(object):

    def __init__(self):
        self.path = os.path.dirname(__file__)
        self.path = os.path.join(self.path, 'nlaps.txt')

        with open(self.path, 'rb') as nl:
            data = nl.read()
            self.names = data.split('\n')


def products_are_nlaps(product_list):

    if not isinstance(product_list, list):
        raise TypeError("product_list must be an instance of list()")

    results = []
    for p in product_list:
        if p in NLAPS().names:
            results.append(p)
    return results
