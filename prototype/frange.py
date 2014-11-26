def frange(start, end, step):
    '''Provides Python range functions over floating point values'''
    return [x*step for x in range(int(start * 1./step), int(end * 1./step))]
