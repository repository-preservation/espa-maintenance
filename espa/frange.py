__author__ = "David V. Hill"

def frange(start,end,step):
       
    return [x*step for x in range(int(start * 1./step), int(end * 1./step))]
