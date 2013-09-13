__author__ = "David V. Hill"

def frange(start,end,step):
    #return map(lambda x: x*step, range(int(start*1./step),int(end*1./step)))
    
    return [x*step for x in range(int(start * 1./step), int(end * 1./step))]
