import sys
def create_tuples_from_temp(inp):
    # 897340|20110917
    # 722577|20120712|                           132.8
    func = lambda i: i.lstrip()
    return tuple(func(i) for i in inp.split("|"))


print(create_tuples_from_temp('896060|20100802|                          -115.2'))

