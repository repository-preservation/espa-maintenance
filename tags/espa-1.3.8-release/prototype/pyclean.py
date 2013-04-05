if __name__ == '__main__':
    handle = open('test.txt', 'rb+')
    data = handle.read()
    handle.close()

    parts = data.split('\n')
    for p in parts:
        print p.split('.tar.gz')[0]
