def debug(num, filename, *args):
    filename = filename[filename.find('rq'):]
    string = 'DEBUG {} at ({})...'.format(num, filename)
    print(string, *args)


MY = debug
