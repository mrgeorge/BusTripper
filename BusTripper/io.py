def readKeys(filename):
    """Construct dict with keys from file

    Expects text file with 1 key per line in the format
    key=value
    Lines can be commented by starting with '#'
    """
    loginDict = {}
    comments = ('#')
    with open(filename) as ff:
        for line in ff:
            if line[0] not in comments:
                elements = line.rstrip().split("=")
                loginDict[elements[0]] = elements[1]

    return loginDict

