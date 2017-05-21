import sys

def write_file(filename):
    target = open(filename, 'w')
    a = ['line 1', 'line 2', 'line 3']
    for b in a:
        target.write(b)
        target.write('\n')
    target.close()

def read_file(filename):
    a = []
    with open(filename) as fp:
        for line in fp:
            if line:
                a.append(line)
                print line
    print a[0], a[1]
    # print fr.readline()
    # fr.close()


write_file('a.txt')

read_file('a.txt')