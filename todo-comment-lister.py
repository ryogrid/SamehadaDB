# coding: utf-8
import os, sys, io

additonalFilter = None

def listTodoComments():
    for root, dirs, files in os.walk(top='./'):
        for file in files:
            filePath = os.path.join(root, file)
            filePathToPrint = filePath.replace('\\', '/')
            isPathPrinted = False
            if filePath.endswith('.go'):
                with open(filePath, mode='r', encoding='utf-8') as f:
                    lineNum = 1
                    for line in f:
                        try:
                            if line.find('TODO') > -1 and line.find('(SDB)') > -1:
                                if additonalFilter and line.find(additonalFilter) == -1:
                                    continue

                                if not isPathPrinted:
                                    print(filePathToPrint)
                                    isPathPrinted = True
                                stripedLine = line.strip()
                                print('  ' + '{:<5}'.format(str(lineNum) + ':') + ' ' + stripedLine)
                        finally:
                            lineNum += 1

def main():
    global additonalFilter

    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

    # additonal filtering option: -f "<filtering string>"
    if len(sys.argv) == 3 and sys.argv[1] == '-f':
        additonalFilter = sys.argv[2]

    listTodoComments()

main()