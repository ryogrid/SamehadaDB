# coding: utf-8
import os, sys, io

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
                        if line.find('TODO') > -1 and line.find('(SDB)') > -1 and line.find('[OPT]') > -1:
                            if not isPathPrinted:
                                print(filePathToPrint)
                                isPathPrinted = True
                            stripedLine = line.strip()
                            print('  ' + '{:<5}'.format(str(lineNum) + ':') + ' ' + stripedLine)
                        lineNum += 1

def main():
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')
    listTodoComments()

main()