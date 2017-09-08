'''
csv2oj.py - Command-line wrapper for CSV to OCL-JSON converter

Arguments:
-i -inputfile Name of CSV input file
-o -outputfile Name for OCL-formatted JSON file
-d -deffile Name for CSV resource definition file
-v Verbosity setting: 0=None, 1=Some logging, 2=All logging
'''
import sys, getopt


def main(argv):
    inputfile = ''
    outputfile = ''
    deffile = ''
    verbosity = 0
    try:
        opts, args = getopt.getopt(argv, "hi:o:d:v:", ["inputfile=","outputfile=","deffile="])
    except getopt.GetoptError as err:
        print 'Unexpected argument exception: ', err
        print 'Syntax:'
        print '    test.py -i <inputfile> -o <outputfile> -d <deffile> -v <verbosity>'
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print 'Syntax:'
            print '    test.py -i <inputfile> -o <outputfile> -d <deffile> -v <verbosity>'
            sys.exit()
        elif opt == '-v':
            if arg in ('0', '1', '2'):
                verbosity = arg
            else:
                print 'Invalid argument: -v (0,1,2)'
                sys.exit()
        elif opt in ("-i", "--ifile"):
            inputfile = arg
        elif opt in ("-o", "--ofile"):
            outputfile = arg
    print "Input file: ", inputfile
    print "Output file: ", outputfile
    print "Def file: ", deffile
    print "Verbosity: ", verbosity


if __name__ == "__main__":
   main(sys.argv[1:])
