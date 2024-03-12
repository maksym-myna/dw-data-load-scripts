from cloudsql.csv_data_processor import CSVDataprocessor
import parsers.ol_dump_parser as oldumpp
import re

def main():
    # # old = oldumpp.OLDumpParser('csv')
    # for _ in range(0, 10000):
    #     old.get_or_generate_reader()
    # old.writeUsers()
    CSVDataprocessor().run()
    

if __name__ == "__main__":
    main()
