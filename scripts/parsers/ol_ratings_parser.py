import orjson
from parsers.ol_abstract_parser import OLAbstractParser 
import os
import glob

class OLRatingsParser(OLAbstractParser):
    """
    A class for parsing Open Library ratings data.

    Attributes:
        None

    Methods:
        process_file(input_file, output_file): Process the input file and write the parsed data to the output file.
        process_latest_file(directory): Process the latest ratings file in the specified directory.
        parse_line(line): Parse a single line of ratings data.

    Returns:
        list[str]: names of output files.
    """
    def process_file(self, input_file, output_file):
        with open(input_file, 'r', encoding='utf-8') as f_in, open(output_file, 'w', encoding='utf-8') as f_out:
            for line in f_in:
                obj = self.__parse_line(line)
                f_out.write(orjson.dumps(obj).decode('utf-8') + '\n')  # Write the JSON object followed by a newline
        return [output_file]
        
    def process_latest_file(self, directory):
        files = glob.glob(os.path.join(directory, 'ol_dump_ratings*.txt'))

        files.sort(reverse=True)
        return self.process_file(files[0], rf'{directory}\data\ratings.jsonl')
      
    def __parse_line(self, line):
        fields = line.split('\t')
        shift = 1 if len(fields) == 4 else 0
        work = fields[0].split('/')[-1]
        rating = fields[1+shift]
        date = fields[2+shift].strip()
        return {
            'work': work,
            'rating': rating,
            'date': date
        }