from io import TextIOWrapper
import orjson
import csv 

class FileWriter:
    def __init__(self, file_type):
       self._write_strategy = self._write_jsonl if file_type == 'jsonl' else self._write_csv
       self.type_name = file_type

    @classmethod
    def _write_jsonl(cls, file: TextIOWrapper, obj: dict)-> None:
        file.write(orjson.dumps(obj).decode('utf-8') + '\n')

    @classmethod
    def _write_csv(cls, file: str, obj: dict) -> None:
        # obj = {k: v.encode('unicode_escape').decode() if isinstance(v, str) else v for k, v in obj.items()}
        try:
            writer = csv.DictWriter(file, fieldnames=obj.keys(), quoting=csv.QUOTE_ALL)
            writer.writerow(obj)
        except Exception:
            pass