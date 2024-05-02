import pandas as pd
import csv
from ..language_speakers import speakers
# from  language_speakers import speakers
from datetime import datetime

class LanguageParser:
    def run(
        self,
        file_in: str = "open library dump/iso-639-2-languages.csv",
        file_out: str = "open library dump/data/lang.csv",
    ) -> str:
        df = pd.read_csv(file_in, names=["id", "name"])

        df["speakers"] = df["id"].map(speakers).fillna(0).astype(int)
        
        df["modified_at"] = datetime.now().isoformat()

        df.to_csv(file_out, index=False, header=False, encoding="utf-8", quoting=csv.QUOTE_ALL)
        
        return file_out
