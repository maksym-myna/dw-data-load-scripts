from cloudsql.csv_data_processor import CSVDataprocessor
from datetime import datetime as dt
import cProfile
import pstats

def main():
    print(f"Script execution started - {dt.now().isoformat()}", flush=True)
    CSVDataprocessor().run()
    print(f"Script execution finished - {dt.now().isoformat()}", flush=True)


if __name__ == "__main__":
    # cProfile.run("main()", "log.txt")
    main()
    # p = pstats.Stats("log.txt")
    # p.sort_stats("cumulative").print_stats(50)
