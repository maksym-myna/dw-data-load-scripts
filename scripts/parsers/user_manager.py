import itertools
import random
import names
from datetime import datetime, timedelta
from parsers.file_writer import FileWriter 

class UserManager(FileWriter):
    def __init__(self, file_type: str):
        FileWriter.__init__(self, file_type)
        
        self.users = []
        self.usersId = itertools.count(1)
        self.default_pfp = {
            "pfp_id": 1,
            "url": 'https://storage.cloud.google.com/data_warehousing_library_data/default-pfp.svg',
            "added_at": datetime.now().isoformat()
        }
        
        self.user_file = open(rf'open library dump\data\user.{file_type}', 'w', encoding='utf-8', newline='')
        
    def get_or_generate_reader(self) -> dict:
        if not self.users or random.random() < 20000/len(self.users)/random.randint(1,500):
            id = next(self.usersId)
            self.users.append(id)
            return id
        return random.choice(self.users)
        
    def fill_user(self, user_id) -> None:
        gender = random.choice(['male', 'female', 'non-binary'])
        first_name = names.get_first_name(gender)
        last_name = names.get_first_name(gender)
        email = f'{first_name}_{last_name}@knyhozbirnia.com'
        
        return {
            "user_id": user_id,
            "first_name": first_name,
            "last_name": last_name,
            "gender": gender[0],
            "email": email,
            "birthday": self._random_birthday(),
            "added_at": datetime.now().isoformat(),
            # "pfp_id": 1
        }
        
    @classmethod
    def _random_birthday(cls, start_year=1950, end_year=datetime.now().year):
        # Generate a random year between start_year and end_year
        year = random.randint(start_year, end_year)
        # Generate a random day in that year
        day = random.randint(1, 366)
        # Convert the year and day to a date
        birthday = datetime(year, 1, 1) + timedelta(days=day - 1)
        return birthday

    def writeUsers(self):
        for user in self.users:
            self._write_strategy(self.users_file, self.fill_user(user))
        
    def writeUsers(self):
        for user in self.users:
            self._write_strategy(self.users_file, self.fill_user(user))
    
    def writePfp(self):
        with open(rf'open library dump\data\pfp.{self.type_name}', 'w', encoding='utf-8', newline='') as f_in:
            self._write_strategy(f_in, self.default_pfp)