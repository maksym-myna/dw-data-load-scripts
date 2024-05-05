import itertools
import random
from faker import Faker
from datetime import datetime, timedelta
from parsers.file_writer import FileWriter


class UserManager(FileWriter):
    def __init__(self, file_type: str):
        """
        Initializes a User Manager object.

        Args:
            file_type (str): The type of file to be written.

        Attributes:
            users (list): A list to store user objects.
            usersId (itertools.count): An iterator to generate unique user IDs.
            default_pfp (dict): A dictionary representing the default profile picture.
            user_file (file): A file object to write user data.
        """
        FileWriter.__init__(self, file_type)

        self.fake = Faker()
        self.users = []
        self.usersId = itertools.count(1)
        self.default_pfp = {
            "user_id": 1,
            "url": "https://storage.cloud.google.com/data_warehousing_library_data/default-pfp.svg",
        }
        self.emails = set()

        self.random_genders = [
            "male",
            "female",
            "non-binary",
        ]
        self.gender_weights = [4, 4, 1]

        self.user_file = None

    def get_or_generate_reader(self) -> dict[int, str] | int | None:
        """
        Retrieves an existing user ID from the list of users or generates a new one.

        Returns:
            dict: The user ID.
        """
        if not self.users or random.random() < 20000 / len(self.users) / random.randint(
            1, 500
        ):
            if not (id := next(self.usersId)):
                return None
            self.users.append(id)
            return id
        return random.choice(self.users)

    def fill_user(self, user_id) -> tuple[int, str, str, str, str, str, str, str]:
        """
        Fills the user details with random data.

        Parameters:
        - user_id: The ID of the user.

        Returns:
        - A dictionary containing the user details:
            - user_id: The ID of the user.
            - first_name: The first name of the user.
            - last_name: The last name of the user.
            - gender: The gender of the user.
            - email: The email address of the user.
            - birthday: The birthday of the user.
            - created_at: The timestamp when the user was added.
        """    
        gender = random.choices(self.random_genders, weights=self.gender_weights, k=1)[0]
        if gender == "male":
            get_first_name = self.fake.first_name_male
            get_last_name = self.fake.last_name_male
        elif gender == "female":
            get_first_name = self.fake.first_name_female
            get_last_name = self.fake.last_name_female
        else:
            get_first_name = self.fake.first_name
            get_last_name = self.fake.last_name
        
        first_name = get_first_name()
        last_name = get_last_name()

        email = f"{first_name}_{last_name}@knyhozbirnia.com"
        index = 0
        while email in self.emails:
            email = f"{first_name}_{last_name}{index}@knyhozbirnia.com"
            index+=1
        self.emails.add(email)

        return (
            user_id,
            first_name,
            last_name,
            gender[0],
            email,
            self.random_birthday(),
            "USER",
            datetime.now().isoformat(),
        )

    @staticmethod
    def random_birthday(
        start_year: int = 1960, end_year: int = datetime.now().year - 6
    ) -> str:
        """
        Generate a random birthday between the specified start_year and end_year.

        Parameters:
        - start_year (int): The starting year for generating the random birthday. Default is 1950.
        - end_year (int): The ending year for generating the random birthday. Default is the current year.

        Returns:
        - birthday (datetime): A randomly generated birthday as a datetime object.
        """
        rand_year = random.random()  # Generate a random number between 0 and 1
        if rand_year < 0.8:  # 80% chance to generate a year between 1975 and 2005
            year =  random.randint(start_year+15, end_year - 11)
        elif rand_year < 0.9:  # 10% chance to generate a year between 1950 and 1974
            year = random.randint(start_year, start_year+14)
        else:  # 10% chance to generate a year between 2006 and 2017
            year = random.randint(end_year - 10, end_year)
        day = random.randint(1, 365)
        birthday = datetime(year, 1, 1) + timedelta(days=day)
        return birthday.date().isoformat()

    def write_user(self, user):
        """
        Writes the user information to the user file.

        Args:
            user: The user object containing the information to be written.

        Returns:
            None
        """
        if not self.user_file:
            self.user_file = open(
                self.get_user_file(), "w", encoding="utf-8", newline=""
            )
        self._tuple_write_strategy(self.user_file, [self.fill_user(user)])

    def write_users(self):
        """
        Writes the users to the user file using the specified write strategy.

        This method iterates over the list of users and writes each user to the user file
        using the write strategy specified in the `_write_strategy` attribute.

        Args:
            None

        Returns:
            None
        """
        print(f"Processing users - {datetime.now().isoformat()}", flush=True)
        self.user_file = open(self.get_user_file(), "w", encoding="utf-8", newline="")

        users = [self.fill_user(user) for user in self.users]

        self._tuple_write_strategy(self.user_file, users)
        self.user_file.flush()
        self.user_file.close()

    def writePfp(self):
        """
        Writes the default profile picture (pfp) to a file.

        This method opens a file and writes the default profile picture (pfp) data to it.

        Args:
            None

        Returns:
            None
        """
        with open(self.get_pfp_file(), "w", encoding="utf-8", newline="") as f_in:
            self._write_strategy(f_in, self.default_pfp)

    def get_user_file(self):
        """
        Returns the path to the user file.

        Args:
            None

        Returns:
            str: The path to the user file.
        """
        return rf"open library dump\data\library_user.{self.type_name}"

    def get_pfp_file(self):
        """
        Returns the path to the pfp file.

        Args:
            None

        Returns:
            str: The path to the pfp file.
        """
        return rf"open library dump\data\pfp.{self.type_name}"
