import itertools
import random
import names
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

        self.users = []
        self.usersId = itertools.count(1)
        self.default_pfp = {
            "pfp_id": 1,
            "url": 'https://storage.cloud.google.com/data_warehousing_library_data/default-pfp.svg',
            "added_at": datetime.now().isoformat()
        }

        self.user_file = None

    def get_or_generate_reader(self) -> dict:
        """
        Retrieves an existing user ID from the list of users or generates a new one.

        Returns:
            dict: The user ID.
        """
        if not self.users or random.random() < 20000/len(self.users)/random.randint(1,500):
            id = next(self.usersId)
            self.users.append(id)
            return id
        return random.choice(self.users)

    def fill_user(self, user_id) -> None:
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
            - added_at: The timestamp when the user was added.
        """
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
        }

    @classmethod
    def _random_birthday(cls, start_year=1950, end_year=datetime.now().year):
        """
        Generate a random birthday between the specified start_year and end_year.

        Parameters:
        - start_year (int): The starting year for generating the random birthday. Default is 1950.
        - end_year (int): The ending year for generating the random birthday. Default is the current year.

        Returns:
        - birthday (datetime): A randomly generated birthday as a datetime object.
        """
        year = random.randint(start_year, end_year)
        day = random.randint(1, 366)
        birthday = datetime(year, 1, 1) + timedelta(days=day - 1)
        return birthday

    def writeUser(self, user):
        """
        Writes the user information to the user file.

        Args:
            user: The user object containing the information to be written.

        Returns:
            None
        """
        if not self.user_file:
            self.user_file = open(self.get_user_file(), 'w', encoding='utf-8', newline='')
        self._write_strategy(self.user_file, self.fill_user(user))

    def writeUsers(self):
        """
        Writes the users to the user file using the specified write strategy.

        This method iterates over the list of users and writes each user to the user file
        using the write strategy specified in the `_write_strategy` attribute.

        Args:
            None

        Returns:
            None
        """
        self.user_file = open(self.get_user_file(), 'w', encoding='utf-8', newline='')
        for user in self.users:
            self._write_strategy(self.user_file, self.fill_user(user))

    def writePfp(self):
        """
        Writes the default profile picture (pfp) to a file.

        This method opens a file and writes the default profile picture (pfp) data to it.

        Args:
            None

        Returns:
            None
        """
        with open(self.get_pfp_file(), 'w', encoding='utf-8', newline='') as f_in:
            self._write_strategy(f_in, self.default_pfp)

    def get_user_file(self):
        """
        Returns the path to the user file.

        Args:
            None

        Returns:
            str: The path to the user file.
        """
        return rf'open library dump\data\library_user.{self.type_name}'

    def get_pfp_file(self):
        """
        Returns the path to the pfp file.

        Args:
            None

        Returns:
            str: The path to the pfp file.
        """
        return rf'open library dump\data\pfp.{self.type_name}'