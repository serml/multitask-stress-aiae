from utils.DataLoader import DataLoader
import os
import pandas as pd

class StudentLifeDataLoader(DataLoader):
    
    def __init__(self, dataset, level='day'):
        super().__init__(dataset)
        self.users_chosen = self.config["users_chosen"]
        self.level = level

    def get_stress_data(self, daily_duplicates='mean'):

        # get all users with stress responses
        relative_stress_path = self.config["stress_data_path"]
        files = os.listdir(os.getcwd() + relative_stress_path)

        # create dataframe to store stress responses of all users, with three columns: user_id, stress_level and response_time
        stress_data = pd.DataFrame(columns=["user_id", "stress_level", "resp_time"])

        for file in files:
            
            # get the user name from the filename with the following format: Stress_u16.json
            user_name = file.split(".")[0].split("_")[-1]

            # get user number from the user_name
            user_number = int(user_name.split("u")[1])

            # check if the user is chosen
            if user_number not in self.users_chosen:
                continue

            # get the data from the json file
            with open(os.getcwd() + relative_stress_path + "/" + file) as f:
                # read json as dataframe
                df = pd.read_json(f)
                df = df[['resp_time', 'level']]
                df['user_id'] = user_number

                # drop nan values in the 'level' column
                df = df.dropna(subset=['level'])

                # rename the 'level' column to 'stress_level'
                df = df.rename(columns={"level": "stress_level"})

                df['date'] = df['resp_time'].dt.date
                df['hour'] = df['resp_time'].dt.hour
                
                df['stress_level'] = df['stress_level'].apply(lambda x: 1 if x == 5 else 2 if x == 4 else 3 if x == 1 else 4 if x == 3 else 5)

                if self.level == 'day':
                    df.drop_duplicates(subset=['stress_level', 'date','hour'], inplace=True)

                    # get average of that date and round to nearest integer
                    df['stress_level'] = df.groupby(['date'])['stress_level'].transform(lambda x: x.mean().round())
                    df.drop_duplicates(subset=['date'], inplace=True)
                elif self.level == 'interval':
                    # create new column name interval that depends on the 'hour' column
                    df['interval'] = df['hour'].apply(lambda x: 1 if x < 12 else 2 if x >= 9 and x < 6 else 3)

                # calculate median value of all the stress levels
                median_stress = round(df['stress_level'].median())
                df['stress_level'] = df['stress_level'].apply(lambda x: 1 if x < median_stress else 2 if x == median_stress else 3)
                

                # delete the 'hour' and 'resp_time' columns
                df = df.drop(columns=["hour", "resp_time"])
                
                # add the data to the dataframe using concat
                stress_data = pd.concat([stress_data if not stress_data.empty else None, df], ignore_index=True)
        
        return stress_data
    
    # the same as the above, but with sleep data instead
    def get_sleep_data(self):
        # the same as the above, but with sleep data instead.
        # get all users with sleep responses
        relative_sleep_path = self.config["sleep_data_path"]
        files = os.listdir(os.getcwd() + relative_sleep_path)

        # create dataframe to store sleep responses of all users, with three columns: user_id, sleep_duration and response_time
        sleep_data = pd.DataFrame(columns=["user_id", "hour", "resp_time"])

        for file in files:

            # get the user name from the filename with the following format: Stress_u16.json
            user_name = file.split(".")[0].split("_")[-1]

            # get user number from the user_name
            user_number = int(user_name.split("u")[1])

            # check if the user is chosen
            if user_number not in self.users_chosen:
                continue

            # get the data from the json file
            with open(os.getcwd() + relative_sleep_path + "/" + file) as f:
                # read json as dataframe
                df = pd.read_json(f)
                df = df[['resp_time', 'hour']]
                df['user_id'] = user_number

                # drop nan values in the 'duration' column
                df = df.dropna(subset=['hour'])

                # rename the 'duration' column to'sleep_duration'
                df = df.rename(columns={"hour": "sleep_duration"})

                df['date'] = df['resp_time'].dt.date
                df['hour'] = df['resp_time'].dt.hour

                if self.level == 'day':
                    df.drop_duplicates(subset=['date'], inplace=True, keep='last')

                elif self.level == 'interval':
                    # create new column name interval that depends on the 'hour' column
                    df['interval'] = df['hour'].apply(lambda x: 1 if x < 12 else 2 if x >= 9 and x < 6 else 3)

                # delete the 'hour' and 'resp_time' columns
                df = df.drop(columns=["hour", "resp_time"])

                # add the data to the dataframe using concat
                sleep_data = pd.concat([sleep_data if not sleep_data.empty else None, df], ignore_index=True)
        
        return sleep_data


    # the same that above but with class data
    def get_class_data(self):
        # get all users with class responses
        relative_class_path = self.config["class_data_path"]
        files = os.listdir(os.getcwd() + relative_class_path)

        # create dataframe to store class responses of all users, with three columns: user_id, class_level and response_time
        class_data = pd.DataFrame(columns=["user_id", "class_level", "resp_time"])

        for file in files:

            # get the user name from the filename with the following format: Stress_u16.json
            user_name = file.split(".")[0].split("_")[-1]

            # get user number from the user_name
            user_number = int(user_name.split("u")[1])

            # check if the user is chosen
            if user_number not in self.users_chosen:
                continue

            # get the data from the json file
            with open(os.getcwd() + relative_class_path + "/" + file) as f:
                # read json as dataframe
                df = pd.read_json(f)
                df = df[['resp_time', 'hours']]
                df['user_id'] = user_number

                # drop nan values in the 'hours' column
                df = df.dropna(subset=['hours'])

                # rename the 'hours' column to 'work_hours'
                df = df.rename(columns={"hours": "work_hours"})

                df['date'] = df['resp_time'].dt.date
                df['hour'] = df['resp_time'].dt.hour

                # create new column name interval that depends on the 'hour' column
                df['interval'] = df['hour'].apply(lambda x: 1 if x < 12 else 2 if x >= 9 and x < 6 else 3)

                # delete the 'hour' and 'resp_time' columns
                df = df.drop(columns=["hour", "resp_time"])

                # add the data to the dataframe using concat
                class_data = pd.concat([class_data if not class_data.empty else None, df], ignore_index=True)
        
        return class_data


    # the same as the above but with lab data
    def get_lab_data(self):
        
        # get all users with lab responses
        relative_lab_path = self.config["lab_data_path"]
        files = os.listdir(os.getcwd() + relative_lab_path)

        # create dataframe to store lab responses of all users, with three columns: user_id, lab_level and response_time

        lab_data = pd.DataFrame(columns=["user_id", "lab_hours", "resp_time"])

        for file in files:
            
            # get the user name from the filename with the following format: Stress_u16.json

            user_name = file.split(".")[0].split("_")[-1]

            # get user number from the user_name

            user_number = int(user_name.split("u")[1])

            # check if the user is chosen

            if user_number not in self.users_chosen:
                continue

            # get the data from the json file

            with open(os.getcwd() + relative_lab_path + "/" + file) as f:

                # read json as dataframe

                df = pd.read_json(f)
                try:
                    df = df[['resp_time', 'duration']]
                except KeyError:
                    pd.concat([lab_data if not lab_data.empty else None, None], ignore_index=True)
                    continue

                df['user_id'] = user_number
                # drop nan values in the 'duration' column
                df = df.dropna(subset=['duration'])

                # rename the 'duration' column to 'lab_hours'
                df = df.rename(columns={"duration": "lab_hours"})

                df['date'] = df['resp_time'].dt.date
                df['hour'] = df['resp_time'].dt.hour

                # create new column name interval that depends on the 'hour' column
                df['interval'] = df['hour'].apply(lambda x: 1 if x < 12 else 2 if x >= 9 and x < 6 else 3)

                # delete the 'hour' and 'resp_time' columns
                df = df.drop(columns=["hour", "resp_time"])

                # add the data to the dataframe using concat
                lab_data = pd.concat([lab_data if not lab_data.empty else None, df], ignore_index=True)
        
        return lab_data



    def get_social_data(self):

        # get all users with social responses
        relative_social_path = self.config["social_data_path"]
        files = os.listdir(os.getcwd() + relative_social_path)

        # create dataframe to store social responses of all users, with three columns: user_id, social_level and response_time
        social_data = pd.DataFrame(columns=["user_id", "social_level", "resp_time"])

        for file in files:
            
            # get the user name from the filename with the following format: Stress_u16.json
            user_name = file.split(".")[0].split("_")[-1]

            # get user number from the user_name
            user_number = int(user_name.split("u")[1])

            # check if the user is chosen
            if user_number not in self.users_chosen:
                continue

            # get the data from the json file
            with open(os.getcwd() + relative_social_path + "/" + file) as f:
                # read json as dataframe
                df = pd.read_json(f)
                df = df[['resp_time', 'number']]
                df['user_id'] = user_number

                # drop nan values in the 'number' column
                df = df.dropna(subset=['number'])

                # rename the 'number' column to 'social_level'
                df = df.rename(columns={"number": "social_level"})

                df['date'] = df['resp_time'].dt.date
                df['hour'] = df['resp_time'].dt.hour

                if self.level == 'day':
                    df.drop_duplicates(subset=['date'], inplace=True, keep='last')

                elif self.level == 'interval':
                    # create new column name interval that depends on the 'hour' column
                    df['interval'] = df['hour'].apply(lambda x: 1 if x < 12 else 2 if x >= 9 and x < 6 else 3)

                # delete the 'hour' and 'resp_time' columns
                df = df.drop(columns=["hour", "resp_time"])

                # add the data to the dataframe using concat
                social_data = pd.concat([social_data if not social_data.empty else None, df], ignore_index=True)
        
        return social_data
    

    def get_conversation_data(self):
        # get all users with conversation responses
        relative_conversation_path = self.config["conversation_data_path"]
        files = os.listdir(os.getcwd() + relative_conversation_path)

        # create dataframe to store stress responses of all users, with three columns: user_id, stress_level and response_time
        conversation_data = pd.DataFrame(columns=["start_timestamp", "end_timestamp"])

        for file in files:
            # get the user name from the filename with the following format: Stress_u16.json
            user_name = file.split(".")[0].split("_")[-1]

            # get user number from the user_name
            user_number = int(user_name.split("u")[1])

            # check if the user is chosen
            if user_number not in self.users_chosen:
                continue

            # get the data from the csv file
            with open(os.getcwd() + relative_conversation_path + "/" + file) as f:
                # read csv as dataframe
                df = pd.read_csv(f)
                df['user_id'] = user_number

                # add the data to the dataframe using concat
                conversation_data = pd.concat([conversation_data if not conversation_data.empty else None, df], ignore_index=True)

        return conversation_data