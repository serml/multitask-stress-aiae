from utils.DataLoader import DataLoader
import os
import pandas as pd

class StudentLife2024DataLoader(DataLoader):
    
    def __init__(self, dataset, level='day'):
        super().__init__(dataset)
        self.users_chosen = self.config["users_chosen"]
        self.level = level

    def get_stress_data(self, daily_duplicates='mean', ambient=True, stress_mapping='median'):

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
                    pass
                    # df.drop_duplicates(subset=['stress_level', 'date','hour'], inplace=True)

                    # get average of that date and round to nearest integer
                    # df['stress_level'] = df.groupby(['date'])['stress_level'].transform(lambda x: x.mean().round())
                    # df.drop_duplicates(subset=['date'], inplace=True)
                elif self.level == 'interval':
                    # create new column name interval that depends on the 'hour' column
                    df['interval'] = df['hour'].apply(lambda x: 1 if x < 12 else 2 if x >= 9 and x < 6 else 3)

                if stress_mapping == 'median':
                    # calculate median value of all the stress levels
                    median_stress = round(df['stress_level'].median())
                    df['stress_level'] = df['stress_level'].apply(lambda x: 1 if x < median_stress else 2 if x == median_stress else 3)
                elif stress_mapping == 'simple':
                    df['stress_level'] = df['stress_level'].apply(lambda x: 1 if x < 3 else 2 if x == 3 else 3)
                

                # delete the 'hour' and 'resp_time' columns
                df = df.drop(columns=["hour", "resp_time"])

                # add the data to the dataframe using concat
                stress_data = pd.concat([stress_data if not stress_data.empty else None, df], ignore_index=True)
        
        return stress_data