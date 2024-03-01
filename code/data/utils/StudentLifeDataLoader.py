from utils.DataLoader import DataLoader
import os
import pandas as pd

class StudentLifeDataLoader(DataLoader):
    
    def get_stress_data(self):
        # get all users with stress responses
        relative_stress_path = self.config["stress_data_path"]
        users_chosen = self.config["users_chosen"]

        files = os.listdir(os.getcwd() + relative_stress_path)

        # create dataframe to store stress responses of all users, with three columns: user_id, stress_level and response_time
        stress_data = pd.DataFrame(columns=["user_id", "stress_level", "resp_time"])

        for file in files:
            
            # get the user name from the filename with the following format: Stress_u16.json
            user_name = file.split(".")[0].split("_")[-1]

            # get user number from the user_name
            user_number = int(user_name.split("u")[1])

            # check if the user is chosen
            if user_number not in users_chosen:
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

                # add the data to the dataframe using concat
                stress_data = pd.concat([stress_data if not stress_data.empty else None, df], ignore_index=True)
        
        return stress_data