from utils.DataLoader import DataLoader
import os
import pandas as pd

class StudentLifeDataLoader(DataLoader):
    
    def __init__(self, dataset, filter_weekends=True, filter_weeks=True, level='day'):
        super().__init__(dataset)
        self.users_chosen = self.config["users_chosen"]
        self.filter_weekends = filter_weekends
        self.filter_weeks = filter_weeks
        self.level = level

    def get_stress_data(self, stress_mapping='median', ):

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
                    #df.drop_duplicates(subset=['stress_level', 'date','hour'], inplace=True)

                    # get average of that date and round to nearest integer
                    df['stress_level'] = df.groupby(['date'])['stress_level'].transform(lambda x: x.mean().round())
                    df.drop_duplicates(subset=['date'], inplace=True)
                elif self.level == 'interval':
                    # create new column name interval that depends on the 'hour' column
                    df['interval'] = df['hour'].apply(lambda x: 1 if x < 12 else 2 if x >= 9 and x < 6 else 3)
                    df.drop_duplicates(subset=['date', 'interval'], inplace=True)

                if stress_mapping == 'median':
                    # calculate median value of all the stress levels
                    median_stress = round(df['stress_level'].median())
                    df['stress_level'] = df['stress_level'].apply(lambda x: 1 if x < median_stress else 2 if x == median_stress else 3)
                elif stress_mapping == 'simple':
                    df['stress_level'] = df['stress_level'].apply(lambda x: 1 if x < 3 else 2 if x == 3 else 3)
                

                # delete the 'hour' and 'resp_time' columns
                df = df.drop(columns=["hour", "resp_time"])

                # remove weekends
                if self.filter_weekends:
                    df = df[df['date'].apply(lambda x: x.weekday() < 5)]

                if self.filter_weeks:
                    # select dates between 1-april and 27-may
                    df = df[(df['date'] >= pd.Timestamp('2013-03-27').date()) & (df['date'] <= pd.Timestamp('2013-05-27').date())]

                # add the data to the dataframe using concat
                stress_data = pd.concat([stress_data if not stress_data.empty else None, df], ignore_index=True)
        
        return stress_data

    def get_stress_and_personality_data(self, daily_duplicates='mean', ambient=True, stress_mapping='median'):

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

                if stress_mapping == 'median':
                    # calculate median value of all the stress levels
                    median_stress = round(df['stress_level'].median())
                    df['stress_level'] = df['stress_level'].apply(lambda x: 1 if x < median_stress else 2 if x == median_stress else 3)
                elif stress_mapping == 'simple':
                    df['stress_level'] = df['stress_level'].apply(lambda x: 1 if x < 3 else 2 if x == 3 else 3)
                

                # delete the 'hour' and 'resp_time' columns
                df = df.drop(columns=["hour", "resp_time"])
                
                # get weather data
                if ambient:
                    weather_data = self.get_weather_data()
                    df = pd.merge(df, weather_data, how='left', on=['date'])

                # get bigfive data
                bigfive = self.get_bigfive_data()
                df['extraversion'] = bigfive[bigfive['uid'] == user_name]['extraversion'].iloc[0]
                df['agreeableness'] = bigfive[bigfive['uid'] == user_name]['agreeableness'].iloc[0]
                df['neuroticism'] = bigfive[bigfive['uid'] == user_name]['neuroticism'].iloc[0]
                df['openness'] = bigfive[bigfive['uid'] == user_name]['openness'].iloc[0]
                df['conscientiousness'] = bigfive[bigfive['uid'] == user_name]['conscientiousness'].iloc[0]

                # get flourishing data
                flourishing = self.get_flourishing_data()
                df['flourishing_score'] = flourishing[flourishing['uid'] == user_name]['flourishing_score'].iloc[0]

                # get loneliness data
                loneliness = self.get_loneliness_data()
                df['loneliness_score'] = loneliness[loneliness['uid'] == user_name]['loneliness_score'].iloc[0]

                # add the data to the dataframe using concat
                stress_data = pd.concat([stress_data if not stress_data.empty else None, df], ignore_index=True)
        
        return stress_data
    
    def get_bigfive_data(self):
        relative_bigfive_path = self.config["bigfive_data_path"]
        bigfive = pd.read_csv(os.getcwd() + relative_bigfive_path)

        # from third column, change name of the column to numbers from 1 to 44
        bigfive.columns = ['uid' , 'type'] + [i for i in range(1, 45)]

        # change cell values from 'Agree a little' to 4
        for i in range(1, 45):
            bigfive.loc[bigfive[i] == 'Disagree Strongly', i] = 1
            bigfive.loc[bigfive[i] == 'Disagree a little', i] = 2
            bigfive.loc[bigfive[i] == 'Neither agree nor disagree', i] = 3
            bigfive.loc[bigfive[i].isna(), i] = 3
            bigfive.loc[bigfive[i] == 'Agree a little', i] = 4
            bigfive.loc[bigfive[i] == 'Agree strongly', i] = 5

        bigfive['extraversion'] = bigfive[1] - bigfive[6] + bigfive[11] + bigfive[16] - bigfive[21] + bigfive[26] - bigfive[31] + bigfive[36]
        bigfive['agreeableness'] = - bigfive[2] + bigfive[7] - bigfive[12] + bigfive[17] + bigfive[22] - bigfive[27] + bigfive[32] - bigfive[37] + bigfive[42]
        bigfive['conscientiousness'] = bigfive[3] - bigfive[8] + bigfive[13] - bigfive[18] - bigfive[23] + bigfive[28] + bigfive[33] + bigfive[38] - bigfive[43]
        bigfive['neuroticism'] = bigfive[4] - bigfive[9] + bigfive[14] + bigfive[19] - bigfive[24] + bigfive[29] - bigfive[34] + bigfive[39]
        bigfive['openness'] = bigfive[5] + bigfive[10] + bigfive[15] + bigfive[20] + bigfive[25] + bigfive[30] - bigfive[35] + bigfive[40] - bigfive[41] + bigfive[44]

        return bigfive[bigfive['type'] == 'pre']
    
    
    def get_loneliness_data(self):
        relative_loneliness_path = self.config["loneliness_data_path"]
        loneliness = pd.read_csv(os.getcwd() + relative_loneliness_path)

        # from third column, change name of the column to numbers from 1 to 20
        loneliness.columns = ['uid' , 'type'] + [i for i in range(1, 21)]

        # change cell values from 'Agree a little' to 4
        for i in range(1, 21):
            loneliness.loc[loneliness[i] == 'Often', i] = 3
            loneliness.loc[loneliness[i] == 'Sometimes', i] = 2
            loneliness.loc[loneliness[i] == 'Rarely', i] = 1
            loneliness.loc[loneliness[i] == 'Never', i] = 0

        loneliness['loneliness_score'] = loneliness[1] + loneliness[2] + loneliness[3] + loneliness[4] \
            + loneliness[5] + loneliness[6] + loneliness[7] + loneliness[8] + loneliness[9] + loneliness[10] \
            + loneliness[11] + loneliness[12] + loneliness[13] + loneliness[14] + loneliness[15] + loneliness[16] \
            + loneliness[17] + loneliness[18] + loneliness[19] + loneliness[20]

        return loneliness[loneliness['type'] == 'pre']
    
    
    def get_flourishing_data(self):
        relative_flourishing_path = self.config["flourishing_data_path"]
        flourishing = pd.read_csv(os.getcwd() + relative_flourishing_path)

        # from third column, change name of the column to numbers from 1 to 8
        flourishing.columns = ['uid' , 'type'] + [i for i in range(1, 9)]

        flourishing['flourishing_score'] = flourishing[1] - flourishing[2] + flourishing[3] + flourishing[4] + flourishing[5] + flourishing[6] + flourishing[7] + flourishing[8]

        return flourishing[flourishing['type'] == 'pre']
    
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
                df = df.rename(columns={"hour": "individual_sleep_duration"})

                df['date'] = df['resp_time'].dt.date
                df['hour'] = df['resp_time'].dt.hour

                if self.level == 'day':
                    df.drop_duplicates(subset=['date'], inplace=True, keep='last')

                elif self.level == 'interval':
                    # create new column name interval that depends on the 'hour' column
                    df['interval'] = df['hour'].apply(lambda x: 1 if x < 12 else 2 if x >= 9 and x < 6 else 3)

                # delete the 'hour' and 'resp_time' columns
                df = df.drop(columns=["hour", "resp_time"])

                # remove weekends
                if self.filter_weekends:
                    df = df[df['date'].apply(lambda x: x.weekday() < 5)]

                if self.filter_weeks:
                    # select dates between 1-april and 27-may
                    df = df[(df['date'] >= pd.Timestamp('2013-03-27').date()) & (df['date'] <= pd.Timestamp('2013-05-27').date())]


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
                df = df.rename(columns={"hours": "organizational_work_hours"})

                df['date'] = df['resp_time'].dt.date
                df['hour'] = df['resp_time'].dt.hour

                if self.level == 'day':
                    df.drop_duplicates(subset=['date'], inplace=True, keep='last')

                elif self.level == 'interval':
                    # create new column name interval that depends on the 'hour' column
                    df['interval'] = df['hour'].apply(lambda x: 1 if x < 12 else 2 if x >= 9 and x < 6 else 3)

                # delete the 'hour' and 'resp_time' columns
                df = df.drop(columns=["hour", "resp_time"])

                # remove weekends
                if self.filter_weekends:
                    df = df[df['date'].apply(lambda x: x.weekday() < 5)]

                if self.filter_weeks:
                    # select dates between 1-april and 27-may
                    df = df[(df['date'] >= pd.Timestamp('2013-03-27').date()) & (df['date'] <= pd.Timestamp('2013-05-27').date())]

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
                df = df.rename(columns={"number": "organizational_social_interaction"})

                df['date'] = df['resp_time'].dt.date
                df['hour'] = df['resp_time'].dt.hour

                if self.level == 'day':
                    df.drop_duplicates(subset=['date'], inplace=True, keep='last')

                elif self.level == 'interval':
                    # create new column name interval that depends on the 'hour' column
                    df['interval'] = df['hour'].apply(lambda x: 1 if x < 12 else 2 if x >= 9 and x < 6 else 3)

                # delete the 'hour' and 'resp_time' columns
                df = df.drop(columns=["hour", "resp_time"])

                # remove weekends
                if self.filter_weekends:
                    df = df[df['date'].apply(lambda x: x.weekday() < 5)]

                if self.filter_weeks:
                    # select dates between 1-april and 27-may
                    df = df[(df['date'] >= pd.Timestamp('2013-03-27').date()) & (df['date'] <= pd.Timestamp('2013-05-27').date())]

                # add the data to the dataframe using concat
                social_data = pd.concat([social_data if not social_data.empty else None, df], ignore_index=True)
        
        return social_data
    

    def get_weather_data(self):
        relative_weather_path = self.config["weather_data_path"]
        data = pd.read_csv(os.getcwd() + relative_weather_path)
        data.set_index('time', inplace=True)
        data.index = pd.to_datetime(data.index)
        data['date'] = data.index.date
        data = data.groupby(['date'], observed=False).agg(environmental_temperature_mean=('temperature_2m (°C)', 'mean'), 
            environmental_temperature_max=('temperature_2m (°C)', 'max'), 
            environmental_temperature_min=('temperature_2m (°C)', 'min'), 
            environmental_humidity_mean=('relativehumidity_2m (%)', 'mean'), 
            environmental_humidity_max=('relativehumidity_2m (%)', 'max'), 
            environmental_humidity_min=('relativehumidity_2m (%)', 'min'), 
            environmental_precipitation=('precipitation (mm)', 'sum'), environmental_cloudcover=('cloudcover (%)', 'mean'))

        # bin ambient temperature to 1-5
        data['environmental_temperature_mean'] = pd.cut(data['environmental_temperature_mean'], bins=5, labels=False)
        data['environmental_temperature_max'] = pd.cut(data['environmental_temperature_max'], bins=5, labels=False)
        data['environmental_temperature_min'] = pd.cut(data['environmental_temperature_min'], bins=5, labels=False)

        # bin ambient humidity to 1-5
        data['environmental_humidity_mean'] = pd.cut(data['environmental_humidity_mean'], bins=5, labels=False)
        data['environmental_humidity_max'] = pd.cut(data['environmental_humidity_max'], bins=5, labels=False)
        data['environmental_humidity_min'] = pd.cut(data['environmental_humidity_min'], bins=5, labels=False)

        # bin ambient precipitation to 1-5
        data['environmental_precipitation'] = pd.cut(data['environmental_precipitation'], bins=5, labels=False)

        # bin ambient cloudcover to 1-5
        data['environmental_cloudcover'] = pd.cut(data['environmental_cloudcover'], bins=5, labels=False)
        return data
    
    def get_deadlines_data(self):

        # get deadlines data
        relative_deadlines_path = self.config["deadlines_data_path"]

        data = pd.read_csv(os.getcwd() + relative_deadlines_path)
        
        # Convertir el DataFrame de formato ancho a largo
        df_deadlines_melted = data.melt(id_vars=['uid'], var_name='date', value_name='deadlines')

        # Extraer el número de la columna 'uid' y convertirlo a tipo int
        df_deadlines_melted['user_id'] = df_deadlines_melted['uid'].str.extract('(\d+)').astype(int)
        df_deadlines_melted.drop(['uid'], axis=1, inplace=True)
        df_deadlines_melted['deadlines'] = df_deadlines_melted['deadlines'].fillna(0)
        #df_deadlines_melted['date'] = pd.to_datetime(df_deadlines_melted['date'])
    
        return df_deadlines_melted
    
    
    def get_activity_data(self):
        # get all users with activity responses
        relative_activity_path = self.config["activity_data_path"]
        files = os.listdir(os.getcwd() + relative_activity_path)

        # create dataframe to store activity responses of all users, with three columns: user_id, activity_level and response_time
        activity_data = pd.DataFrame(columns=["user_id", "date", 'minutes_stationary', 'minutes_walking', 'minutes_running', 'minutes_unknown'])

        for file in files:
            # get the user name from the filename with the following format: Stress_u16.json
            user_name = file.split(".")[0].split("_")[-1]

            # get user number from the user_name
            user_number = int(user_name.split("u")[1])

            # check if the user is chosen
            if user_number not in self.users_chosen:
                continue

            # get the data from the csv file
            with open(os.getcwd() + relative_activity_path + "/" + file) as f:
                # read csv as dataframe
                df = pd.read_csv(f)
                
                # Convertir 'timestamp' a formato datetime
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')

                # Establecer 'timestamp' como índice
                df.set_index('timestamp', inplace=True)

                # Agrupar en intervalos de 1 minuto y calcular la moda
                df = df.resample('1min').agg(lambda x: x.mode()[0] if not x.mode().empty else None)

                # Definir el mapeo de actividad a nombre
                activity_mapping = {
                    0.0: 'individual_minutes_stationary',
                    1.0: 'individual_minutes_walking',
                    2.0: 'individual_minutes_running',
                    3.0: 'individual_minutes_unknown'
                }

                # Rellenar NaN con un valor temporal para agrupamiento
                df[' activity inference'] = df[' activity inference'].fillna(-1)

                # Mapear la actividad a su correspondiente columna
                df['activity_column'] = df[' activity inference'].map(activity_mapping)

                # Agrupar por fecha y contar los minutos para cada tipo de actividad
                df = df.groupby(df.index.date).apply(lambda x: x['activity_column'].value_counts()).unstack(fill_value=0)

                # Renombrar las columnas para que tengan el formato correcto
                df = df.rename(columns={
                    'individual_minutes_stationary': 'individual_minutes_stationary',
                    'individual_minutes_walking': 'individual_minutes_walking',
                    'individual_minutes_running': 'individual_minutes_running',
                    'individual_minutes_unknown': 'individual_minutes_unknown'
                })

                # Asegurarse de que todas las columnas están presentes
                for col in ['individual_minutes_stationary', 'individual_minutes_walking', 'individual_minutes_running', 'individual_minutes_unknown']:
                    if col not in df.columns:
                        df[col] = 0

                df = df[['individual_minutes_stationary', 'individual_minutes_walking', 'individual_minutes_running', 'individual_minutes_unknown']]

                df['user_id'] = user_number
                
                df.reset_index(inplace=True)

                # remove weekends
                if self.filter_weekends:
                    df = df[df['index'].apply(lambda x: x.weekday() < 5)]

                if self.filter_weeks:
                    # select dates between 1-april and 27-may
                    df = df[(df['index'] >= pd.Timestamp('2013-03-27').date()) & (df['index'] <= pd.Timestamp('2013-05-27').date())]

                activity_data = pd.concat([activity_data if not activity_data.empty else None, df], ignore_index=True)

        return activity_data
    

    def get_audio_data(self):
        # get all users with audio responses
        relative_audio_path = self.config["audio_data_path"]
        files = os.listdir(os.getcwd() + relative_audio_path)

        # create dataframe to store stress responses of all users, with three columns: user_id, stress_level and response_time
        audio_data = pd.DataFrame(columns=["user_id", "date", "stress_level", "response_time"])

        for file in files:
            # get the user name from the filename with the following format: Stress_u16.json
            user_name = file.split(".")[0].split("_")[-1]

            # get user number from the user_name
            user_number = int(user_name.split("u")[1])

            # check if the user is chosen
            if user_number not in self.users_chosen:
                continue

            # get the data from the csv file
            with open(os.getcwd() + relative_audio_path + "/" + file) as f:
                # read csv as dataframe
                df = pd.read_csv(f)
                
                # Convertir 'timestamp' a formato datetime
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')

                # Establecer 'timestamp' como índice
                df.set_index('timestamp', inplace=True)

                # Agrupar en intervalos de 1 minuto y calcular la moda
                df = df.resample('1min').agg(lambda x: x.mode()[0] if not x.mode().empty else None)

                # Definir el mapeo de actividad a nombre
                audio_mapping = {
                    0.0: 'environmental_minutes_silence',
                    1.0: 'environmental_minutes_voice',
                    2.0: 'environmental_minutes_noise',
                    3.0: 'environmental_minutes_unknown'
                }

                # Rellenar NaN con un valor temporal para agrupamiento
                df[' audio inference'] = df[' audio inference'].fillna(-1)

                # Mapear la actividad a su correspondiente columna
                df['audio_column'] = df[' audio inference'].map(audio_mapping)

                # Agrupar por fecha y contar los minutos para cada tipo de actividad
                df = df.groupby(df.index.date).apply(lambda x: x['audio_column'].value_counts()).unstack(fill_value=0)

                # Renombrar las columnas para que tengan el formato correcto
                df = df.rename(columns={
                    'environmental_minutes_silence': 'environmental_minutes_silence',
                    'environmental_minutes_voice': 'environmental_minutes_voice',
                    'environmental_minutes_noise': 'environmental_minutes_noise',
                    'environmental_minutes_unknown': 'environmental_minutes_unknown'
                })

                # Asegurarse de que todas las columnas están presentes
                for col in ['environmental_minutes_silence', 'environmental_minutes_voice', 'environmental_minutes_noise', 'environmental_minutes_unknown']:
                    if col not in df.columns:
                        df[col] = 0

                df = df[['environmental_minutes_silence', 'environmental_minutes_voice', 'environmental_minutes_noise', 'environmental_minutes_unknown']]

                df['user_id'] = user_number
                
                df.reset_index(inplace=True)

                # remove weekends
                if self.filter_weekends:
                    df = df[df['index'].apply(lambda x: x.weekday() < 5)]

                if self.filter_weeks:
                    # select dates between 1-april and 27-may
                    df = df[(df['index'] >= pd.Timestamp('2013-03-27').date()) & (df['index'] <= pd.Timestamp('2013-05-27').date())]

                audio_data = pd.concat([audio_data if not audio_data.empty else None, df], ignore_index=True)
        
        return audio_data


    def get_conversation_data(self):
        # get all users with conversation responses
        relative_conversation_path = self.config["conversation_data_path"]
        files = os.listdir(os.getcwd() + relative_conversation_path)

        # create dataframe to store stress responses of all users, with three columns: user_id, stress_level and response_time
        conversation_data = pd.DataFrame(columns=["user_id", "date", "organizational_social_voice_sum", "organizational_social_voice_count", "organizational_social_voice_mean", "organizational_social_voice_max"])

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
                
                df['date'] = pd.to_datetime(df['start_timestamp'], unit='s').dt.date

                
                df['duration'] = (df[' end_timestamp'] - df['start_timestamp'])
                
                #df = df.resample('D').sum(numeric_only=True)
                df.drop(['start_timestamp', ' end_timestamp'], axis=1, inplace=True)
                
                

                df = df.groupby(['date'], observed=False, as_index=False).agg(organizational_social_voice_sum=('duration', 'sum'), organizational_social_voice_count=('duration', 'count'), organizational_social_voice_mean=('duration', 'mean'), organizational_social_voice_max=('duration', 'max'))

                #df.drop(['start_timestamp', ' end_timestamp'], axis=1, inplace=True)
                # add the data to the dataframe using concat
                df['user_id'] = user_number
                
                # remove weekends
                if self.filter_weekends:
                    df = df[df['date'].apply(lambda x: x.weekday() < 5)]

                if self.filter_weeks:
                    # select dates between 1-april and 27-may
                    df = df[(df['date'] >= pd.Timestamp('2013-03-27').date()) & (df['date'] <= pd.Timestamp('2013-05-27').date())]

                conversation_data = pd.concat([conversation_data if not conversation_data.empty else None, df], ignore_index=True)

        return conversation_data
    

