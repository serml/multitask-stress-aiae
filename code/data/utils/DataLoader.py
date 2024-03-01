import yaml
import os

class DataLoader:
    def __init__(self, dataset_name):
        self.dataset_name = dataset_name

        # load configuration file
        with open("./data_config.yaml") as f:
            self.config = yaml.load(f, Loader=yaml.FullLoader)['datasets'][dataset_name]