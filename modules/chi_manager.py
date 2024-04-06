import pandas as pd
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv

from modules.connection_manager import read_from_db

from logger import logger


class Chi:
    def __init__(self, conjunto, pred_year, pred_month, months_ahead):
        load_dotenv()

        self.has_error = False
        self.conjunto_chi_goals = []

        chi_goals = pd.DataFrame()
        read_from_database = os.getenv("READ_FROM_DATABASE").lower()
        if read_from_database == "yes":
            # READ FROM DATABASE
            table = 'metas_chi'
            try:
                chi_goals = read_from_db(table_name=table)
                chi_goals = chi_goals[chi_goals["SIGLA"] == conjunto]
            except Exception as ex:
                logger.info(
                    f"An exception is occurred in reading {table}, {ex}")
                self.has_error = True

        else:
            # READ FROM CSV FILE
            interrup_path = os.getenv("INTERRUPTION_PATH")
            chi_goals_filename = os.getenv("CHI_GOALS_FILENAME")
            path = os.path.join(interrup_path, chi_goals_filename)
            try:
                chi_goals = pd.read_csv(path)
                chi_goals = chi_goals[chi_goals["SIGLA"] == conjunto]
            except Exception as ex:
                logger.info(f"An exception is occured, {ex}")
                self.has_error = True

        if not chi_goals.empty:
            self.process_chi_values(
                chi_goals, pred_year, pred_month, months_ahead)
        else:
            self.has_error = True

    def process_chi_values(self, chi_goals, pred_year, pred_month, months_ahead):
        start_pred_date = datetime(year=pred_year, month=pred_month, day=1)

        next_date = start_pred_date
        for mon in range(months_ahead):
            # chi_goal column format sample: 1/1/2020
            col = f"{next_date.day}/{next_date.month}/{next_date.year}"
            chi_vals_df = chi_goals[col].tolist()
            if len(chi_vals_df) > 0:
                self.conjunto_chi_goals.append(sum(chi_vals_df))
            else:
                self.conjunto_chi_goals.append(0)

            next_date += relativedelta(months=1)

        prev_date = start_pred_date
        for mon in range(12 - months_ahead):
            prev_date -= relativedelta(months=1)

            # chi_goal column format sample: 1/1/2020
            col = f"{prev_date.day}/{prev_date.month}/{prev_date.year}"
            chi_vals_df = chi_goals[col].tolist()
            if len(chi_vals_df) > 0:
                self.conjunto_chi_goals.append(sum(chi_vals_df))
            else:
                self.conjunto_chi_goals.append(0)
