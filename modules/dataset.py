import pandas as pd
import os
from dotenv import load_dotenv
from datetime import datetime

from modules.Merge import merge_dataset_interpoltion, merge_dataset_cod_conjunto
from modules.interpolation import Interpolation
from modules.connection_manager import read_from_db

from logger import logger


class Dataset:
    def __init__(self, conjunto, pred_year, pred_month):
        load_dotenv()

        self.pred_year = pred_year
        self.pred_month = pred_month
        self.has_error = False

        self.dataset = pd.DataFrame()

        read_from_database = os.getenv("READ_FROM_DATABASE").lower()
        if read_from_database == "yes":
            # READ FROM DATABASE
            table = 'dataset'
            try:
                raw_dataset = read_from_db(table_name=table)
            except Exception as ex:
                logger.info(
                    f"An exception is occurred in reading {table}, {ex}")
                self.has_error = True

            # READ FROM DATABASE
            table = 'conjunto_alimentador'
            try:
                code_df = read_from_db(table_name=table)
            except Exception as ex:
                logger.info(
                    f"An exception is occurred in reading {table}, {ex}")
                self.has_error = True

        else:
            dataset_path = os.getenv("DATASET_PATH")
            dataset_filename = os.getenv("DATASET_FILENAME")
            path = os.path.join(dataset_path, dataset_filename)
            try:
                raw_dataset = pd.read_csv(path)
            except Exception as ex:
                logger.info(f"An exception is occurred, {ex}")
                self.has_error = True

            code_path = os.getenv("CODE_PATH")
            code_filename = os.getenv("CODE_FILENAME")
            path = os.path.join(code_path, code_filename)
            try:
                code_df = pd.read_csv(path)
            except Exception as ex:
                logger.info(f"An exception is occurred, {ex}")
                self.has_error = True

        if not code_df.empty:
            conjunto_feeders = code_df[(
                code_df["sigla_conjunto"] == conjunto)]["alimentador"].tolist()
        else:
            self.has_error = True

        if not raw_dataset.empty and not self.has_error:
            # remove the Nane values from column 'mes'
            raw_dataset.dropna(subset=['mes'], inplace=True)

            # set zero value for column 'vlr_qtd_arvore' in case of Nane values
            values = {'vlr_qtd_arvore': 0}
            raw_dataset.fillna(value=values, inplace=True)

            # remove the Nane values from column 'qtde_cliente_ru'
            raw_dataset.dropna(subset=['qtde_cliente_ru'], inplace=True)

            raw_dataset['ano'] = raw_dataset['ano'].astype(int)
            raw_dataset['mes'] = raw_dataset['mes'].astype(int)

            self.dataset = raw_dataset[(
                raw_dataset["cod_alim"].isin(conjunto_feeders))]

            # filter the dataset based on the prediction date; pred_year and pred_month
            pred_date = datetime(year=self.pred_year,
                                 month=self.pred_month, day=1)
            self.dataset['date'] = pd.to_datetime(self.dataset['ano'].astype(str) +
                                                  self.dataset['mes'].astype(str).str.zfill(2) +
                                                  '01')
            self.dataset = self.dataset[self.dataset['date'] < pred_date]
            sorted_df = self.dataset.sort_values(by='date', ascending=True)
            start_date = sorted_df['date'].iloc[0]
            self.dataset.drop(['date'], axis=1, inplace=True)
            del sorted_df
        else:
            self.has_error = True

        if not self.has_error:
            interpolation = Interpolation(
                conjunto_feeders, start_date, pred_date)
            if not interpolation.has_error:
                interp_out = interpolation.run_interpolation()
            else:
                self.has_error = True

            if not self.dataset.empty and not interp_out.empty:
                self.dataset = merge_dataset_interpoltion(
                    raw_dataset, interp_out)

                self.dataset["DESCR_NOVO"] = conjunto
                self.dataset['feeder_effect'] = self.dataset['qtde_cliente_total'] / \
                    self.dataset['cons_conjunto']
                self.dataset['feeder_effect'] = self.dataset['feeder_effect'].astype(
                    float)

                # giving more weights to the months 10, 11, 12, 1, 2, 3
                month_weights = {1: 2, 2: 2, 3: 2, 4: 1, 5: 1,
                                 6: 1, 7: 1, 8: 1, 9: 1, 10: 2, 11: 2, 12: 2}
                self.dataset['month_weighted'] = self.dataset['mes'].map(
                    month_weights)
            else:
                self.has_error = True
