import pandas as pd
import os
from dotenv import load_dotenv
from dateutil.relativedelta import relativedelta
from datetime import datetime

import modules.common as common
from modules.connection_manager import read_from_db

from logger import logger


class Services:
    def __init__(self, feeders, dataset, conjunto, months_ahead, chi_goals):
        load_dotenv()

        self.has_error = False
        self.suggestions = {}

        self.feeder_preds = feeders
        self.dataset = dataset
        self.conjunto = conjunto
        self.months_ahead = months_ahead
        self.chi_goals = chi_goals

        self.coef_poda = float(os.getenv("COEF_PODA"))
        self.coef_faixa = float(os.getenv("COEF_FAIXA"))
        self.coef_estrutura = float(os.getenv("COEF_ESTRUTURA"))

        read_from_database = os.getenv("READ_FROM_DATABASE").lower()
        if read_from_database == "yes":
            # READ FROM DATABASE
            table = 'predit_interrupcoes'
            try:
                if len(self.feeder_preds) == 1:
                    feeder_key = list(self.feeder_preds.keys())[0]
                    condition_str = f"WHERE cod_alim='{feeder_key}'"
                else:
                    feeders_keys = tuple([k for k in self.feeder_preds.keys()])
                    condition_str = f"WHERE cod_alim IN {feeders_keys}"

                self.interruptions = read_from_db(table_name=table,
                                                  condition_str=condition_str)
            except Exception as ex:
                logger.info(
                    f"An exception is occurred in reading {table}, {ex}")
                self.has_error = True

            # READ FROM DATABASE
            table = 'predit_causa_interrupcoes_grupo'
            try:
                self.causes_df = read_from_db(table_name=table)
                self.causes_df[["grupo"]] = self.causes_df[["grupo"]].apply(
                    common.replace_wild_characters, axis=0)
            except Exception as ex:
                logger.info(
                    f"An exception is occured in reading {table}, {ex}")
                self.has_error = True

            # READ FROM DATABASE
            table = 'predit_servicos_manutencao'
            try:
                if len(self.feeder_preds) == 1:
                    feeder_key = list(self.feeder_preds.keys())[0]
                    condition_str = f"WHERE cod_alim='{feeder_key}'"
                else:
                    feeders_keys = tuple([k for k in self.feeder_preds.keys()])
                    condition_str = f"WHERE cod_alim IN {feeders_keys}"

                columns = ["us_pontos", "vlr_us_servico", "cod_alim",
                           "dat_execucao", "grupo_dataset_servico"]
                self.manutencao = read_from_db(table_name=table,
                                               condition_str=condition_str,
                                               column_list=columns)
            except Exception as ex:
                logger.info(
                    f"An exception is occurred in reading {table}, {ex}")
                self.has_error = True

        else:
            # READ FROM CSV FILE
            interrup_path = os.getenv("INTERRUPTION_PATH")
            interrup_data_file = os.getenv("INTERRUPTION_DATA_FILENAME")
            path = os.path.join(interrup_path, interrup_data_file)
            try:
                self.interruptions = pd.read_csv(path)
            except Exception as ex:
                logger.info(f"An exception is occurred, {ex}")
                self.has_error = True

            # READ FROM CSV FILE
            interrup_group_file = os.getenv("INTERRUPTION_GROUPS_FILENAME")
            path = os.path.join(interrup_path, interrup_group_file)
            try:
                self.causes_df = pd.read_csv(path)
                self.causes_df[["grupo"]] = self.causes_df[["grupo"]].apply(
                    common.replace_wild_characters, axis=0)
            except Exception as ex:
                logger.info(f"An exception is occured, {ex}")
                self.has_error = True

        if not self.has_error:
            logger.info("Generating suggestions in process ...")
            self.find_chi_values()
            logger.info("Generating suggestions is finished.")
            del self.interruptions
            del self.manutencao
            del self.causes_df
            del self.dataset

    def find_chi_values(self):
        for feeder in self.feeder_preds.keys():
            end_date = self.feeder_preds[feeder]["pred_date"]
            start_date = end_date - relativedelta(months=12)

            feeder_interups = self.interruptions[self.interruptions["cod_alim"] == feeder]
            df_cols = [common.replace_wild_characters_str(
                col) for col in list(feeder_interups.columns)]
            feeder_interups.columns = df_cols

            # convert the date column to datetime format
            feeder_interups["dat_inicio"] = pd.to_datetime(
                feeder_interups["dat_inicio"])
            feeder_based = feeder_interups[(feeder_interups["dat_inicio"] >= start_date) & (
                feeder_interups["dat_inicio"] < end_date)]
            feeder_based = pd.merge(feeder_based, self.causes_df,
                                    left_on=["cod_grupo", "cod_causa"],
                                    right_on=["num_idt_grupo_causas", "num_idt_causa_interrupcao"]).drop(["num_idt_grupo_causas", "num_idt_causa_interrupcao"], axis=1)

            # define the required variables for output
            chi_poda = 0
            chi_faixa = 0
            chi_estruturas = 0
            cnt_poda = 0
            cnt_faixa = 0
            cnt_estruturas = 0

            chi_total = feeder_based["chi_tot"].sum()
            cnt_total = feeder_based["id_interrupcao"].nunique()

            tmp_df = feeder_based[feeder_based["grupo"] == "arvore"]
            if not tmp_df.empty:
                tmp_df2 = tmp_df[tmp_df["tag_urb_rur"] == "U"]
                if not tmp_df2.empty:
                    tmp_val = tmp_df2["chi_tot"].sum()
                    id_count = tmp_df2["id_interrupcao"].nunique()
                    cnt_poda += id_count
                    chi_poda += tmp_val
                tmp_df2 = tmp_df[tmp_df["tag_urb_rur"] == "R"]
                if not tmp_df2.empty:
                    tmp_val = tmp_df2["chi_tot"].sum()
                    id_count = tmp_df2["id_interrupcao"].nunique()
                    cnt_faixa += id_count
                    chi_faixa += tmp_val

            tmp_df = feeder_based[feeder_based["grupo"]
                                  == "manutencao estruturas"]
            if not tmp_df.empty:
                tmp_val = tmp_df["chi_tot"].sum()
                id_count = tmp_df["id_interrupcao"].nunique()
                cnt_estruturas += id_count
                chi_estruturas += tmp_val

            tmp_df = feeder_based[feeder_based["grupo"]
                                  == f"50% avores/ 50% outros"]
            if not tmp_df.empty:
                tmp_df2 = tmp_df[tmp_df["tag_urb_rur"] == "U"]
                if not tmp_df2.empty:
                    tmp_val = tmp_df2["chi_tot"].sum()
                    id_count = tmp_df2["id_interrupcao"].nunique()
                    cnt_poda += int(id_count / 2)
                    chi_poda += tmp_val / 2
                tmp_df2 = tmp_df[tmp_df["tag_urb_rur"] == "R"]
                if not tmp_df2.empty:
                    tmp_val = tmp_df2["chi_tot"].sum()
                    id_count = tmp_df2["id_interrupcao"].nunique()
                    cnt_faixa += int(id_count / 2)
                    chi_faixa += tmp_val / 2

            tmp_df = feeder_based[feeder_based["grupo"]
                                  == f"70% arvore / 30% outros"]
            if not tmp_df.empty:
                tmp_df2 = tmp_df[tmp_df["tag_urb_rur"] == "U"]
                if not tmp_df2.empty:
                    tmp_val = tmp_df2["chi_tot"].sum()
                    id_count = tmp_df2["id_interrupcao"].nunique()
                    cnt_poda += int(id_count * 0.7)
                    chi_poda += tmp_val * 0.7
                tmp_df2 = tmp_df[tmp_df["tag_urb_rur"] == "R"]
                if not tmp_df2.empty:
                    tmp_val = tmp_df2["chi_tot"].sum()
                    id_count = tmp_df2["id_interrupcao"].nunique()
                    cnt_faixa += int(id_count * 0.7)
                    chi_faixa += tmp_val * 0.7

            # calculating the average of each service for 1 year
            output = {}
            if cnt_poda == 0:
                chi_poda = 0
            else:
                chi_poda = chi_poda / cnt_poda

            if cnt_faixa == 0:
                chi_faixa = 0
            else:
                chi_faixa = chi_faixa / cnt_faixa

            if cnt_estruturas == 0:
                chi_estruturas = 0
            else:
                chi_estruturas = chi_estruturas / cnt_estruturas

            # generating dates for suggestion period
            from_date = end_date
            from_text = f"{from_date.month}/{from_date.year}"
            to_date = end_date + relativedelta(months=(self.months_ahead - 1))
            to_text = f"{to_date.month}/{to_date.year}"

            # suggestion initialization for each feeder
            self.suggestions[feeder] = {
                "conjunto": self.conjunto,
                "from": from_text,
                "to": to_text,
                "poda_sugg": 0,
                "faixa_sugg": 0,
                "estruturas_sugg": 0,
            }

            output["poda_sugg"] = [cnt_poda, chi_poda]
            output["faixa_sugg"] = [cnt_faixa, chi_faixa]
            output["estruturas_sugg"] = [cnt_estruturas, chi_estruturas]
            output["total"] = [cnt_total, chi_total]

            dataset_feeder = self.dataset[self.dataset["cod_alim"] == feeder]
            suggestions = self.generate_suggestions(
                dataset_feeder, output, self.feeder_preds[feeder])

            self.suggestions[feeder]["poda_sugg"] = round(suggestions["poda_sugg"] * self.coef_poda)
            self.suggestions[feeder]["faixa_sugg"] = round(suggestions["faixa_sugg"] * self.coef_faixa)
            self.suggestions[feeder]["estruturas_sugg"] = round(suggestions["estruturas_sugg"] * self.coef_estrutura)
            self.suggestions[feeder]["chi_pred"] = suggestions["chi_pred"]
            self.suggestions[feeder]["chi_goal"] = suggestions["chi_goal"]
            self.suggestions[feeder]["chi_diff"] = suggestions["chi_diff"]

            # calculations for finding the average and maximum us services from last three years
            self.statistics_36_months_us(feeder, end_date)

            # calculations for finding the average and maximum unique numbers of interruptions from last three years
            self.statistics_36_months_interups(
                feeder, end_date, feeder_interups)

    def generate_suggestions(self, dataset_feeder, service_chi, feeder_pred):
        total_prev_preds = 0
        for indx in range(1, self.months_ahead + 1):
            month_key = f"M{indx}"
            total_prev_preds += feeder_pred['preds'][month_key]["pred"]

        if self.months_ahead != 12:
            pred_date = feeder_pred["pred_date"]
            start_date = pred_date - \
                relativedelta(months=(12-self.months_ahead))

            dataset_feeder["date"] = pd.to_datetime(dataset_feeder["ano"].astype(str) +
                                                    dataset_feeder["mes"].astype(str).str.zfill(2) + '01')

            # we need this column in the original dataset. Here just added for test purposes
            dataset_feeder["chi_pred"] = dataset_feeder["chi_total"]

            tmp = dataset_feeder[dataset_feeder["date"] >= start_date]
            chi_pred = tmp["chi_pred"].sum()
            chi_pred += total_prev_preds
        else:
            chi_pred = total_prev_preds

        chi_goal = sum(self.chi_goals) * feeder_pred['effect']
        diff = chi_pred - chi_goal

        suggestions = {}
        suggestions["chi_pred"] = round(chi_pred, 2)
        suggestions["chi_goal"] = round(chi_goal, 2)
        suggestions["chi_diff"] = round(diff, 2)

        for service in service_chi.keys():
            if diff > 0 and service_chi["total"][0] > 0:
                if service != "total":
                    percentage = service_chi[service][0] / \
                        service_chi["total"][0]

                    if service_chi[service][1] != 0:
                        service_number = int(
                            diff * percentage / service_chi[service][1])
                    else:
                        service_number = 0
            else:
                service_number = 0
            suggestions[service] = service_number

        return suggestions

    def statistics_36_months_us(self, feeder, end_date):
        final_year = end_date.year
        # define the required variables for output
        podas = []
        faixas = []
        estruturas = []

        feeder_based = self.manutencao[self.manutencao["cod_alim"] == feeder]
        feeder_based["dat_execucao"] = pd.to_datetime(
            feeder_based["dat_execucao"])
        feeder_based.fillna(0, inplace=True)

        # loop for 3 years
        for i in range(3, -1, -1):
            start_period_year = datetime(year=final_year - i, month=1, day=1)
            end_period_year = datetime(year=final_year - i + 1, month=1, day=1)

            feeder_yearly = feeder_based[(feeder_based["dat_execucao"] >= start_period_year) &
                                         (feeder_based["dat_execucao"] < end_period_year)]

            calc_year = start_period_year.year

            tmp_df = feeder_yearly[feeder_yearly["grupo_dataset_servico"] == "PODA"]
            sum1 = tmp_df["us_pontos"].sum()
            sum2 = tmp_df["vlr_us_servico"].sum()
            total = sum1 + sum2
            podas.append((total, calc_year))

            tmp_df = feeder_yearly[feeder_yearly["grupo_dataset_servico"] == "FAIXA"]
            sum1 = tmp_df["us_pontos"].sum()
            sum2 = tmp_df["vlr_us_servico"].sum()
            total = sum1 + sum2
            faixas.append((total, calc_year))

            tmp_df = feeder_yearly[feeder_yearly["grupo_dataset_servico"]
                                   == "ESTRUTURA"]
            sum1 = tmp_df["us_pontos"].sum()
            sum2 = tmp_df["vlr_us_servico"].sum()
            total = sum1 + sum2
            estruturas.append((total, calc_year))

        poda_avg = round(sum([x[0] for x in podas[:3]]) / 3, 2)
        self.suggestions[feeder]["poda_avg_us"] = round(poda_avg, 2)
        self.suggestions[feeder]["poda_year-3_us"] = round(podas[0][0], 2)
        self.suggestions[feeder]["poda_year-2_us"] = round(podas[1][0], 2)
        self.suggestions[feeder]["poda_year-1_us"] = round(podas[2][0], 2)
        self.suggestions[feeder]["poda_cur_year_us"] = round(podas[3][0], 2)

        podas.sort(key=lambda x: x[0], reverse=True)
        self.suggestions[feeder]["poda_max_us"] = round(podas[0][0], 2)
        self.suggestions[feeder]["poda_max_year_us"] = round(podas[0][1], 2)

        faixa_avg = round(sum([x[0] for x in faixas[:3]]) / 3, 2)
        self.suggestions[feeder]["faixa_avg_us"] = round(faixa_avg, 2)
        self.suggestions[feeder]["faixa_year-3_us"] = round(faixas[0][0], 2)
        self.suggestions[feeder]["faixa_year-2_us"] = round(faixas[1][0], 2)
        self.suggestions[feeder]["faixa_year-1_us"] = round(faixas[2][0], 2)
        self.suggestions[feeder]["faixa_cur_year_us"] = round(faixas[3][0], 2)

        faixas.sort(key=lambda x: x[0], reverse=True)
        self.suggestions[feeder]["faixa_max_us"] = round(faixas[0][0], 2)
        self.suggestions[feeder]["faixa_max_year_us"] = round(faixas[0][1], 2)

        estruturas_avg = round(sum([x[0] for x in estruturas[:3]]) / 3, 2)
        self.suggestions[feeder]["estruturas_avg_us"] = round(
            estruturas_avg, 2)
        self.suggestions[feeder]["estruturas_year-3_us"] = round(
            estruturas[0][0], 2)
        self.suggestions[feeder]["estruturas_year-2_us"] = round(
            estruturas[1][0], 2)
        self.suggestions[feeder]["estruturas_year-1_us"] = round(
            estruturas[2][0], 2)
        self.suggestions[feeder]["estruturas_cur_year_us"] = round(
            estruturas[3][0], 2)

        estruturas.sort(key=lambda x: x[0], reverse=True)
        self.suggestions[feeder]["estruturas_max_us"] = round(
            estruturas[0][0], 2)
        self.suggestions[feeder]["estruturas_max_year_us"] = round(
            estruturas[0][1], 2)

    def statistics_36_months_interups(self, feeder, end_date, feeder_interups):
        final_year = end_date.year
        # define the required variables for output
        podas = []
        faixas = []
        estruturas = []

        # loop for 3 years
        for i in range(3, 0, -1):
            start_period_year = datetime(year=final_year - i, month=1, day=1)
            end_period_year = datetime(year=final_year - i + 1, month=1, day=1)

            feeder_based = feeder_interups[(feeder_interups["dat_inicio"] >= start_period_year) &
                                           (feeder_interups["dat_inicio"] < end_period_year)]

            feeder_based =\
                pd.merge(feeder_based, self.causes_df,
                         left_on=["cod_grupo", "cod_causa"],
                         right_on=["num_idt_grupo_causas",
                                   "num_idt_causa_interrupcao"]).drop(["num_idt_grupo_causas",
                                                                       "num_idt_causa_interrupcao"], axis=1)

            # define the required variables for output
            cnt_poda = 0
            cnt_faixa = 0
            cnt_estruturas = 0

            tmp_df = feeder_based[feeder_based["grupo"] == "arvore"]
            if not tmp_df.empty:
                tmp_df2 = tmp_df[tmp_df["tag_urb_rur"] == "U"]
                if not tmp_df2.empty:
                    id_count = tmp_df2["id_interrupcao"].nunique()
                    cnt_poda += id_count
                tmp_df2 = tmp_df[tmp_df["tag_urb_rur"] == "R"]
                if not tmp_df2.empty:
                    id_count = tmp_df2["id_interrupcao"].nunique()
                    cnt_faixa += id_count

            tmp_df = feeder_based[feeder_based["grupo"]
                                  == "manutencao estruturas"]
            if not tmp_df.empty:
                id_count = tmp_df["id_interrupcao"].nunique()
                cnt_estruturas += id_count

            tmp_df = feeder_based[feeder_based["grupo"]
                                  == f"50% avores/ 50% outros"]
            if not tmp_df.empty:
                tmp_df2 = tmp_df[tmp_df["tag_urb_rur"] == "U"]
                if not tmp_df2.empty:
                    id_count = tmp_df2["id_interrupcao"].nunique()
                    cnt_poda += int(id_count / 2)
                tmp_df2 = tmp_df[tmp_df["tag_urb_rur"] == "R"]
                if not tmp_df2.empty:
                    id_count = tmp_df2["id_interrupcao"].nunique()
                    cnt_faixa += int(id_count / 2)

            tmp_df = feeder_based[feeder_based["grupo"]
                                  == f"70% arvore / 30% outros"]
            if not tmp_df.empty:
                tmp_df2 = tmp_df[tmp_df["tag_urb_rur"] == "U"]
                if not tmp_df2.empty:
                    id_count = tmp_df2["id_interrupcao"].nunique()
                    cnt_poda += int(id_count * 0.7)
                tmp_df2 = tmp_df[tmp_df["tag_urb_rur"] == "R"]
                if not tmp_df2.empty:
                    id_count = tmp_df2["id_interrupcao"].nunique()
                    cnt_faixa += int(id_count * 0.7)

            calc_year = start_period_year.year
            podas.append((cnt_poda, calc_year))
            faixas.append((cnt_faixa, calc_year))
            estruturas.append((cnt_estruturas, calc_year))

        poda_avg = round(sum([x[0] for x in podas]) / 3, 2)
        self.suggestions[feeder]["poda_avg_interup"] = round(poda_avg, 2)
        self.suggestions[feeder]["poda_year-3_interup"] = round(podas[0][0], 2)
        self.suggestions[feeder]["poda_year-2_interup"] = round(podas[1][0], 2)
        self.suggestions[feeder]["poda_year-1_interup"] = round(podas[2][0], 2)

        podas.sort(key=lambda x: x[0], reverse=True)
        self.suggestions[feeder]["poda_max_interup"] = round(podas[0][0], 2)
        self.suggestions[feeder]["poda_max_year_interup"] = round(
            podas[0][1], 2)

        faixa_avg = round(sum([x[0] for x in faixas]) / 3, 2)
        self.suggestions[feeder]["faixa_avg_interup"] = round(faixa_avg, 2)
        self.suggestions[feeder]["faixa_year-3_interup"] = round(
            faixas[0][0], 2)
        self.suggestions[feeder]["faixa_year-2_interup"] = round(
            faixas[1][0], 2)
        self.suggestions[feeder]["faixa_year-1_interup"] = round(
            faixas[2][0], 2)

        faixas.sort(key=lambda x: x[0], reverse=True)
        self.suggestions[feeder]["faixa_max_interup"] = round(faixas[0][0], 2)
        self.suggestions[feeder]["faixa_max_year_interup"] = round(
            faixas[0][1], 2)

        estruturas_avg = round(sum([x[0] for x in estruturas]) / 3, 2)
        self.suggestions[feeder]["estruturas_avg_interup"] = round(
            estruturas_avg, 2)
        self.suggestions[feeder]["estruturas_year-3_interup"] = round(
            estruturas[0][0], 2)
        self.suggestions[feeder]["estruturas_year-2_interup"] = round(
            estruturas[1][0], 2)
        self.suggestions[feeder]["estruturas_year-1_interup"] = round(
            estruturas[2][0], 2)

        estruturas.sort(key=lambda x: x[0], reverse=True)
        self.suggestions[feeder]["estruturas_max_interup"] = round(
            estruturas[0][0], 2)
        self.suggestions[feeder]["estruturas_max_year_interup"] = round(
            estruturas[0][1], 2)
