import pandas as pd
import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta

from modules.Multi_regression_models import MultiRegressors

from logger import logger


# giving more weights to the months 10, 11, 12, 1, 2, 3
month_weights = {1: 2, 2: 2, 3: 2, 4: 1, 5: 1,
                 6: 1, 7: 1, 8: 1, 9: 1, 10: 2, 11: 2, 12: 2}


class Predict:
    def __init__(self, conj_based_df, CHI_INPUT, pred_year, pred_month, months_ahead):
        self.conj_based_df = conj_based_df
        self.chi_goals = CHI_INPUT
        self.feeder_preds = {}
        self.predicted_conj_chi = months_ahead * [0]
        self.pred_year = pred_year
        self.pred_month = pred_month
        self.months_ahead = months_ahead

        self.feeders = self.conj_based_df['cod_alim'].unique().tolist()

        self.feature_cols = list(self.conj_based_df.drop(['cod_alim', 'ano', 'mes', 'chi_total',
                                                          'cons_conjunto', 'qtde_cliente_total',
                                                          'qtde_cliente_ru', 'qtde_cliente_ub',
                                                          'DESCR_NOVO'], axis=1).columns)
        self.target_col = 'chi_total'

        logger.info("Prediction in process ...")
        self.predict()
        logger.info("Prediction process is finished.")
        del self.conj_based_df

    def generate_next_months(self, feeder, feeder_based_df):
        # end_month = feeder_based_df['mes'].iloc[-1]
        # end_year = feeder_based_df['ano'].iloc[-1]
        # pred_date = datetime(year=end_year, month=end_month, day=1)
        # self.feeder_preds[feeder]["end_date"] = end_date
        pred_date = datetime(year=self.pred_year, month=self.pred_month, day=1)
        self.feeder_preds[feeder]["pred_date"] = pred_date

        prediction_df = pd.DataFrame(columns=feeder_based_df.columns.to_list())
        for month_indx in range(self.months_ahead):
            tmp_df = feeder_based_df.tail(1)
            tmp_df['ano'] = pred_date.year
            tmp_df['mes'] = pred_date.month
            tmp_df['month_weighted'] = tmp_df['mes'].map(month_weights)

            # tmp_df['dec_cj'] = self.DEC_INPUT * tmp_df['feeder_effect']
            consumers = self.conj_based_df['cons_conjunto'].iloc[-1]
            # dec_goal = self.DEC_INPUT * tmp_df['feeder_effect'].iloc[-1]
            dec_goal = (self.chi_goals[month_indx] /
                        consumers) * tmp_df['feeder_effect'].iloc[-1]

            tmp_df['chi_total'] = 0

            tmp_df2 = feeder_based_df[feeder_based_df['mes']
                                      == pred_date.month]
            if tmp_df2.shape[0] >= 1:
                dec_from_mean = tmp_df2['dec_cj'].mean()
                dec_feeder = np.mean([dec_goal, dec_from_mean])
                tmp_df['dec_cj'] = dec_feeder
                tmp_df['temp_max'] = tmp_df2['temp_max'].mean()
                tmp_df['temp_min'] = tmp_df2['temp_min'].mean()
                tmp_df['temp_mean'] = tmp_df2['temp_mean'].mean()
                tmp_df['precipitation'] = tmp_df2['precipitation'].mean()
                tmp_df['humidity_min'] = tmp_df2['humidity_min'].mean()
                tmp_df['humidity_mean'] = tmp_df2['humidity_mean'].mean()
                tmp_df['windvelo_mean'] = tmp_df2['windvelo_mean'].mean()

            prediction_df = pd.concat([prediction_df, tmp_df])

            pred_date += relativedelta(months=1)

        prediction_df.reset_index(drop=True, inplace=True)

        return prediction_df

    def predict(self):
        for feeder in self.feeders:
            self.feeder_preds[feeder] = {}
            self.feeder_preds[feeder]['preds'] = {}
            feeder_based_df = self.conj_based_df[self.conj_based_df['cod_alim'] == feeder]

            X_all = feeder_based_df[:][self.feature_cols]
            y_all = feeder_based_df[:][self.target_col]

            num_train = int(len(y_all) * 0.90)
            # num_test = X_all.shape[0] - num_train
            # num_train = len(y_all) - 2
            # num_test = 2
            X_train = X_all[:num_train]
            y_train = y_all[:num_train]
            X_test = X_all[num_train:]
            y_test = y_all[num_train:]

            # train regression models to find the est fit for this feeder
            regModel = MultiRegressors(
                verbose=0, ignore_warnings=True, custom_metric=None)
            models_test, predictions_test = regModel.fit(
                X_train, X_test, y_train, y_test)

            models_test.sort_values(by=['MAPE', 'RMSE'], inplace=True)
            best_model = list(models_test.head(1).index)

            # select the best found model as final regression model
            new_regModel = MultiRegressors(
                regressors=best_model, verbose=0, ignore_warnings=True)

            # generate prediction dataframe for next requested months
            prediction_df = self.generate_next_months(feeder, feeder_based_df)

            # train the found best model with all the historical data and predict the next 2 months
            X_pred = prediction_df[:][self.feature_cols]
            preds = new_regModel.fit_pred(X_all, X_pred, y_all)

            for month in range(self.months_ahead):
                month_index = f"M{month + 1}"
                self.feeder_preds[feeder]['preds'][month_index] = {}

                self.feeder_preds[feeder]['preds'][month_index]['pred'] = preds[month]

                feeder_effect = float(
                    feeder_based_df['feeder_effect'].iloc[-1])
                self.feeder_preds[feeder]['effect'] = feeder_effect

                goal_chi = self.chi_goals[month] * feeder_effect
                self.feeder_preds[feeder]['preds'][month_index]['goal_chi'] = goal_chi

                delta_chi = goal_chi - preds[month]
                self.feeder_preds[feeder]['preds'][month_index]['delta_chi'] = delta_chi

                self.feeder_preds[feeder]['preds'][month_index]['delta_chi_percent'] = round(
                    delta_chi / goal_chi, 2)

                self.predicted_conj_chi[month] += preds[month]
