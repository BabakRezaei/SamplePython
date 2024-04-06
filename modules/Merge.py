import pandas as pd


def merge_dataset_interpoltion(data_df, interpolation_df):

    if not data_df.empty and not interpolation_df.empty:
        out_df = pd.merge(data_df, interpolation_df,
                          how='inner', left_on=['cod_alim', 'ano', 'mes'],
                          right_on=['cod_alim', 'year', 'month']).drop(['year', 'month'], axis=1)

        ordering_cols = ['cod_alim', 'ano', 'mes', 'cons_conjunto', 'dec_cj',
                         'chi_total', 'temp_max', 'temp_min', 'temp_mean',
                         'precipitation', 'humidity_min', 'humidity_mean',
                         'windvelo_mean']

        other_cols = [x for x in out_df.columns.tolist() if x not in ordering_cols]
        df1 = out_df.loc[:, ordering_cols]
        df2 = out_df.loc[:, other_cols]

        final_df = pd.concat([df1, df2], axis=1)

        return final_df
    else:
        return None


def merge_dataset_cod_conjunto(dataset, code_df):
    if not dataset.empty and not code_df.empty:
        merged_conjunto = pd.merge(dataset, code_df,
                                   left_on=['cod_alim'],
                                   right_on=['ALIMENTADOR']).drop(['ALIMENTADOR'], axis=1)
        
        return merged_conjunto
