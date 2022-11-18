import pandas as pd
from config import herccrt, mosaic

hrc = herccrt().con()


dow_map_x = {
  1:1,
  2:0.809,
  3:0.381,
  4:0.037,
  5:0.037,
  6:0.381,
  7:0.809
}
dow_map_y = {
  1:0.494,
  2:0.89,
  3:0.987,
  4:0.713,
  5:0.274,
  6:0,
  7:0.098
}

wk_map_x = {1: 0.99889441,
 2: 0.99118029,
 3: 0.976378386,
 4: 0.954703312,
 5: 0.926466233,
 6: 0.892075869,
 7: 0.852027483,
 8: 0.806898384,
 9: 0.75734042,
 10: 0.704067472,
 11: 0.647848951,
 12: 0.58949479,
 13: 0.529846938,
 14: 0.469765855,
 15: 0.410118003,
 16: 0.351763842,
 17: 0.295545321,
 18: 0.242272373,
 19: 0.192714409,
 20: 0.147585311,
 21: 0.107536925,
 22: 0.07314656,
 23: 0.044909482,
 24: 0.023234407,
 25: 0.008432503,
 26: 0.000718384,
 27: 0.000202608,
 28: 0.006892681,
 29: 0.02069255,
 30: 0.041403109,
 31: 0.068725199,
 32: 0.10226511,
 33: 0.141539082,
 34: 0.185980313,
 35: 0.234947962,
 36: 0.287736151,
 37: 0.343582473,
 38: 0.401682498,
 39: 0.46119828,
 40: 0.521270358,
 41: 0.581032771,
 42: 0.639624059,
 43: 0.69619827,
 44: 0.749939968,
 45: 0.800073739,
 46: 0.845876698,
 47: 0.886688492,
 48: 0.921919805,
 49: 0.951062367,
 50: 0.973696454,
 51: 0.989495891,
 52: 0.998231556,
 53: 0.998}

wk_map_y = {1: 0.534132227,
 2: 0.593716044,
 3: 0.651945139,
 4: 0.707980061,
 5: 0.761012381,
 6: 0.810276688,
 7: 0.855063604,
 8: 0.894726284,
 9: 0.928692922,
 10: 0.956473257,
 11: 0.977667074,
 12: 0.991968211,
 13: 0.999170557,
 14: 0.999170557,
 15: 0.991968211,
 16: 0.977667074,
 17: 0.956473257,
 18: 0.928692922,
 19: 0.894726284,
 20: 0.855063604,
 21: 0.810276688,
 22: 0.761012381,
 23: 0.707980061,
 24: 0.651945139,
 25: 0.593716044,
 26: 0.534132227,
 27: 0.474053144,
 28: 0.414345761,
 29: 0.355871035,
 30: 0.299472418,
 31: 0.245963344,
 32: 0.196115226,
 33: 0.150647946,
 34: 0.110216356,
 35: 0.075404266,
 36: 0.046713445,
 37: 0.024557614,
 38: 0.009256943,
 39: 0.001031051,
 40: 0.0,
 41: 0.006177299,
 42: 0.0194749,
 43: 0.0397007,
 44: 0.066562544,
 45: 0.099673226,
 46: 0.138554988,
 47: 0.182648034,
 48: 0.231315022,
 49: 0.283854578,
 50: 0.339509298,
 51: 0.397475752,
 52: 0.456917994,
 53: 0.457}

DOW_DAY_NAMES = [
    (1, "monday"),
    (2, "tuesday"),
    (3, "wednesday"),
    (4, "thursday"),
    (5, "friday"),
    (6, "saturday"),
    (7, "sunday"),
]

DEFAULT_POOL_RASM_H2 = 1.0
DEFAULT_POOL_RASM_HL = 0.0



def dow_binary(row, current_dow, column_label):
    """
        Returns 1 if the current_dow matches the value in column_label
        df['monday'] = df.apply(dow, axis=1, args=(1,'forecastDayOfWeek'))
        We also use this for the Holiday
    :param row: implicitly passed from apply
    :param current_dow: day-of-week to compare to
    :param column_label: which column we're looking at in DataFrame
    :return: int 1 if column matches the numeric
    """
    if row[column_label] == current_dow:
        return 1
    return 0


def week_of_year_binary(row, week_number_column):
    week_num = row[week_number_column]
    week_col = f"week_{week_num}"
    if week_col in row:
        row[week_col] = 1
    return row


# TODO: Optimize this, it's really slow for PA
def add_day_columns(data_frame, dow_column):
    """
     Add columns for each day of the week. Set value to 1 if it matches
     the dow_column name
    :param data_frame:
    :param dow_column:
    :return:
    """
    for val in DOW_DAY_NAMES:
        (day_number, day_name) = val
        data_frame[day_name] = data_frame.apply(dow_binary, axis=1, args=(day_number, dow_column))

    return True

def add_week_binary(data_frame, source_column):
    """
    add columns week_1 .. week_53 to dataframe
    We set the matching week column to 1
    :param data_frame:
    :param source_column: name of column containing week number
    :return:
    """
    # Columns week_1 .. week_53 for binary columns of departure week-number
    WEEK_COLUMNS = [f"week_{i+1}" for i in range(53)]
    # Add the new columns with default 0
    for week in WEEK_COLUMNS:
        data_frame[week] = 0

    # we can't modify data_frame in place for "apply" so returning a new df
    new_df = data_frame.apply(week_of_year_binary, axis=1, args=(source_column,))
    return new_df


def add_forecast_departure_date(data_frame):
    # Calculate forecastDepartureDate
    data_frame["forecastDepartureDate"] = data_frame.apply(
        compute_forecast_departure_date,
        axis=1,
        args=("flightDepartureDate", "forecastDayOfWeek"),
    )


def add_holiday_features(df):
    df["holiday"] = df.apply(lambda x: 1 if x.poolCode != 'M' else 0, axis=1)
    HOLIDAY_POOL_CODES = ["H1", "H2", "H3", "HL"]
    # We use the dow_binary function to identify pool codes rather than day-of-week
    for holiday_pool_code in HOLIDAY_POOL_CODES:
        df[holiday_pool_code] = df.apply(dow_binary, axis=1, args=(holiday_pool_code, "poolCode"))

def multi_index_pivot(df, columns=None, values=None, flatten=False):
    """
    Pivot a pandas data frame from long to wide format on multiple index variables.
    Copied from: https://github.com/pandas-dev/pandas/issues/23955
    Note: you can perform the opposite operation, i.e. unpivot a DataFrame from
    wide format to long format with df.melt().
    In contrast to `pivot`, `melt` does accept a multiple index specified
    as the `id_vars` argument.
    Otherwise the error message is cryptic:
    KeyError: "None of [Index([None], dtype='object')] are in the [columns]"
    TODO: add warning when there is no index set.
    Usage:
        >>> df.multiindex_pivot(index   = ['idx_column1', 'idx_column2'],
        >>>                     columns = ['col_column1', 'col_column2'],
        >>>                     values  = 'bar')
    """
    names = list(df.index.names)
    df = df.reset_index()
    list_index = df[names].values
    tuples_index = [tuple(i) for i in list_index]  # hashable
    df = df.assign(tuples_index=tuples_index)
    df = df.pivot(index="tuples_index", columns=columns, values=values)
    df.index = pd.MultiIndex.from_tuples(df.index, names=names)
    # Remove confusing index column name #
    df.columns.name = None
    df = df.reset_index()

    if flatten:
        # Collapse down to a single level
        df.columns.to_flat_index()

        # Fix column names so they're not tuples
        column_list = []
        for c in df.columns:
            column_suffix = "" if not str(c[1]) else "_" + str(c[1])
            column_list.append(f"{c[0]}{column_suffix}")
        # replace all column names with our new list
        df.columns = column_list

    return df

def pull_data(orig,dest,fcst_id,new_market):
    
    if fcst_id == -1:
        query = f"""SELECT /*+PARALLEL(8)*/  TO_CHAR(FLT_DPTR_DATE, 'YYYY-MM-DD') FLT_DPTR_DATE,  
        FCST_CLS, 
        CABIN_CODE,
        LCL_FLW_IND, 
        FCST_PERIOD, 
        FRAC_CLOSURE, 
        FRAC_CLOSURE_BELOW, 
        TRAFFIC_CT, 
        NVL(TRAFFIC_CT_AADV, 0) TRAFFIC_CT_AADV,
        POOL_CD, 
        DOW, 
        nvl(FCST_ID,0) FCST_ID,
        FLT_ID, 
        POS_IND, 
        TO_CHAR(TRUNC(SYSDATE),'YYYY-MM-DD') SNAPSHOT_DATE
        FROM fcst_history_v
        WHERE 1=1
        and LEG_ORIG = '{orig}'
        and leg_dest = '{dest}'
        AND BAD_HIST_IND='N' 
        AND CABIN_CODE = 'Y'
        and dow in (1,2,3,4,5,6,7) 
        and POOL_CD != 'I'
        """
        input_df = pd.read_sql(query, con=hrc)
    else:
        if new_market == False:
            query = f"""SELECT /*+PARALLEL(8)*/  TO_CHAR(FLT_DPTR_DATE, 'YYYY-MM-DD') FLT_DPTR_DATE,  
            FCST_CLS, 
            CABIN_CODE,
            LCL_FLW_IND, 
            FCST_PERIOD, 
            FRAC_CLOSURE, 
            FRAC_CLOSURE_BELOW, 
            TRAFFIC_CT, 
            NVL(TRAFFIC_CT_AADV, 0) TRAFFIC_CT_AADV,
            POOL_CD, 
            DOW, 
            nvl(FCST_ID,0) FCST_ID,
            FLT_ID, 
            POS_IND, 
            TO_CHAR(TRUNC(SYSDATE),'YYYY-MM-DD') SNAPSHOT_DATE
            FROM fcst_history_v
            WHERE 1=1
            and LEG_ORIG = '{orig}'
            and leg_dest = '{dest}'
            AND fcst_id = {fcst_id} 
            AND BAD_HIST_IND='N' 
            AND CABIN_CODE = 'Y'
            and dow in (1,2,3,4,5,6,7) 
            and POOL_CD != 'I'
            """
            input_df = pd.read_sql(query, con=hrc)
        if new_market == True:
            query = f"""SELECT /*+PARALLEL(8)*/  TO_CHAR(FLT_DPTR_DATE, 'YYYY-MM-DD') FLT_DPTR_DATE,  
            FCST_CLS, 
            CABIN_CODE,
            LCL_FLW_IND, 
            FCST_PERIOD, 
            FRAC_CLOSURE, 
            FRAC_CLOSURE_BELOW, 
            TRAFFIC_CT, 
            NVL(TRAFFIC_CT_AADV, 0) TRAFFIC_CT_AADV,
            POOL_CD, 
            DOW, 
            nvl(FCST_ID,0) FCST_ID,
            FLT_ID, 
            POS_IND, 
            TO_CHAR(TRUNC(SYSDATE),'YYYY-MM-DD') SNAPSHOT_DATE
            FROM fcst_history_v
            WHERE 1=1
            and LEG_ORIG = '{orig}'
            and leg_dest = '{dest}'
            --AND fcst_id = {fcst_id} 
            AND BAD_HIST_IND='N' 
            AND CABIN_CODE = 'Y'
            and dow in (1,2,3,4,5,6,7) 
            and POOL_CD != 'I'
            """
            input_df = pd.read_sql(query, con=hrc)

    input_df.columns = ['flightDepartureDate','forecastClass','cabinCode','localFlowIndicator',
                    'forecastPeriod','fracClosure','fracClosureBelow',
                    'trafficCount','trafficCountAadv','poolCode',
                    'forecastDayOfWeek','forecastId','flightId','POS','snapshotDate']

    input_df['fracClosure'] = pd.to_numeric(input_df['fracClosure'])
    input_df['trafficCount'] = pd.to_numeric(input_df['trafficCount'])
    input_df['trafficCountAadv'] = pd.to_numeric(input_df['trafficCountAadv'])

    input_df["trafficSum"] = input_df.trafficCount + input_df.trafficCountAadv
    input_df['origin'] = orig
    input_df['destination'] = dest
    input_df['forecastDepartureDate'] = input_df.flightDepartureDate


    groupby_columns_pos = [
      "snapshotDate",
      "origin",
      "destination",
      "forecastId",
      "forecastDepartureDate",
      "forecastDayOfWeek",
      "poolCode",
      "cabinCode",
      "forecastPeriod",
      "forecastClass",
      "localFlowIndicator",
      "flightId",
      "flightDepartureDate",
    ]

    groupby_columns_flight_id = [
      "snapshotDate",
      "origin",
      "destination",
      "forecastId",
      "forecastDepartureDate",
      "forecastDayOfWeek",
      "poolCode",
      "cabinCode",
      "forecastPeriod",
      "forecastClass",
      "localFlowIndicator",
      "flightDepartureDate",
    ]



    # generate mean of frac closure & sum of traffic across points of sale
    agg_columns = {"fracClosure": "mean", "trafficSum": "sum"}
    agg_columns_flight_id = {"fracClosure": "mean", "trafficSum": "mean"}

    agg_columns["trafficCountAadv"] = "sum"
    agg_columns_flight_id["trafficCountAadv"] = "mean"

    ret_data = input_df.groupby(groupby_columns_pos, as_index=False).agg(agg_columns)

    # Group again to combine across multiple flight-id's (if they exist)
    ret_data = ret_data.groupby(groupby_columns_flight_id, as_index=False).agg(agg_columns_flight_id)

    # Rename column
    ret_data = ret_data.rename(columns={"trafficSum": "trafficActual"})

    ret_data = ret_data.rename(columns={"trafficCountAadv": "trafficActualAadv"})

    DATA_INDEX = [
        "snapshotDate",
        "origin",
        "destination",
        "forecastId",
        "forecastDepartureDate",
        "forecastDayOfWeek",
        "poolCode",
        "cabinCode",
        "forecastPeriod",
        "localFlowIndicator",
        "flightDepartureDate",
    ]

    # Group input data by PoS & FlightId to compute traffic/closure sum & average
    # input_df = self.aggregate_input_data(self.data_frame)

    ret_data.set_index(DATA_INDEX, inplace=True)

    pivot_value_columns = ["fracClosure", "trafficActual", "trafficActualAadv"]

    df = multi_index_pivot(
      ret_data,
      columns="forecastClass",
      values=pivot_value_columns,
      flatten=True,
    )

    add_holiday_features(df)
    df['weekNumber'] = pd.DatetimeIndex(df['forecastDepartureDate']).week
    df['week_x'] = df['weekNumber'].map(wk_map_x)
    df['week_y'] = df['weekNumber'].map(wk_map_y)
#     add_day_columns(df, 'forecastDayOfWeek')
#     df = add_week_binary(df, "weekNumber")
    df['dow_x'] = df['forecastDayOfWeek'].map(dow_map_x)
    df['dow_y'] = df['forecastDayOfWeek'].map(dow_map_y)


    return(df)

def pull_seas(df,orig,dest):
    week_query = f"""SELECT /*+PARALLEL(8)*/ *
    FROM OR_LOAD.KRONOS_WEEK_SEASONALITY
    where 1=1
    and leg_orig = '{orig}' and leg_dest = '{dest}' 
    and cabin_code = 'Y'
    """
    week_seas = pd.read_sql(week_query, con=hrc)
    week_seas.columns = ['origin','destination','cabinCode','localFlowIndicator','weekNumber','avgtraffic',
                      'avgtrafficopenness','avgrasm']

    dow_query = f"""SELECT /*+PARALLEL(8)*/ *
    FROM OR_LOAD.KRONOS_DOW_SEASONALITY
    where 1=1
    and leg_orig = '{orig}' and leg_dest = '{dest}' 
    and cabin_code = 'Y'
    """

    dow_seas = pd.read_sql(dow_query, con=hrc)
    dow_seas.columns = ['origin','destination','cabinCode','localFlowIndicator','forecastDayOfWeek','dowavgtraffic',
                      'dowavgtrafficopenness','dowavgrasm']

    pool_query = f"""SELECT /*+PARALLEL(8)*/ *
    FROM OR_LOAD.KRONOS_POOL_SEASONALITY
    where 1=1
    and leg_orig = '{orig}' and leg_dest = '{dest}' 
    and cabin_code = 'Y'
    """

    pool_seas = pd.read_sql(pool_query, con=hrc)
    pool_seas.columns = ['origin','destination','cabinCode','poolCode','poolrasm']

    df['weekNumber'] = pd.DatetimeIndex(df['forecastDepartureDate']).week
    df = pd.merge(df, week_seas, on=['origin','destination','cabinCode','localFlowIndicator','weekNumber'])
    df = pd.merge(df, dow_seas, on=['origin','destination','cabinCode','localFlowIndicator','forecastDayOfWeek'])
#     df = pd.merge(df, pool_seas, on=['origin','destination','cabinCode','poolCode'])

    return(df)