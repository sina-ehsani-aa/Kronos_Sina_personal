import datetime as dt
from collections import defaultdict
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from sklearn.preprocessing import minmax_scale

# ---------- Data Pulling (OAG, AA):


def find_all_dest_given_leg(orig, hcrt):
    """Finds all destination cities given a orig code:

    Args:
        orig (string): Origen Airport Code
        hcrt (cx_Oracle.Connection): herccrt().con()

    Returns:
        list: list of all Destination flying from the given orig
    """

    fcst_id_qry = f"""
    select Distinct LEG_DEST_S as dest
    from fcst.fcst_id_ref
    where LEG_ORIG_S = '{orig}'
    """
    fcst_id_df = pd.read_sql_query(fcst_id_qry, con=hcrt)

    return list(value[0] for value in fcst_id_df.values)


def get_fcst_given_leg(orig, dest, hcrt):
    """Finds fcst_id and start and end of the fcst time_bounds for a given orig and dest

    Args:
        orig (string): Origen Airport Code
        dest (string): Destination Airport Code
        hcrt (cx_Oracle.Connection): herccrt().con()

    Returns:
        _type_: _description_
    """

    fcst_id_qry = f"""
    select Distinct LEG_ORIG_S as orig, LEG_DEST_S as dest, FCST_ID as fcst_id,
            TIME_BAND_START as time_band_start, TIME_BAND_END as time_band_end
    from fcst.fcst_id_ref
    where 1=1
    and LEG_ORIG_S = '{orig}'
    and LEG_DEST_S = '{dest}'
    order by 1,2,3,4
    """
    fcst_id_df = pd.read_sql_query(fcst_id_qry, con=hcrt)

    return fcst_id_df


def get_oag_data(orig, dest, pull_start, pull_end, ulcc_list, mos):
    """Data from other airlines (it also includes AA data), showing the their rout and capacity, given dates and destinations.
    Contains the latest publication of scheduled flights.

    Args:
        orig (string): Origen Airport Code
        dest (string): Destination Airport Code
        pull_start (string): Starting bound for the pull date
        pull_end (string): Ending bound for the pull date
        ulcc_list (list): list of ULCC airline codes
        mos (pyodbc.Connection): mosaic().con()

    Returns:
        pd.DataFrame: OA flight infos with Unique keys: [orig, dest, dep_data, dep_mam, airline, flt_id]
    """

    oag_qry = f"""
    select DEP_AIRPRT_IATA_CD as orig,
            ARVL_AIRPRT_IATA_CD as dest,
            LOCAL_DEP_DT as dep_date,
            DEP_MINUTE_PAST_MDNGHT_QTY as dep_mam,
            FLIGHT_SCHD_PUBLSH_DT as snapshot_date,
            OPERAT_AIRLN_IATA_CD as airline,
            OPERAT_FLIGHT_NBR as flt_id, -- Flight Number
            EQUIP_COACH_CABIN_SEAT_QTY as seats,
            ASMS_QTY as asm,
            EQUIP_COACH_CABIN_SEAT_QTY * MILE_GREAT_CIRCLE_DISTANC_QTY as asm_y -- ASM for Coach Cabin
    from PROD_INDSTR_FLIGHT_SCHD_VW.OAG_CURR
    where 1=1
    and DEP_AIRPRT_IATA_CD = '{orig}'
    and ARVL_AIRPRT_IATA_CD = '{dest}'
    and LOCAL_DEP_DT between '{pull_start}' and '{pull_end}'
    -- and LOCAL_DEP_DT = '2022-09-12'
    -- and OPERAT_AIRLN_IATA_CD = 'AA'
    and OPERAT_PAX_FLIGHT_IND = 'Y' -- new field that determines if record is a scheduled operating flight record
    and FLIGHT_OAG_PUBLSH_CD <> 'X' -- record is active and not cancelled
    order by 1,2,3,4,5
    """

    oag_df = pd.read_sql(oag_qry, con=mos)

    # convert to datetime format
    oag_df["dep_date"] = pd.to_datetime(oag_df["dep_date"], format="%Y/%m/%d")

    # convert the dep_time before 3am to the previous dep_date
    # + Also you have ti change the dep_date for that one as well.
    oag_df["dep_mins"] = [val + 24 * 60 if val < 180 else val for val in oag_df["dep_mam"]]
    oag_df["adj_dep_date"] = [
        date - dt.timedelta(days=1) if mam < 180 else date for mam, date in zip(oag_df["dep_mam"], oag_df["dep_date"])
    ]

    # add yr, mo, wk cols
    oag_df["yr"] = oag_df["adj_dep_date"].dt.year
    oag_df["mo"] = oag_df["adj_dep_date"].dt.month
    oag_df["wk"] = oag_df["adj_dep_date"].dt.isocalendar().week
    # add ulcc indicator
    oag_df["ulcc_ind"] = [1 if val in ulcc_list else 0 for val in oag_df["airline"]]
    oag_df["seats_ulcc"] = [seats if val in ulcc_list else 0 for val, seats in zip(oag_df["airline"], oag_df["seats"])]

    return oag_df


def get_cap_data(orig, dest, pull_start, pull_end, mos, cabin="Y"):
    """AA Capacity per flight in the given dates and destinations.

    Args:
        orig (string): Origen Airport Code
        dest (string): Destination Airport Code
        pull_start (string): Starting bound for the pull date
        pull_end (string): Ending bound for the pull date
        mos (pyodbc.Connection): mosaic().con()
        cabin (str, optional): Flight cabin class. Defaults to 'Y'.

    Returns:
        pd.DataFrame: AA Capacity with unique keys: [orig, dest, dep_data, dep_time, snapshot_date, cabin, flt_id]
    """
    cap_query = f"""
    select LEG_DEP_AIRPRT_IATA_CD as orig,
            LEG_ARVL_AIRPRT_IATA_CD as dest,
            SCHD_LEG_DEP_DT as dep_date,
            SCHD_LEG_DEP_TM as dep_time,
            FILE_SNPSHT_DT as snapshot_date,
            LEG_CABIN_CD as cabin,
            OPERAT_AIRLN_IATA_CD as airline,
            MKT_FLIGHT_NBR as flt_id,  --Flight Number
            CABIN_CAPCTY_SEAT_QTY as seats,
            CAB_ASM_QTY as asm,
            CAB_TOT_RPM_QTY as rpm,
            CAB_TOT_REVNUE_AMT as rev,
            CAB_TOT_PAX_QTY as pax
    from PROD_RM_BUSINES_VW.LIFE_OF_FLIGHT_LEG_CABIN
    where 1=1
    and FILE_SNPSHT_DT = SCHD_LEG_DEP_DT-1 -- only extract the data one day before departure
    and LEG_DEP_AIRPRT_IATA_CD = '{orig}'
    and LEG_ARVL_AIRPRT_IATA_CD = '{dest}'
    and SCHD_LEG_DEP_DT between '{pull_start}' and '{pull_end}'
    -- and SCHD_LEG_DEP_DT = '2022-09-12'
    and LEG_CABIN_CD = '{cabin}'
    order by 1,2,3,4,5
    """

    cap_df = pd.read_sql(cap_query, con=mos)

    # convert to datetime format
    cap_df["dep_date"] = pd.to_datetime(cap_df["dep_date"], format="%Y/%m/%d")
    # cap_df['dep_time'] = pd.to_datetime(cap_df['dep_date']+cap_df['dep_time'], format='%H:%M:%S')
    cap_df["dep_time"] = pd.to_datetime(
        cap_df["dep_date"].astype(str) + " " + cap_df["dep_time"].astype(str), format="%Y/%m/%d %H:%M:%S"
    )

    # count the minutes from mid-night
    cap_df["dep_mins"] = pd.DatetimeIndex(cap_df["dep_time"]).hour * 60 + pd.DatetimeIndex(cap_df["dep_time"]).minute
    # convert the dep_time before 3am to the previous dep_date
    # + Also you have to adjust the dep_date for that one as well.
    cap_df["adj_dep_date"] = [
        date - dt.timedelta(days=1) if mam < 180 else date for mam, date in zip(cap_df["dep_mins"], cap_df["dep_date"])
    ]
    cap_df["dep_mins"] = [val + 24 * 60 if val < 180 else val for val in cap_df["dep_mins"]]

    # add yr, mo, wk
    cap_df["yr"] = cap_df["adj_dep_date"].dt.year
    cap_df["mo"] = cap_df["adj_dep_date"].dt.month
    cap_df["wk"] = cap_df["adj_dep_date"].dt.isocalendar().week

    return cap_df


# -------------- Data Processing:


def oag_per_day(oag_df):
    """Before focusing on each individual flight, lets look at a big bigger picture (flights per day) and compare flights done by AA with OA and UCLL in terms of available seats, flight numbers, and ASM.

    Args:
        oag_df (pd.DataFrame): OA/AA flight infos

    Returns:
        pd.DataFrame: OA/AA flights info (seats)  per day DataFrame
    """
    # get OA Cap per day.

    # groupby for the entire market (So we can calculate the Shares) - Sina changed it to per day.
    gp_cols = ["adj_dep_date", "airline"]
    agg_cols = {"seats": "sum", "asm_y": "sum", "flt_id": "count", "ulcc_ind": "sum", "seats_ulcc": "sum"}

    oag_kl_Per_airline_Day = oag_df.groupby(gp_cols).agg(agg_cols).reset_index()

    # change flt_id name as flt_ct, and asm_y to asm (since we are focusing on y cabin)
    oag_kl_Per_airline_Day.rename(
        columns={"flt_id": "flt_ct", "ulcc_ind": "ulcc_ind_mkt", "asm_y": "asm"}, inplace=True
    )

    # Now lets aggregate all airlines to have information on all airlines.
    gp_cols = ["adj_dep_date"]
    agg_cols = {"seats": "sum", "asm": "sum", "flt_ct": "sum", "ulcc_ind_mkt": "sum", "seats_ulcc": "sum"}

    oag_kl_total_Per_Day = oag_kl_Per_airline_Day.groupby(gp_cols).agg(agg_cols).reset_index()

    # Filter American flights into a separate view

    filter0 = oag_kl_Per_airline_Day["airline"] == "AA"
    oag_kl_Per_american_Day = oag_kl_Per_airline_Day[filter0]

    # Drop unrelated information
    oag_kl_Per_american_Day.drop(columns=["ulcc_ind_mkt", "seats_ulcc", "airline"], inplace=True)

    # Merge the AA data with the aggregate data
    oag_kl_total_Per_Day_and_AA = pd.merge(
        oag_kl_total_Per_Day, oag_kl_Per_american_Day, on=gp_cols, how="left", suffixes=("_All", "_AA")
    )
    oag_kl_total_Per_Day_and_AA.rename(columns={"ulcc_ind_mkt": "flt_ct_ulcc"}, inplace=True)

    # Calculate OA data.
    oag_kl_total_Per_Day_and_AA["seats_OA"] = (
        oag_kl_total_Per_Day_and_AA["seats_All"]
        - oag_kl_total_Per_Day_and_AA["seats_AA"]
        - oag_kl_total_Per_Day_and_AA["seats_ulcc"]
    )
    oag_kl_total_Per_Day_and_AA["flt_ct_OA"] = (
        oag_kl_total_Per_Day_and_AA["flt_ct_All"]
        - oag_kl_total_Per_Day_and_AA["flt_ct_AA"]
        - oag_kl_total_Per_Day_and_AA["flt_ct_ulcc"]
    )

    # Reformat the data
    oag_kl_total_Per_Day_and_AA = oag_kl_total_Per_Day_and_AA.loc[
        :,
        [
            "adj_dep_date",
            "seats_AA",
            "seats_OA",
            "seats_ulcc",
            "seats_All",
            "flt_ct_AA",
            "flt_ct_OA",
            "flt_ct_ulcc",
            "flt_ct_All",
            "asm_AA",
            "asm_All",
        ],
    ]

    return oag_kl_total_Per_Day_and_AA


def oag_per_fcst(oag_df, fcst_start, fcst_end):
    """Extracts the on the flight based on a given FCST_bond

    Args:
        oag_df (pd.DataFrame): OA/AA flight infos
        fcst_start (int): FCST time bound start (in minutes after midnight)
        fcst_end (int): FCST time bound end (in minutes after midnight)

    Returns:
        pd.DataFrame: OA/AA flights info (seats) per given FCST DataFrame
    """
    # OAG only keep the dep_mins in the fcst_id
    oag_df2 = oag_df[(oag_df["dep_mins"] >= fcst_start) & (oag_df["dep_mins"] <= fcst_end)]

    # here we only care about AA.
    gp_cols = ["adj_dep_date", "airline"]
    agg_cols = {"seats": "sum", "asm_y": "sum", "flt_id": "count", "ulcc_ind": "sum", "seats_ulcc": "sum"}

    oag_kl = oag_df2.groupby(gp_cols).agg(agg_cols).reset_index()
    # change flt_id name as flt_ct
    oag_kl.rename(columns={"flt_id": "flt_ct", "asm_y": "asm"}, inplace=True)
    oag_kl
    # groupby for the entire market (So we can calculate the Shares)
    gp_cols = ["adj_dep_date"]
    agg_cols = {"seats": "sum", "asm_y": "sum", "flt_id": "count", "ulcc_ind": "sum", "seats_ulcc": "sum"}

    oag_kl_AAOA = oag_df2.groupby(gp_cols).agg(agg_cols).reset_index()
    # change flt_id name as flt_ct
    oag_kl_AAOA.rename(columns={"flt_id": "flt_ct", "ulcc_ind": "ulcc_count", "asm_y": "asm"}, inplace=True)

    # Filter the oag_df2 to show theAA Cap (per fcst span) and merge AAOA Cap (on the specific fcst).
    filterAA = oag_kl["airline"] == "AA"
    oag_kl = oag_kl[filterAA]
    oag_kl.drop(columns=["ulcc_ind", "seats_ulcc", "airline"], inplace=True)

    oag_kl = pd.merge(oag_kl, oag_kl_AAOA, on=gp_cols, how="left", suffixes=("_AA_fcst", "_All_fcst"))

    # add OA Cap
    oag_kl["seats_OA_fcst"] = oag_kl["seats_All_fcst"] - oag_kl["seats_AA_fcst"] - oag_kl["seats_ulcc"]
    oag_kl["asm_OA_fcst"] = oag_kl["asm_All_fcst"] - oag_kl["asm_AA_fcst"]
    oag_kl["flt_ct_OA_fcst"] = oag_kl["flt_ct_All_fcst"] - oag_kl["flt_ct_AA_fcst"] - oag_kl["ulcc_count"]

    # # add AA market share
    # oag_kl['seats_share'] = oag_kl['seats_AA_fcst']/oag_kl['seats_AAOA']
    # oag_kl['asm_share'] = oag_kl['asm_AA_fcst']/oag_kl['asm_AAOA']
    # oag_kl['flt_ct_share'] = oag_kl['flt_ct_AA_fcst']/oag_kl['flt_ct_AAOA']
    # # add seats_per_flt
    # oag_kl['seats_per_flt_AA'] = oag_kl['seats_AA']/oag_kl['flt_ct_AA']
    # oag_kl['seats_per_flt_OA'] = oag_kl['seats_OA']/oag_kl['flt_ct_OA']
    # oag_kl['seats_per_flt_AAOA'] = oag_kl['seats_AAOA']/oag_kl['flt_ct_AAOA']

    oag_kl["fcst_start"] = fcst_start
    oag_kl["fcst_end"] = fcst_end
    oag_kl.rename(columns={"seats_ulcc": "seats_ulcc_fcst", "ulcc_count": "flt_ct_ulcc_fcst"}, inplace=True)

    oag_kl = oag_kl.loc[
        :,
        [
            "adj_dep_date",
            "fcst_start",
            "fcst_end",
            "seats_AA_fcst",
            "seats_OA_fcst",
            "seats_ulcc_fcst",
            "seats_All_fcst",
            "flt_ct_AA_fcst",
            "flt_ct_OA_fcst",
            "flt_ct_ulcc_fcst",
            "flt_ct_All_fcst",
            "asm_AA_fcst",
            "asm_All_fcst",
        ],
    ]

    return oag_kl


def normalize_oag_kl_fcst_total(oag_kl_fcst_total):
    """Normalizes the data using min-max scale

    Args:
        oag_kl_fcst_total (DataFrame): OAG Data

    Returns:
        DataFrame: Normalized OAG Data
    """

    # Drop Null
    oag_kl_fcst_total.dropna(inplace=True)

    # # remove the
    # oag_kl_fcst_total.drop(columns=['seats','asm','flt_ct' , 'fcst_start' , 'fcst_end'],inplace=True)

    # Normalize Cap features
    norm_cols = [
        "seats_AA_fcst",
        "seats_OA_fcst",
        "seats_ulcc_fcst",
        "seats_All_fcst",
        "flt_ct_AA_fcst",
        "flt_ct_OA_fcst",
        "flt_ct_ulcc_fcst",
        "flt_ct_All_fcst",
        "asm_AA_fcst",
        "asm_All_fcst",
        "seats_AA",
        "seats_OA",
        "seats_ulcc",
        "seats_All",
        "flt_ct_AA",
        "flt_ct_OA",
        "flt_ct_ulcc",
        "flt_ct_All",
        "asm_AA",
        "asm_All",
    ]

    oag_kl_fcst_total[norm_cols] = minmax_scale(oag_kl_fcst_total[norm_cols])

    return oag_kl_fcst_total


def aa_cap_fcst(cap_df, fcst_start, fcst_end):
    """After filtering the data by the fcst bond, here we group by all the AA flights on the departure date

    Args:
        cap_df (DataFrame): AA Cap Data
        fcst_start (int): FCST time bound start (in minutes after midnight)
        fcst_end (int): FCST time bound end (in minutes after midnight)

    Returns:
        DataFrame: Aggregated flight information per day based on given FCST
    """
    # AA Cap only keep the dep_mins in the fcst_id
    cap_df2 = cap_df[(cap_df["dep_mins"] >= fcst_start) & (cap_df["dep_mins"] <= fcst_end)]

    # get AA Cap
    gp_cols = ["dep_date"]
    agg_cols = {"seats": "sum", "asm": "sum", "flt_id": "count", "rpm": "sum", "rev": "sum", "pax": "sum"}

    cap_kl = cap_df2.groupby(gp_cols).agg(agg_cols).reset_index()
    cap_kl.rename(columns={"flt_id": "flt_ct"}, inplace=True)

    # add other Cap features
    cap_kl["rasm"] = cap_kl["rev"] / cap_kl["asm"]
    cap_kl["yield"] = cap_kl["rev"] / cap_kl["rpm"]
    cap_kl["load_fac"] = cap_kl["rpm"] / cap_kl["asm"]

    # replace N/A with 0
    # print(cap_kl.isnull().sum())
    cap_kl = cap_kl.replace(np.nan, 0)
    # print(cap_kl.isnull().sum())

    return cap_kl


def merge_oag_aacap(oag_kl, cap_kl):
    """merge both OAG and AA cap on dep_date
    And:
    1. Merge on cap_K1 (so data from OAG when CAP data is None would not be included.)
        Either data is not included because all the flights on that data are canceled (I think)
    2. 'seats','asm','airline','ulcc_ind' from the CAP are dropped (so we use the data gathered from the OAG dataset, which might be less accurate.)
    3. Data are normalized using minmax_scale

    Args:
        oag_kl (DtaFrame): OAG DF
        cap_kl (DtaFrame): AA Cap DF

    Returns:
        DtaFrame: _description_
    """
    # merge OAG and AA Cap data

    oag_kl.rename(columns={"adj_dep_date": "dep_date"}, inplace=True)

    oag_cap_kl = pd.merge(cap_kl, oag_kl, on=["dep_date"], how="left", suffixes=("_cap", "_oag"))

    # print(oag_cap_kl.isnull().sum())
    oag_cap_kl.dropna(inplace=True)

    # remove the
    oag_cap_kl.drop(columns=["seats", "asm", "flt_ct", "fcst_start", "fcst_end"], inplace=True)

    # Normalize Cap features
    norm_cols = [
        "rpm",
        "rev",
        "pax",
        "rasm",
        "yield",
        "load_fac",
        "seats_AA_fcst",
        "seats_OA_fcst",
        "seats_ulcc_fcst",
        "seats_All_fcst",
        "flt_ct_AA_fcst",
        "flt_ct_OA_fcst",
        "flt_ct_ulcc_fcst",
        "flt_ct_All_fcst",
        "asm_AA_fcst",
        "asm_All_fcst",
    ]

    oag_cap_kl[norm_cols] = minmax_scale(oag_cap_kl[norm_cols])

    return oag_cap_kl


# ----------------  Group and Paddings:


def create_group_id(df):
    """Groups Data in the 14 rows (local/flow, 7 fcst_perd) x 10 cols (frac_closure), adds a REAL token to the DataFrame that already exists.
    Args:
        df (DataFrame): Given DataFrame Format

    Returns:
        DataFrame: Grouped data + real tokens
    """

    df = df.sort_values(
        [
            "snapshotDate",
            "origin",
            "destination",
            "forecastId",
            "flightDepartureDate",
            "forecastDayOfWeek",
            "poolCode",
            "cabinCode",
            "localFlowIndicator",
        ]
    )

    # 'GroupBy.cumcount': Number each item in each group from 0 to (the length of that group - 1).
    # '== 0' returns True or False
    # '.astype(int)' ,convert True/False to 1/0
    df["groupID"] = (
        df.groupby(
            [
                "snapshotDate",
                "origin",
                "destination",
                "forecastId",
                "flightDepartureDate",
                "forecastDayOfWeek",
                "poolCode",
                "cabinCode",
            ]
        ).cumcount()
        == 0
    ).astype(int)
    # assign each unique group a new group id
    # groupID==1 will be a new id (+1), groupID==0 will indicate the same id for the same group
    df["groupID"] = df["groupID"].cumsum()

    # Full History Pre Fixing
    # count the num of 'forecastPeriod' in each group
    # because some group might not have all 7 'forecastPeriod'
    df["fullHistory"] = df.groupby(["groupID"])["forecastPeriod"].transform("count")
    # Indicator it is part of history and not a pad
    df["real"] = 1
    return df


def empty_group():
    """Creates an empty group with 14 rows (local/flow, 7 fcst_perd), what we call "FAKE data".
    It is used to populate the missing data with 0 traffic

    Returns:
        DataFrame: a 14-row DataFrame of Empty values (FAKE - Date)
    """
    # Creating an empty group to be used for padding.
    fullKeysArray = np.zeros((14, 50))  # 'fullKeys' uses 50 columns

    # frac_closure = 0: fully closed with 0 traffic
    for i in range(0, fullKeysArray.shape[0]):
        if i <= 6:  # Flow: 0 ~ 6
            fullKeysArray[i][0] = 1  # 'localFlowIndicator' column
            fullKeysArray[i][1] = i + 1  # 'forecastPeriod' column: 1 ~ 7
            fullKeysArray[i][2:12] = 1  # 'fracClosure' column all set to 1
        else:  # Local: 7 ~ 13s
            fullKeysArray[i][0] = 0  # 'localFlowIndicator' column
            fullKeysArray[i][1] = i - 6  # 'forecastPeriod' column: 1 ~ 7
            fullKeysArray[i][2:12] = 1  # 'fracClosure' column: all set to 1

    fullKeys = pd.DataFrame(fullKeysArray)
    fullKeys.columns = (
        ["localFlowIndicator", "forecastPeriod"]
        + [f"fracClosure_{i}" for i in range(1, 11)]
        + [f"trafficActual_{i}" for i in range(1, 11)]
        + [f"trafficActualAadv_{i}" for i in range(1, 11)]
        + [
            "holiday",
            "H1",
            "H2",
            "H3",
            "HL",
            "weekNumber",
            "week_x",
            "week_y",
            "dow_x",
            "dow_y",
            "avgtraffic",
            "avgtrafficopenness",
            "avgrasm",
            "dowavgtraffic",
            "dowavgtrafficopenness",
            "dowavgrasm",
            "groupID",
            "fullHistory",
        ]
    )
    fullKeys["localFlowIndicator"] = ["F" if lfi == 1 else "L" for lfi in fullKeys["localFlowIndicator"]]
    return fullKeys


def empty_group_future():
    """This function works similarly to the empty_group, but instead of populating traffic with 0, it populates it with -1.
    it is used for future flights where we have not closed the periods yet.

    Returns:
        DataFrame: a 14-row DataFrame with -1 value for traffic (FAKE - Date)
    """
    # Creating an empty group to be used for padding.
    fullKeysArray = np.zeros((14, 50))  # 'fullKeys' uses 50 columns

    # frac_closure = 0: fully closed with 0 traffic
    for i in range(0, fullKeysArray.shape[0]):
        if i <= 6:  # Flow: 0 ~ 6
            fullKeysArray[i][0] = 1  # 'localFlowIndicator' column
            fullKeysArray[i][1] = i + 1  # 'forecastPeriod' column: 1 ~ 7
            fullKeysArray[i][2:12] = 0  # 'fracClosure' column all set to 0
            fullKeysArray[i][12:32] = -1  # 'trafficActual' column all set to -1
        else:  # Local: 7 ~ 13s
            fullKeysArray[i][0] = 0  # 'localFlowIndicator' column
            fullKeysArray[i][1] = i - 6  # 'forecastPeriod' column: 1 ~ 7
            fullKeysArray[i][2:12] = 0  # 'fracClosure' column: all set to 0
            fullKeysArray[i][12:32] = -1  # 'trafficActual' column all set to -1

    fullKeys = pd.DataFrame(fullKeysArray)
    fullKeys.columns = (
        ["localFlowIndicator", "forecastPeriod"]
        + [f"fracClosure_{i}" for i in range(1, 11)]
        + [f"trafficActual_{i}" for i in range(1, 11)]
        + [f"trafficActualAadv_{i}" for i in range(1, 11)]
        + [
            "holiday",
            "H1",
            "H2",
            "H3",
            "HL",
            "weekNumber",
            "week_x",
            "week_y",
            "dow_x",
            "dow_y",
            "avgtraffic",
            "avgtrafficopenness",
            "avgrasm",
            "dowavgtraffic",
            "dowavgtrafficopenness",
            "dowavgrasm",
            "groupID",
            "fullHistory",
        ]
    )
    fullKeys["localFlowIndicator"] = ["F" if lfi == 1 else "L" for lfi in fullKeys["localFlowIndicator"]]
    fullKeys
    return fullKeys


def padding_groups(df, fullKeys):
    """This function loops through the data, and replaces any missing values with the "Fake Data". (Fake Data is data that has 0 (-1) as traffic)
    It finds the time-periods (the 14 periods for each day) that are missing and populates the "fake Data" for them.

    Args:
        df (DataFrame): _description_
        fullKeys (DataFrame): The empty Group (output of either empty_group or empty_group_future)

    Returns:
        DataFrame: DataFrame after populating it with "Fake Data" and group them based on departure day (where each day has 14 rows (7 Time periods * 2 Local/Flow))
    """

    groupbyColumns = [
        "snapshotDate",
        "origin",
        "destination",
        "forecastId",
        "flightDepartureDate",
        "forecastDayOfWeek",
        "poolCode",
        "cabinCode",
    ]
    grouped = df.groupby(groupbyColumns)

    merged_list = []
    count_rows_misskey = 0
    for g in grouped:

        # g[0] is the directory key and g[1] is the value (actual data)
        # identify the cells of g[1] that not in fullKeys
        # g[1][~g[1].isin(fullKeys)]

        # identify the missing keys in g[1]
        key = g[1][["localFlowIndicator", "forecastPeriod"]]
        missingKeys = fullKeys[
            ~fullKeys[["localFlowIndicator", "forecastPeriod"]].apply(tuple, 1).isin(key.apply(tuple, 1))
        ]
        count_rows_misskey += missingKeys.shape[0]

        # append the missing keys under the data
        fullHistory = pd.concat([g[1], missingKeys])

        # use 0 to indicate padding data
        fullHistory["real"].fillna(0, inplace=True)

        # fill the data with missing keys
        fullHistory = fullHistory.fillna(method="ffill")
        merged_list.append(fullHistory)

    # merge all data across 'flightDepartureDate'
    out = pd.concat(merged_list)
    out = out.sort_values(
        [
            "snapshotDate",
            "origin",
            "destination",
            "forecastId",
            "flightDepartureDate",
            "forecastDayOfWeek",
            "poolCode",
            "cabinCode",
            "localFlowIndicator",
        ]
    )

    # Get full history and then concat fake history (padding) from above
    post = out.copy()
    post = post.sort_values(
        [
            "snapshotDate",
            "origin",
            "destination",
            "forecastId",
            "flightDepartureDate",
            "forecastDayOfWeek",
            "poolCode",
            "cabinCode",
            "localFlowIndicator",
        ]
    )

    post["groupID"] = (
        post.groupby(
            [
                "snapshotDate",
                "origin",
                "destination",
                "forecastId",
                "flightDepartureDate",
                "forecastDayOfWeek",
                "poolCode",
                "cabinCode",
            ]
        ).cumcount()
        == 0
    ).astype(int)
    post["groupID"] = post["groupID"].cumsum()
    # Full Hisotyr Pre Fixing
    post["fullHistory"] = post.groupby(["groupID"])["forecastPeriod"].transform("count")

    post["flightDepartureDate"] = pd.to_datetime(post["flightDepartureDate"], format="%Y/%m/%d")

    post = post.sort_values(
        [
            "forecastDepartureDate",
            "origin",
            "destination",
            "forecastId",
            "flightDepartureDate",
            "forecastDayOfWeek",
            "poolCode",
            "cabinCode",
            "localFlowIndicator",
            "forecastPeriod",
        ]
    )
    return post


def group_and_pad(df):
    """This function calls all the above functions.
    Also use the Date-time today, to use the empty_group_future for any future data.

    Args:
        df (DataFrame): DataFrame with all the data.

    Returns:
        DataFrame: Grouped and padded DataGFrame with "True" and "Fake" Date
    """

    yesterday = (datetime.today() - timedelta(days=2)).strftime("%Y-%m-%d")

    fullKeys = empty_group()
    fullKeysfuture = empty_group_future()

    # Divide DF in past and Future:
    df["flightDepartureDate"] = pd.to_datetime(df["flightDepartureDate"], format="%Y/%m/%d")

    df_past = df[df["flightDepartureDate"] <= yesterday]
    df_future = df[df["flightDepartureDate"] >= yesterday]

    if len(df_future) > 10:
        df_future = padding_groups(create_group_id(df_future), fullKeysfuture)
        df_past = padding_groups(create_group_id(df_past), fullKeys)
        df = pd.concat([df_past, df_future])
    else:
        df = padding_groups(create_group_id(df), fullKeys)

    return df


# ----------------   Tensor Masking - Processing: TILL HERE


def randPeriod(prdMaps):
    """Returns a Random Day to Depatrue (and its time-class to departure)

    Args:
        prdMaps (Dataframe): Dataframe that shows the time to departure where the period class of a given flight gets closed.

    Returns:
        random_period (int): Random Class to departure. (between 1-7)
        random_day (int): Random Day to Departure (it should be between 2 to 331- When the first class opens up.)
    """
    random_period = np.random.randint(1, 7)  # Gets a class between 1 to 7, these are our period to departure classes.
    rrd_start, rrt_end = prdMaps[prdMaps["FORECASTPERIOD"] == random_period].loc[:, ["RRD_START", "RRD_END"]].values[0]
    random_day = np.random.randint(rrd_start, rrt_end)
    return random_period, random_day


def tf_timeseries_masking(tf_tensors, data_index, prdMaps, window):
    """This function will generate masked time-series traffic data, for a given index(day). - When using Daily-Timeseries.

    Args:
        tf_tensors (np.array): All traffic tensors with the shape of (n_samples, Channel, 7 (time-classes), 10 (fare-classes))
        data_index (int): Index of the data (corresponds to one data point (day) in our dataset)
        prdMaps (Dataframe): Dataframe that shows the time to departure where the period class of a given flight gets closed.
        window (int): window size for our time-series.

    Returns:
        tf_tensors (np.array): Returns a Trrafic tensor for the given index (day) with the shape of (window, Channel, 7 (time-classes), 10 (fare-classes))
    """

    random_period, random_day_to_dept = randPeriod(prdMaps)
    # print(random_period , random_day_to_dept )
    arr = prdMaps.iloc[:, 3].values

    # output = tf_tensors[data_index].copy()
    test_tensors = tf_tensors.copy()
    test_tensors[data_index][
        :,
        :random_period,
    ] = -1

    max_bond_period = random_day_to_dept
    min_bond_period = arr[random_period - 1]
    remaining_window = window - 1
    current_index = data_index
    max_min_range = max_bond_period - min_bond_period
    current_period = random_period

    if max_min_range < remaining_window:
        while max_min_range <= remaining_window:
            # print(current_index-max_min_range,current_index)
            test_tensors[
                current_index - max_min_range : current_index,
                :,
                :current_period,
            ] = -1
            current_period -= 1
            if current_period == 0:
                break
            current_index -= max_min_range
            remaining_window -= max_min_range
            max_bond_period -= max_min_range
            min_bond_period = arr[current_period - 1]
            max_min_range = max_bond_period - min_bond_period
            # reaching Today date:

    if max_min_range >= remaining_window:
        # print(current_index-max_min_range,current_index)
        test_tensors[
            current_index - remaining_window : current_index,
            :,
            :current_period,
        ] = -1

    return test_tensors[data_index + 1 - window : data_index + 1]


def get_tensors2(
    DataFarame,
    sea_col_Cap,
    prdMaps=None,
    FC_time_series=False,
    traffic_time_series=True,
    use_channels=True,
    seasenality_one_dimension=True,
    window=10,
    DOW=False,
):
    """Given a DataFrame, this function will transfer the dataframe into tensors of processed data.

    Args:
        DataFarame (pd.DataFrame): Input DataFrame can be either our train, test, or future data in Pandas Format.
        sea_col_Cap (_type_): The list of seasonalities we would like to extract from the DataFrame to be used for our DeepLearning model.
        prdMaps (_type_, Dataframe): Dataframe that shows the time to departure where the period class of a given flight gets closed (it is needed when traffic_time_series=True). Defaults to None.
        FC_time_series (bool, optional): Makes the FC into time-series. Defaults to False.
        traffic_time_series (bool, optional): Returns a time-series of the traffic of the past days for all datapoints. Defaults to True.
        use_channels (bool, optional): If it is true, it makes our data into 3d tensors by adding traffic flow/local into another dimension. Defaults to True.
        seasenality_one_dimension (bool, optional): Reshape the data into one dimension. Defaults to True.
        window (int, optional): window size for our time-series. Defaults to 10.
        DOW (bool, optional): Whether we are processing DOW timeseries or daily. Defaults to False.

    Returns:
        FC (np.tensor): FairClousre Data Tensor. with shape of (data_size, channel, Time_classes, Fair_classes) if FC_time_series = True, shape will be: (data_size, window , channel, Time_classes, Fair_classes)
        Seasonality (np.tensor): Flight Seasonality, shape of (data_size, Seasenality_size)
        Traffic (np.tensor): Trrafic, used for output. Shape of: (data_size, channel, Time_classes, Fair_classes)
        TF_time (np.tensor): Traffic time-series data for each given flight with the size of (data_size, window, channel, Time_classes, Fair_classes)
    """

    len_sea_cap = len(sea_col_Cap)

    # fractional closure
    PRE_FC_L = DataFarame[["fracClosure_" + str(i + 1) for i in range(10)]].values.astype("float32")
    # seasonality
    PRE_Sea_L = DataFarame[sea_col_Cap].values.astype("float32")
    # actual traffic
    PRE_Traf_L = DataFarame[["trafficActual_" + str(i + 1) for i in range(10)]].values.astype("float32")

    # reshape the data for CNNLSTM model
    FC = PRE_FC_L.reshape(int(PRE_FC_L.shape[0] / 14), 1, 14, 10)
    Seasenality = PRE_Sea_L.reshape(int(PRE_Sea_L.shape[0] / 14), 1, 14, len_sea_cap)
    Traffic = PRE_Traf_L.reshape(int(PRE_Traf_L.shape[0] / 14), 1, 14, 10)

    # Remove Duplicates (from 2d to 1d vector)
    if seasenality_one_dimension:
        Seasenality = np.delete(Seasenality, slice(13), 2).reshape(Seasenality.shape[0], len_sea_cap)

    if use_channels:
        FC = FC.reshape(len(FC), 2, 7, 10)
        Traffic = Traffic.reshape(len(Traffic), 2, 7, 10)

    # Change FC shape to refelect time series:
    # print(FC.shape)
    if FC_time_series:
        time_series_widow = list()
        Seasenality_times = list()
        for i in range(window, len(FC)):
            # print(FC[i-window:i].shape)
            time_series_widow.append(FC[i - window : i].reshape(window, 2, 7, 10))
            # print((Seasenality[i-window:i].shape))
            Seasenality_times.append(Seasenality[i - window : i])
        FC = np.array(time_series_widow)
        Seasenality = np.array(Seasenality_times)

        # Since the 1st window size data points are removed:
        # Seasenality = Seasenality[window:]
        Traffic = Traffic[window:]

    elif traffic_time_series:
        traffic_time_series_window = list()
        Seasenality_times = list()
        for i in range(window, len(Traffic)):
            # Find Random period and random day:
            if DOW:
                tf_window_masked = tf_timeseries_masking_DOW(Traffic, i, prdMaps, window)
            else:
                tf_window_masked = tf_timeseries_masking(Traffic, i, prdMaps, window)
            traffic_time_series_window.append(tf_window_masked)
            # Seasenality_times.append(Seasenality[i-window:i])
        TF_time = np.array(traffic_time_series_window)
        # Seasenality = np.array(Seasenality_times)
        Seasenality = Seasenality[window:]
        FC = FC[window:]

        Traffic = Traffic[window:]

        return FC, Seasenality, Traffic, TF_time

    return FC, Seasenality, Traffic, None


def floorSearch(arr, low, high, x):
    """Floor search function. Given a sorted list, and a number, it will find the floor index for that number.

    Args:
        arr (list): sorted list
        low (int): Smallest index to be considered in the list
        high (int): highest index to be considered in the list
        x (int): the number that we want to find the floor of.

    Returns:
        int: the floor index of the arr list given number x.
    """

    # If low and high cross each other
    if low > high:
        return -1

    # If last element is smaller than x
    if x >= arr[high]:
        return high

    # Find the middle point
    mid = int((low + high) / 2)

    # If middle point is floor.
    if arr[mid] == x:
        return mid

    # If x lies between mid-1 and mid
    if mid > 0 and arr[mid - 1] <= x and x < arr[mid]:
        return mid - 1

    # If x is smaller than mid,
    # floor must be in left half.
    if x < arr[mid]:
        return floorSearch(arr, low, mid - 1, x)

    # If mid-1 is not floor and x is greater than
    # arr[mid],
    return floorSearch(arr, mid + 1, high, x)


def tf_timeseries_masking_DOW(tf_tensors, data_index, prdMaps, window):
    """This function will generate masked time-series traffic data and is based on DOW.
    It is similar to the tf_timeseries_masking but works for the DOW data.

    Args:
        tf_tensors (np.array): All traffic tensors with the shape of (n_samples, Channel, 7 (time-classes), 10 (fare-classes))
        data_index (int): Index of the data (corresponds to one data point (day) in our dataset)
        prdMaps (Dataframe): Dataframe that shows the time to departure where the period class of a given flight gets closed.
        window (int): window size for our time-series.

    Returns:
        tf_tensors (np.array): Returns a Trrafic tensor for the given index (day) with the shape of (window, Channel, 7 (time-classes), 10 (fare-classes))
    """

    _, random_day_to_dept = randPeriod(prdMaps)
    arr = prdMaps.iloc[:, 3].values
    test_tensors = tf_tensors.copy()

    day_to_dept = random_day_to_dept
    current_index = data_index

    for i in range(0, window):
        # Move back 7 days in each iter.
        day_to_dept = random_day_to_dept - i * 7
        # Get the period of that day to dept.
        flrs = floorSearch(arr, 0, 6, day_to_dept)
        current_period = flrs + 1
        # If we get today, will break the loop. and use all the values (no masking)
        if current_period == 0:
            break
        # mask the values
        test_tensors[
            current_index,
            :,
            :current_period,
        ] = -1

        # Update index:
        current_index -= 1

    return test_tensors[data_index + 1 - window : data_index + 1]


def get_prdMaps(orig, dest, hcrt):
    """It will find the time-period bounds for a given flight.
    TODO: add the lcl_flw_ind and change the data to mask the difference between the local and Flow Traffic

    Args:
        orig (string): Origen Airport Code
        dest (string): Destination Airport Code
        hcrt (cx_Oracle.Connection): herccrt().con()

    Returns:
        DataFrame: DF of time periods with timeperiod ID and given daily bounds when each closes.
    """

    prdMaps = pd.read_sql(
        f"""select DISTINCT leg_orig as origin, leg_dest as destination, fcst_period as forecastPeriod, rrd_band_start_i as rrd_start, rrd_band_end_i as rrd_end
                            -- , lcl_flw_ind
                            from market_xref a
                            join FCST.FCST_PERIOD_REF b
                            on a.infl_period_id = b.FCST_PERIOD_ID
                            where 1=1
                            and cabin_code = 'Y'
                            and leg_orig = '{orig}'
                            and leg_dest = '{dest}'
                            and lcl_flw_ind = 'L'
                            ORDER BY forecastPeriod
                            """,
        con=hcrt,
    )
    return prdMaps


# def dow_get_tensors2(DataFarame , sea_col_Cap, prdMaps= None  ,  test = False, time_series = True,  use_channels = False , window = 10):
# def dow_get_tensors2(DataFarame , sea_col_Cap, prdMaps= None  ,  FC_time_series = True , traffic_time_series = False ,  use_channels = False , seasenality_one_dimension = True ,  window = 10):
#     DOW = True
#     FC_dow , Seasenality_dow, Traffic_dow ,  TF_time_dow  = list(), list(), list(), list()

#     for i in DataFarame.loc[ :,	['forecastDayOfWeek' ]].drop_duplicates().values:
#         # filter_y = DataFarame['dow_y' ] == i[1]
#         # filter_x = DataFarame['dow_x'] == i[0]
#         filter_dow =  DataFarame['forecastDayOfWeek'] == i[0]
#         # print(filter_y.shape , filter_x.shape)
#         # print(i)
#         Data_dow =DataFarame[filter_dow]
#         # print(Data_dow.shape)
#         FC, Seasenality, Traffic, TF_time= get_tensors2(Data_dow, sea_col_Cap, prdMaps  , FC_time_series  , traffic_time_series ,  use_channels  , seasenality_one_dimension  ,  window, DOW )
#         FC_dow.append(FC)
#         Seasenality_dow.append(Seasenality)
#         Traffic_dow.append(Traffic)
#         if traffic_time_series:
#             TF_time_dow.append(TF_time)

#     # Then Concat together, now each datapoint is based on DOW.
#     FC_dow = [ i  for i in FC_dow if i.shape!=(0,)]
#     Seasenality_dow = [ i  for i in Seasenality_dow if i.shape!=(0,)]
#     # Traffic_dow = [ i  for i in Traffic_dow if i.shape!=(0,)]

#     if traffic_time_series:
#         TF_time_dow = [ i  for i in TF_time_dow if i.shape!=(0,)]
#         TF_time_dow = np.concatenate(TF_time_dow)

#     FC_dow = np.concatenate(FC_dow)
#     Seasenality_dow = np.concatenate(Seasenality_dow)
#     Traffic_dow = np.concatenate(Traffic_dow)

#     return FC_dow , Seasenality_dow, Traffic_dow , TF_time_dow


def dow_get_tensors2(
    DataFarame,
    sea_col_Cap,
    prdMaps=None,
    FC_time_series=False,
    traffic_time_series=True,
    use_channels=True,
    seasenality_one_dimension=True,
    window=10,
    random_masking=True,
    test_today=None,
):
    """Given a DataFrame, this function will transfer the dataframe into tensors of processed data.

    Args:
        DataFarame (pd.DataFrame): Input DataFrame can be either our train, test or future data in Pandas Format.
        sea_col_Cap (list): The list of seasonalities we would like to extract from the DataFrame to be used for our DeepLearning model.
        prdMaps (Dataframe, optional): Dataframe that shows the time to departure where the period class of a given flight gets closed (it is needed when traffic_time_series=True). Defaults to None.
        FC_time_series (bool, optional): Makes the FC into time-series. Defaults to False.
        traffic_time_series (bool, optional): Returns a time-series of the traffic of the past days for all datapoints. Defaults to True.
        use_channels (bool, optional): If it is true, it makes our data into 3d tensors by adding traffic flow/local into another dimension. Defaults to True.
        seasenality_one_dimension (bool, optional): Reshape the data into one dimension. Defaults to True.
        window (int, optional): window size for our time-series. Defaults to 10.
        random_masking (bool, optional): If random masking is true, for each datapoint we assign a "day to departure" randomly, and mask the data based on that. If False we use test_today as our "fake today" and assign the maskings accordingly. Defaults to True.
        test_today (string, optional): If the random_masking is False we should define "fake today", and based on this fake today we'll mask our data. Defaults to None.

    Returns:
        FC (np.tensor): FairClousre Data Tensor. with shape of (data_size, channel, Time_classes, Fair_classes) if FC_time_series = True, shape will be: (data_size, window , channel, Time_classes, Fair_classes)
        Seasonality (np.tensor): Flight Seasonality, shape of (data_size, Seasenality_size)
        Traffic (np.tensor): Trrafic, used for output. Shape of: (data_size, channel, Time_classes, Fair_classes)
        TF_time (np.tensor): Traffic time-series data for each given flight with the size of (data_size, window, channel, Time_classes, Fair_classes)
    """
    DOW = True
    FC_dow, Seasenality_dow, Traffic_dow, TF_time_dow = defaultdict(), defaultdict(), defaultdict(), defaultdict()

    if not random_masking:
        masked_df = create_masking_based_on_given_day(DataFarame, test_today, prdMaps)

    for i in DataFarame.loc[:, ["forecastDayOfWeek"]].drop_duplicates().values:
        # filter_y = DataFarame['dow_y' ] == i[1]
        # filter_x = DataFarame['dow_x'] == i[0]
        filter_dow = DataFarame["forecastDayOfWeek"] == i[0]
        # print(filter_y.shape , filter_x.shape)
        # print(i)
        Data_dow = DataFarame[filter_dow]
        # print(Data_dow.shape)
        if random_masking:
            FC, Seasenality, Traffic, TF_time = get_tensors2(
                Data_dow,
                sea_col_Cap,
                prdMaps,
                FC_time_series,
                traffic_time_series,
                use_channels,
                seasenality_one_dimension,
                window,
                DOW,
            )
        else:
            Data_dow_masked = masked_df[filter_dow]
            FC, Seasenality, Traffic, TF_time = get_tensors2_faketoday(
                Data_dow, Data_dow_masked, sea_col_Cap, use_channels, seasenality_one_dimension, window
            )

        # FC, Seasenality, Traffic, TF_time= get_tensors2_faketoday(Data_dow, Data_dow_masked ,  sea_col_Cap , use_channels , seasenality_one_dimension ,  window)
        for i, j in enumerate(Data_dow.index[::14][window:]):
            FC_dow[j] = FC[i]
            Seasenality_dow[j] = Seasenality[i]
            Traffic_dow[j] = Traffic[i]
            if traffic_time_series:
                TF_time_dow[j] = TF_time[i]

    FC_dow = np.stack(list(dict(sorted(FC_dow.items())).values()))
    Seasenality_dow = np.stack(list(dict(sorted(Seasenality_dow.items())).values()))
    Traffic_dow = np.stack(list(dict(sorted(Traffic_dow.items())).values()))
    if traffic_time_series:
        TF_time_dow = np.stack(list(dict(sorted(TF_time_dow.items())).values()))

    return FC_dow, Seasenality_dow, Traffic_dow, TF_time_dow


def create_masking_based_on_given_day(DataFrame, test_today, prdMaps):
    """As the name suggests, this function creates masking based on a given day (as today).

    Args:
        DataFrame (DataFrame): The dataframe that we would like to be masked based on a given "today"
        test_today (string,): Based on this given day, we assume this day is today and mask all the data accordingly. Defaults to None.
        prdMaps (Dataframe): Dataframe that shows the time to departure where the period class of a given flight gets closed (it is needed when traffic_time_series=True).

    Returns:
        DataFrame: Returns a DataFrame with maskings based on test_today value.
    """

    arr = prdMaps.iloc[:, 3].values
    try:
        test_today_index = int(DataFrame[DataFrame["forecastDepartureDate"] >= test_today].index[0] / 14)
    except Exception:
        print("No Date after the set fake today date")
        return DataFrame
    test_df = DataFrame.copy()

    current_period = 1
    day_from_today = 0
    for current_index in range(test_today_index, len(test_df) // 14):

        if current_period < 7:
            if day_from_today == arr[current_period]:
                current_period += 1
                # print(current_period)
        else:
            current_period = 7

        day_data_df = test_df[current_index * 14 : (current_index + 1) * 14]
        day_data_df.loc[day_data_df["forecastPeriod"] <= current_period, "trafficActual_1":"trafficActualAadv_10"] = -1

        day_from_today = day_from_today + 1
        # break
    return test_df


def get_tensors2_faketoday(
    DataFarame, DataFarame_Masked, sea_col_Cap, use_channels=True, seasenality_one_dimension=True, window=10
):
    """This function uses a masked dataframe. (it is used when we want to set a fake_today for our test set)

    Args:
        DataFarame (DataFarame): Origenal DataFrame
        DataFarame_Masked (DataFarame): The masked DataFrame
        sea_col_Cap (list): The list of personalities we would like to extract from the DataFrame to be used for our DeepLearning model.
        use_channels (bool, optional): If it is true, it makes our data into 3d tensors by adding traffic flow/local into another dimension. Defaults to True.
        seasenality_one_dimension (bool, optional): Reshape the data into one dimension. Defaults to True.
        window (int, optional): window size for our time-series. Defaults to 10.

    Returns:
        FC (np. tensor): FairClousre Data Tensor. with shape of (data_size, channel, Time_classes, Fair_classes) if FC_time_series = True, shape will be: (data_size, window , channel, Time_classes, Fair_classes)
        Seasonality (np.tensor): Flight Seasonality, the shape of (data_size, Seasenality_size)
        Traffic (np.tensor): Trrafic, used for output. Shape of: (data_size, channel, Time_classes, Fair_classes)
        TF_time (np.tensor): Traffic time-series data for each given flight with the size of (data_size, window, channel, Time_classes, Fair_classes)
    """
    len_sea_cap = len(sea_col_Cap)

    # fractional closure
    PRE_FC_L = DataFarame[["fracClosure_" + str(i + 1) for i in range(10)]].values.astype("float32")
    # seasonality
    PRE_Sea_L = DataFarame[sea_col_Cap].values.astype("float32")
    # actual traffic
    PRE_Traf_L = DataFarame[["trafficActual_" + str(i + 1) for i in range(10)]].values.astype("float32")
    # Masked Traffic
    PRE_Traf_L_Masked = DataFarame_Masked[["trafficActual_" + str(i + 1) for i in range(10)]].values.astype("float32")

    # reshape the data for CNNLSTM model
    FC = PRE_FC_L.reshape(int(PRE_FC_L.shape[0] / 14), 1, 14, 10)
    Seasenality = PRE_Sea_L.reshape(int(PRE_Sea_L.shape[0] / 14), 1, 14, len_sea_cap)
    Traffic = PRE_Traf_L.reshape(int(PRE_Traf_L.shape[0] / 14), 1, 14, 10)
    Traffic_Masked = PRE_Traf_L_Masked.reshape(int(PRE_Traf_L_Masked.shape[0] / 14), 1, 14, 10)

    # Remove Duplicates (from 2d to 1d vector)
    if seasenality_one_dimension:
        Seasenality = np.delete(Seasenality, slice(13), 2).reshape(Seasenality.shape[0], len_sea_cap)

    if use_channels:
        FC = FC.reshape(len(FC), 2, 7, 10)
        Traffic = Traffic.reshape(len(Traffic), 2, 7, 10)
        Traffic_Masked = Traffic_Masked.reshape(len(Traffic_Masked), 2, 7, 10)

    traffic_time_series_window = list()
    # Seasenality_times = list()
    for i in range(window, len(Traffic)):
        # Get Masked Matrix
        tf_window_masked = Traffic_Masked[i + 1 - window : i + 1]
        traffic_time_series_window.append(tf_window_masked)
        # Seasenality_times.append(Seasenality[i-window:i])
    TF_time = np.array(traffic_time_series_window)
    # Seasenality = np.array(Seasenality_times)
    Seasenality = Seasenality[window:]
    FC = FC[window:]

    Traffic = Traffic[window:]

    return FC, Seasenality, Traffic, TF_time


def get_train_test_samples2(
    Data_PRE,
    Data_POST,
    Data_FUTURE,
    sea_col_Cap,
    prdMaps,
    DOW=False,
    train_val_percentage=0.9,
    FC_time_series=False,
    traffic_time_series=True,
    use_channels=True,
    seasenality_one_dimension=True,
    window=10,
    test_random_masking=True,
    test_today=None,
):
    """Given the POST, PRE and FUTURE dataframes this function process them using all the above functions to get the corresponding tensors.
    It returns data as train, val, test, with each having Traffic, Fair-closure, Seasonality, and Traffic time-series data.

    Args:
        Data_PRE (DataFrame): Data that we would like to use for our training/validation sets.
        Data_POST (DataFrame): Data we want to use for our testing stage.
        Data_FUTURE (DataFrame): The future data (the ones we don't have a label for yet - as they have not flown)
        sea_col_Cap (list): The list of seasonalities we would like to extract from the DataFrame to be used for our DeepLearning model.
        prdMaps (Dataframe): Dataframe that shows the time to departure where the period class of a given flight gets closed (it is needed when traffic_time_series=True).
        DOW (bool, optional): _description_. Defaults to False.
        train_val_percentage (float, optional): _description_. Defaults to 0.9.
        FC_time_series (bool, optional): Makes the FC into a time-series. Defaults to False.
        traffic_time_series (bool, optional): Returns a time-series of the traffic of the past days for all datapoints. Defaults to True.
        use_channels (bool, optional): If it is true, it makes our data into 3d tensors by adding traffic flow/local into another dimension. Defaults to True.
        seasenality_one_dimension (bool, optional): Reshape the data into one dimension. Defaults to True.
        window (int, optional): window size for our time-series. Defaults to 10.
        test_random_masking (bool, optional): _description_. Defaults to True.
        test_today (string, optional): If the random_masking is False we should define "fake today", and based on this fake today we'll mask our data. test_today format =  yyyy-mm-dd Defaults to None.

    Returns:
        train (list of tensors): list of tensors for our training dataset.
        val (list of tensors): list of tensors for our validation dataset.
        test (list of tensors): list of tensors for our test dataset.
    """

    if DOW:
        PRE_FC, PRE_Seas, PRE_Traf, PRE_TF_timeseries = dow_get_tensors2(
            Data_PRE,
            sea_col_Cap,
            prdMaps,
            FC_time_series=FC_time_series,
            traffic_time_series=traffic_time_series,
            use_channels=use_channels,
            seasenality_one_dimension=seasenality_one_dimension,
            window=window,
            random_masking=True,
            test_today=None,
        )
        POST_FC, POST_Seas, POST_Traf, POST_TF_timeseries = dow_get_tensors2(
            Data_POST,
            sea_col_Cap,
            prdMaps,
            FC_time_series=FC_time_series,
            traffic_time_series=traffic_time_series,
            use_channels=use_channels,
            seasenality_one_dimension=seasenality_one_dimension,
            window=window,
            random_masking=test_random_masking,
            test_today=test_today,
        )
        # FUTURE_FC , FUTURE_Seas , FUTURE_Traf ,FUTUR_TF_timeseries = dow_get_tensors2(Data_FUTURE , sea_col_Cap, prdMaps  , FC_time_series = False , traffic_time_series = True ,  use_channels = True , seasenality_one_dimension = True ,   window = window)

    else:
        PRE_FC, PRE_Seas, PRE_Traf, PRE_TF_timeseries = get_tensors2(
            Data_PRE,
            sea_col_Cap,
            prdMaps,
            FC_time_series=FC_time_series,
            traffic_time_series=traffic_time_series,
            use_channels=use_channels,
            seasenality_one_dimension=seasenality_one_dimension,
            window=window,
        )

        if test_random_masking:
            POST_FC, POST_Seas, POST_Traf, POST_TF_timeseries = get_tensors2(
                Data_POST,
                sea_col_Cap,
                prdMaps,
                FC_time_series=FC_time_series,
                traffic_time_series=traffic_time_series,
                use_channels=use_channels,
                seasenality_one_dimension=seasenality_one_dimension,
                window=window,
            )
        else:
            masked_df = create_masking_based_on_given_day(Data_POST, test_today, prdMaps)
            POST_FC, POST_Seas, POST_Traf, POST_TF_timeseries = get_tensors2_faketoday(
                Data_POST, masked_df, sea_col_Cap, use_channels, seasenality_one_dimension, window
            )

        # FUTURE_FC , FUTURE_Seas , FUTURE_Traf , FUTURE_TF_timeseries = get_tensors2(Data_FUTURE, sea_col_Cap, prdMaps , FC_time_series = False , traffic_time_series = True ,  use_channels = True , seasenality_one_dimension = True ,   window = window)

    # Train/Val Spilit:
    # TODO: THIS SHOULD BE CHANGED TO RANDOMIZED.
    train_val_cutoff = round(PRE_FC.shape[0] * train_val_percentage)

    # prepare train/val/test datasets
    PRE_FC_train = PRE_FC[:train_val_cutoff, :]
    PRE_FC_val = PRE_FC[train_val_cutoff:, :]

    PRE_Seas_train = PRE_Seas[:train_val_cutoff, :]
    PRE_Seas_val = PRE_Seas[train_val_cutoff:, :]

    PRE_Traf_train = PRE_Traf[:train_val_cutoff, :]
    PRE_Traf_val = PRE_Traf[train_val_cutoff:, :]

    PRE_TF_timeseries_train = PRE_TF_timeseries[:train_val_cutoff, :]
    PRE_TF_timeseries_val = PRE_TF_timeseries[train_val_cutoff:, :]

    train = [PRE_FC_train, PRE_Seas_train, PRE_TF_timeseries_train, PRE_Traf_train]
    val = [PRE_FC_val, PRE_Seas_val, PRE_TF_timeseries_val, PRE_Traf_val]
    test = [POST_FC, POST_Seas, POST_TF_timeseries, POST_Traf]

    return train, val, test
