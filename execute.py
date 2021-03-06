import os, sys
import datetime
import calendar
import time
import string
import random
from urllib.parse import urlparse
import pandas as pd
import numpy as np
import timeit
import pyarrow.parquet as pq
import pyarrow as pa
import datetime
import logging

import findspark
findspark.init('/usr/lib/spark2')
from pyspark.sql import SparkSession
from pyspark.sql import functions as F, types as T, Window
import wmfdata.spark as wmfspark

## defining the spark session
spark_config = {}
spark = wmfspark.get_session(
    app_name='Pyspark notebook',
    type='regular'
#     extra_settings=spark_config
)
spark

## Receive params
if (len(sys.argv) == 4):
    exec_year = sys.argv[1]
    exec_month = sys.argv[2]
    exec_day = sys.argv[3]
    exec_filename = "dat_of_data_"
else:
    print(datetime.datetime.now(), "error: input incorrect")
    sys.exit("error: input incorrect:" + str(sys.argv))

logging.basicConfig(
    filename='logs/' + str(exec_year) + str(exec_month).zfill(2) + str(exec_day).zfill(2) + "_run.log",
    format='%(asctime)s %(message)s',
    filemode='w'
)
logger=logging.getLogger()
logger.setLevel(logging.DEBUG)

## logprint function
def logprint(msg):
    print(datetime.datetime.now(), msg)
    logger.info(msg)

logprint("reading execute.py file")

### parsing uri-query field in webrequest
parse_uri_query = """parse_url(concat('http://bla.org/woo/', uri_query), 'QUERY', '{0}')"""
## hashing user-agent and client-ip (with salt) to generate pseudo user-id
parse_user_id = """sha2(CONCAT(user_agent, client_ip, '{0}'), 256)"""
# salts for UA/IP hash (1st = userhash one day)
# from https://github.com/geohci/covid-19-sessions/blob/master/covid_19_data.ipynb
salt_one = ''.join(random.choice(string.ascii_lowercase + string.ascii_uppercase + string.digits) for _ in range(random.randint(8,16)))

## windowing function
w = Window.partitionBy(F.col('user_id'))

def set_parquet_paths(
    year = 2021,
    month = 10,
    day=1,
    folder = 'output/',
    filename_main = 'day_of_data_',
    skiptestlocal = False
):
    main_output = folder + filename_main
    PATH_hadoop = "/user/effeietsanders/"
    FILE_hadoop = main_output + str(year) + str(month).zfill(2) + str(day).zfill(2) + '.parquet'
    PATH_local = "/home/effeietsanders/shared_notebooks/"
    FILE_local = main_output + str(year) + str(month).zfill(2) + str(day).zfill(2) + '.parquet'
    filename = filename_main + str(year) + str(month).zfill(2) + str(day).zfill(2) + '.parquet'
    localfile_exists = os.path.isdir(PATH_local + FILE_local)
    if (localfile_exists):
        if (skiptestlocal):
            pass
        else:
            logprint("file already exists locally: "+ str(PATH_local) + str(FILE_local))
            sys.exit("file already exists locally")
    hadoopfile_exists = os.system("hadoop fs -ls %s" % (PATH_hadoop + FILE_hadoop) )
    if (hadoopfile_exists == 0):
        logprint("error: file already exist on hadoop")
        sys.exit("file already exist on hadoop")
    logprint("success: strings set")
    return(
        PATH_hadoop,
        FILE_hadoop,
        PATH_local,
        FILE_local,
        filename
    )

# copy of get_pd_df2 but then try without toPandas()
def get_pq(
    query_year = 2021,
    query_month = 10,
    query_day = 1,
#     query_hour = 1,
    FILE_hadoop='output/testing.parquet'
):
    df = (
        spark.read.table("wmf.webrequest")
        ## specify time-window (snapshot)
        .where(F.col("year")==query_year)
        .where(F.col("month")==query_month)
        .where(F.col("day")==query_day)
#         .where(F.col("hour")==query_hour)

        ## generate user id
        .withColumn("user_id", F.expr(parse_user_id.format(salt_one)) )

        ## agent-type user to filter spiders
        ## https://meta.wikimedia.org/wiki/Research:Page_view/Tags#Spider
        .where(F.col("agent_type") == "user")
        .where(F.col("webrequest_source") == "text")
        ## only users that are not logged in?
        .withColumn('logged_in', F.coalesce(F.col('x_analytics_map.loggedIn'),F.lit(0)) )
#         .where( F.col('logged_in') == 0 )
        ## drop requests with no timestamps
        .where(F.col("dt")!='-')

        ## select banner-impressions or pageviews
        ## or special pages CreateAccount or Upload or UploadWizard
        .withColumn("uri_title",F.expr(parse_uri_query.format("title")))

        .where(
            (F.col("is_pageview")==1)|\
            F.col("uri_title").isin(
                "Special:BannerLoader",
                "Special:CreateAccount",
                "??????????????????:CreateAccount",
                "X??susi:HesabA??",
                "Especial:Criar_conta",
                "Spezial:Benutzerkonto_anlegen",
                "??????????:??????????_????????????",
                "????????:??????????_????????_????????????",
                "Posebno:Stvori_ra??un",
                "Special:??nregistrare",
                "????????????????????:????????????????????????????????????????????",
                "??????????????????:??????????????_??????????????_????????????",
                "????????????????????:????????????????_??????????????????_??????????",
                "Special:Upload",
                "Special:UploadWizard"
            )
        )

        ## create columns for campaign-properties
        .withColumn("bl_campaign", F.expr(parse_uri_query.format("campaign")))
        .withColumn("bl_banner", F.expr(parse_uri_query.format("banner")))
#         .withColumn("bl_uselang", F.expr(parse_uri_query.format("uselang")))

        ## create a column to indicate whether a request came from a wlm-banner impression (otherwise None)
        .withColumn(
            "bi_iswlm",
            F.when(
                F.col("bl_banner").startswith("wlm_2021"),
                1
            ).otherwise(None)
        )

        .withColumn(
            "sp_createaccount",
            F.when(
                F.col("uri_title").isin(
                    "Special:CreateAccount",
                    "??????????????????:CreateAccount",
                    "X??susi:HesabA??",
                    "Especial:Criar_conta",
                    "Spezial:Benutzerkonto_anlegen",
                    "??????????:??????????_????????????",
                    "????????:??????????_????????_????????????",
                    "Posebno:Stvori_ra??un",
                    "Special:??nregistrare",
                    "????????????????????:????????????????????????????????????????????",
                    "??????????????????:??????????????_??????????????_????????????",
                    "????????????????????:????????????????_??????????????????_??????????"
                ),
                1
            ).otherwise(None)
        )

        .withColumn(
            "sp_upload",
            F.when(
                F.col("uri_title").isin("Special:Upload", "Special:UploadWizard"),
                1
            ).otherwise(None)
        )

        ## mark all requests from users that saw a wlm-banner
        .withColumn('user_wlm', F.max(F.col("bi_iswlm")).over(w))
        ## keep all requests from users that saw a wlm-banner
        .where(F.col("user_wlm").isNotNull())

        ## rename some columns
        .withColumn("page_title",F.col('pageview_info.page_title'))
        .withColumn("country",F.col('geocoded_data.country'))
        .withColumn("project_family",F.col("normalized_host.project_family"))
        .withColumn("project",F.col("normalized_host.project"))
    )
    # convert to pandas to make it easier to inspect and process
    return(df.write.parquet(FILE_hadoop))


# TODO: fix bad encoding for AZ, IR, UA?

landingpages = pd.DataFrame([
    ['wikipedia', 'hy', '??????????????????:??????????_????????????_??_????????????????????????_2021', 'am'],
    ['wikipedia', 'az', 'Vikipediya:Viki_Abid%C9%99l%C9%99ri_Sevir_2021', 'az'],
    ['external', 'external', 'http://wlm.wikimedia.rs.ba/', 'ba'],
    ['wikimedia', 'commons', 'Commons:Wiki_Loves_Monuments_2021_in_Benin', 'bj'],
    ['wikipedia', 'pt', 'Wikip??dia:Wiki_Loves_Monuments_2021/Brasil', 'br'],
    ['wikipedia', 'de', 'Wikipedia:Wiki_Loves_Monuments_2021/Deutschland', 'de'],
    ['wikimedia', 'commons', 'Commons:Wiki_Loves_Monuments_2021_in_Algeria', 'dz'],
    ['external', 'external', 'https://www.wikilov.es/es/Wiki_Loves_Monuments', 'es'],
    ['external', 'external', 'http://wlm.wikimedia.fi/', 'fi'],
    ['wikimedia', 'commons', 'Commons:Wiki_Loves_Monuments_2021_in_France', 'fr'],
    ['wikimedia', 'commons', 'Commons:Wiki_Loves_Monuments_2021_in_Ghana', 'gh'],
    ['external', 'external', 'https://wlm.wikimedia.gr/', 'gr'],
    ['wikipedia', 'he', '????????????????:??????????_????????????????/????????????????_??????????_????????_??????????/??????????_??????????????', 'il'],
    ['wikipedia', 'fa', '????????%E2%80%8C????????:????????_??????????????_????????????%E2%80%8C????_????????_??????????', 'ir'],
    ['wikipedia', 'hr', 'Wikipedija:Wiki_voli_spomenike', 'hr'],
    ['wikimedia', 'commons', 'Commons:Wiki_Loves_Monuments_2021_in_Ireland', 'ie'],
    ['wikimedia', 'commons', 'Commons:Wiki_Loves_Monuments_2021_in_India', 'in'],
    ['external', 'external', 'https://wikilovesmonuments.wikimedia.it/', 'it'],
    ['wikipedia', 'ro', 'Wikipedia:Wiki_Loves_Monuments/Moldova', 'md'],
    ['wikipedia', 'mk', '????????????????????:????????_????_????????_??????????????????????_2021/????????????????????', 'mk'],
    ['wikimedia', 'commons', 'Commons:Wiki_Loves_Monuments_2021_in_Malta', 'mt'],
    ['wikimedia', 'commons', 'Commons:Wiki_Loves_Monuments_2021_in_Malaysia', 'my'],
    ['wikimedia', 'commons', 'Commons:Wiki_Loves_Monuments_2021_in_Peru', 'pe'],
    ['wikimedia', 'commons', 'Commons:Wiki_Loves_Monuments_2021_in_Pakistan', 'pk'],
    ['external', 'external', 'https://wikimedia.pl/zabytki', 'pl'],
    ['external', 'external', 'https://wikilovesmonuments.org.pt/', 'pt'],
    ['wikimedia', 'commons', 'Commons:Wiki_Loves_Monuments_2021_in_Qatar', 'qa'],
    ['wikivoyage', 'ru', 'Wikivoyage:????????_??????????_??????????????????_2021', 'ru'],
    ['wikimedia', 'commons', 'Commons:Wiki_Loves_Monuments_2021_in_Rwanda', 'rw'],
    ['wikimedia', 'commons', 'Commons:Wiki_Loves_Monuments_2021_in_Sweden/sv', 'se'],
    ['wikimedia', 'commons', 'Commons:Wiki_Loves_Monuments_2021_in_Slovenia', 'si'],
    ['wikimedia', 'commons', 'Commons:Wiki_Loves_Monuments_2021_in_El_Salvador', 'sv'],
    ['wikimedia', 'commons', 'Commons:Wiki_Loves_Monuments_2021_in_Taiwan', 'tw'],
    ['wikipedia', 'uk', '??????????????????:????????_????????????_??????%27????????', 'ua'],
    ['wikimedia', 'commons', 'Commons:Wiki_Loves_Monuments_2021_in_Uganda', 'ug'],
    ['wikimedia', 'commons', 'Commons:Wiki_Loves_Monuments_2021_in_the_United_States', 'us'],
    ['wikimedia', 'commons', 'Commons:Wiki_Loves_Monuments_2021_in_Zimbabwe', 'zw'],
    ['wikimedia', 'commons', 'Commons:Czech_Wiki_Photo', 'cz'],
    ['wikimedia', 'commons', 'Commons:Wiki_Loves_Fashion_in_the_Philippines', 'ph']
], columns=['project_family', 'project', 'page_title', 'country'])
banners = [
    ['wlm_2021_zw', 'zw'],
    ['wlm_2021_us', 'us'],
    ['wlm_2021_ug', 'ug'],
    ['wlm_2021_ua', 'ua'],
    ['wlm_2021_tw', 'tw'],
    ['wlm_2021_sv', 'sv'],
    ['wlm_2021_si', 'si'],
    ['wlm_2021_se', 'se'],
    ['wlm_2021_rw', 'rw'],
    ['wlm_2021_ru', 'ru'],
    ['wlm_2021_qa', 'qa'],
    ['wlm_2021_pt', 'pt'],
    ['wlm_2021_pl', 'pl'],
    ['wlm_2021_PK', 'pk'],
    ['wlm_2021_pe', 'pe'],
    ['wlm_2021_my', 'my'],
    ['wlm_2021_mt', 'mt'],
    ['wlm_2021_mk', 'mk'],
    ['wlm_2021_md', 'md'],
    ['wlm_2021_it', 'it'],
    ['wlm_2021_ir', 'ir'],
    ['wlm_2021_in', 'in'],
    ['wlm_2021_il', 'il'],
    ['wlm_2021_ie', 'ie'],
    ['wlm_2021_hr', 'hr'],
    ['wlm_2021_gr', 'gr'],
    ['wlm_2021_gh', 'gh'],
    ['wlm_2021_fr', 'fr'],
    ['wlm_2021_fi', 'fi'],
    ['wlm_2021_es', 'es'],
    ['wlm_2021_dz', 'dz'],
    ['wlm_2021_de', 'de'],
    ['wlm_2021_br', 'br'],
    ['wlm_2021_bj', 'bj'],
    ['wlm_2021_ba_srp', 'ba'],
    ['wlm_2021_az', 'az'],
    ['wlm_2021_am', 'am'],
    ['wmcz_czech_wiki_photo_2021', 'cz'],
    ['wmcz_czech_wiki_photo_2021_late', 'cz'],
    ['WLFashion Philippines', 'ph']
]
# Data from https://petscan.wmflabs.org/?psid=20657414
monument_lists_de = pd.read_csv('data/monument_lists_de.csv')
monument_lists_de['project_family'] = 'wikipedia'
monument_lists_de['project'] = 'de'

# Data from https://petscan.wmflabs.org/?psid=20654082
monument_lists_us = pd.read_csv('data/monument_lists_us.csv')
monument_lists_us['project_family'] = 'wikipedia'
monument_lists_us['project'] = 'en'

# Set a maximum number of banner views that we report for anonymity
banner_view_cap = 10.0
landing_view_cap = 1
listpg_seen_cap = 10

def qualify_pages (df, returnme = True, lp=landingpages):
    df.sort_values(by="ts", inplace=True)
    df['pg_landing'] = False
    for i in landingpages.index:
        df['pg_landing'] = df['pg_landing'] | (
            df[['project_family', 'project', 'page_title']]==landingpages.loc[i, ['project_family', 'project', 'page_title']]
        ).all(axis=1)
    df['pg_commons_help'] = (df[['project_family', 'project', 'namespace_id']]==['wikimedia', 'commons', 6]).all(axis=1)
    df['pg_commons_commons'] = (df[['project_family', 'project', 'namespace_id']]==['wikimedia', 'commons', 4]).all(axis=1)

    df['pg_list_de'] = pd.merge(
        left=df,
        right=monument_lists_de[['project_family', 'project', 'pageid']],
        how='left',
        left_on=['project_family', "page_id"],
        right_on=['project_family', 'pageid'],
        copy=False,
        indicator=True
    )['_merge'] == 'both'
    df['pg_list_us'] = pd.merge(
        left=df,
        right=monument_lists_us[['project_family', 'project', 'pageid']],
        how='left',
        left_on=['project_family', "page_id"],
        right_on=['project_family', 'pageid'],
        copy=False,
        indicator=True
    )['_merge'] == 'both'
    df['banners_seen'] = df.groupby(['user_id'])['bi_iswlm'].cumsum().fillna(method='ffill').fillna(0).astype('int')
    df.loc[df['banners_seen'] > banner_view_cap,'banners_seen'] = banner_view_cap
    df['landing_seen'] = df.groupby(['user_id'])['pg_landing'].cumsum().fillna(method='ffill').fillna(0).astype('int')
    df.loc[df['landing_seen'] > landing_view_cap,'landing_seen'] = landing_view_cap
    df['listpg_us_seen'] = df.groupby(['user_id'])['pg_list_us'].cumsum().fillna(method='ffill').fillna(0).astype('int')
    df.loc[df['listpg_us_seen'] > listpg_seen_cap,'listpg_us_seen'] = listpg_seen_cap
    df['listpg_de_seen'] = df.groupby(['user_id'])['pg_list_de'].cumsum().fillna(method='ffill').fillna(0).astype('int')
    df.loc[df['listpg_de_seen'] > listpg_seen_cap,'listpg_de_seen'] = listpg_seen_cap
    if returnme:
        return(df)


def get_df_pd(PATH_local, FILE_local):
    localfile_exists = os.path.isdir(PATH_local + FILE_local)
    if (localfile_exists):
        logprint("file exists locally")
    else:
        logprint("error: file does not exist locally")
        sys.exit("file does not exist locally")
    logprint("converting to pd")
    df_pd = pd.read_parquet(
        path = FILE_local,
        columns = [
            'year',
            'month',
            'day',
            'hour',
            'ts',
            'user_id',
            'logged_in',
            'is_pageview',
            'access_method',
            'referer_class',
            'bl_campaign',
            'bl_banner',
            'bi_iswlm',
            'sp_createaccount',
            'sp_upload',
            'user_wlm',
            'namespace_id',
            'page_title',
            'page_id',
            'project_family',
            'project'
        ]
    )
    logprint("adding new columns")
    df_out = qualify_pages(df_pd)
    logprint("done with get_df_pd")
    return(df_out)


def get_day_of_data(
    year=2021,
    month=10,
    day=1,
    filename = 'day_of_data_'
):
    PATH_hadoop, FILE_hadoop, PATH_local, FILE_local, filename_set = set_parquet_paths(
        year=year,
        month=month,
        day=day,
        folder = 'output/',
        filename_main = filename
    )
    get_pq(
        query_year = year,
        query_month = month,
        query_day = day,
        FILE_hadoop = FILE_hadoop
    )
    hadoopfile_exists = os.system("hadoop fs -ls %s" % (PATH_hadoop + FILE_hadoop) )
    if (hadoopfile_exists == 0):
        logprint("good: file exists on hadoop")
    else:
        logprint("error: does file exist on hadoop?")
        sys.exit("does file exist on hadoop?")
    transfer_success = os.system("hadoop fs -get %s %s" % (PATH_hadoop + FILE_hadoop, PATH_local + FILE_local))
    if (transfer_success == 0):
        logprint("good: file transferred")
    else:
        logprint("error: was transfer successful?")
        sys.exit("was transfer successful?")
    delete_success = os.system('hadoop fs -rm -r ' + PATH_hadoop + FILE_hadoop)
    if (delete_success == 0):
        logprint("good: file on hadoop deleted")
    else:
        logprint("error: was deletion on hadoop successful?")
        sys.exit("was deletion on hadoop successful?")
    df_pd = get_df_pd(
        PATH_local = PATH_local,
        FILE_local = FILE_local
    )
    logprint("success: done with get_day_of_data")
    return(df_pd)


# Create a table with only banner impressions
bi_cols_out = [
    'year', 'month', 'day',
    'access_method', #mobile web, desktop, mobile app
    'bl_campaign', 'bl_banner', #should contain same information
    'logged_in', #binary
    'referer_class', # 'internal', 'external (search engine)', 'none', 'external'
    'project_family', 'project',
    'user_wlm', #sanity check, should always be 1
    'banners_seen', #count with cap at 10
    'landing_seen' #count with cap at 1
              ]

lp_cols_out = [
    'year', 'month', 'day',
    'access_method', #mobile web, desktop, mobile app
    'logged_in', #binary
    'referer_class', # 'internal', 'external (search engine)', 'none', 'external'
    'project_family', 'project',
    'page_title',
    'user_wlm', #sanity check, should always be 1
    'banners_seen', #count with cap at 10
    'landing_seen' #sanity check, should always be 1
]

ca_cols_out = [
    'year', 'month', 'day',
    'access_method', #mobile web, desktop, mobile app
    'logged_in', #sanity check, should be 0 mostly
    'referer_class', # 'internal', 'external (search engine)', 'none', 'external'
    'project_family', 'project',
    'sp_createaccount', #sanity check, should always be 1
    'user_wlm', #sanity check, should always be 1
    'banners_seen', #count with cap at 10
    'landing_seen' #sanity check, should be 1 mostly
]

up_cols_out = [
    'year', 'month', 'day',
    'access_method', #mobile web, desktop, mobile app
    'logged_in', #sanity check, should be always 1
    'referer_class', # 'internal', 'external (search engine)', 'none', 'external'
    'project_family', 'project', #sanity check, should always be Wikimedia Commons
    'sp_createaccount', #sanity check, should always be 0
    'user_wlm', #sanity check, should always be 1
    'banners_seen', #count with cap at 10
    'landing_seen', #sanity check, should be 1 mostly
    'listpg_us_seen', 'listpg_de_seen'
]
def report_uniques (df):
    logprint("length of df:" + str(len(df)))
    for col in df.columns:
        logprint("Values of " + str(col) + " are: " + str(df[col].unique()))

def get_cleaned_df(
    df,
    filter_col,
    cols_out
):
    logprint('starting export of csv for ' + filter_col)
    df_out = df.loc[df[filter_col] > 0,cols_out]
    df_out = df_out.sample(frac=1).reset_index(drop=True)
    logprint('done export of csv for ' + filter_col)
    return(df_out)


def clean_data_make_csv(
    df_pd,
    cols_out_bi = bi_cols_out,
    cols_out_lp = lp_cols_out,
    cols_out_ca = ca_cols_out,
    cols_out_up = up_cols_out,
    PATH_local = '/home/effeietsanders/shared_notebooks/',
    FILE_out_base = 'output/csv/day_of_data_testing'
):
    logprint("--- converting bi data")
    df_bi = get_cleaned_df(df=df_pd,filter_col='bi_iswlm',cols_out=cols_out_bi)
    report_uniques(df_bi)
    df_bi.to_csv(FILE_out_base + "_bi.csv")
    logprint("--- converting lp data")
    df_lp = get_cleaned_df(df=df_pd,filter_col='pg_landing',cols_out=cols_out_lp)
    report_uniques(df_lp)
    df_lp.to_csv(FILE_out_base + "_lp.csv")
    logprint("--- converting ca data")
    df_ca = get_cleaned_df(df=df_pd,filter_col='sp_createaccount',cols_out=cols_out_ca)
    report_uniques(df_ca)
    df_ca.to_csv(FILE_out_base + "_ca.csv")
    logprint("--- converting up data")
    df_up = get_cleaned_df(df=df_pd,filter_col='sp_upload',cols_out=cols_out_up)
    report_uniques(df_up)
    df_up.to_csv(FILE_out_base + "_up.csv")
    logprint("success cleaning data and making csv's!")


def agg_users(df):
    logprint('preformatting grouping users')
    # df['logged_in'] = df[['logged_in']].fillna(0)
    # df['logged_in'] = pd.to_numeric(df['logged_in'], downcast = "unsigned")
#     df['bl_banner'] = df['bl_banner'].astype("category") # Doesn't seem to work
    logprint('grouping users')
    df_grouped = df[df['user_wlm'] > 0][[
        'user_id',
        'user_wlm',
        'logged_in',
        'bl_banner',
        'sp_createaccount',
        'sp_upload',
        'landing_seen',
        'listpg_us_seen',
        'listpg_de_seen'
    ]].groupby('user_id')
    logprint('aggregating')
    df_out = pd.DataFrame()
    logprint('aggregating logged_in')
    df_out['logged_in'] = df_grouped['logged_in'].unique()
#     logprint('aggregating bl_campaign')
#     df_out['bl_campaign'] = df_grouped['bl_campaign'].unique()
    logprint('aggregating bl_banner')
    df_out['bl_banner'] = df_grouped['bl_banner'].unique()
    logprint('aggregating sp_createaccount')
    df_out['sp_createaccount'] = df_grouped['sp_createaccount'].max()
    logprint('aggregating sp_upload')
    df_out['sp_upload'] = df_grouped['sp_upload'].max()
    logprint('aggregating landing_seen')
    df_out['landing_seen'] = df_grouped['landing_seen'].max()
    logprint('aggregating listpg_us_seen')
    df_out['listpg_us_seen'] = df_grouped['listpg_us_seen'].max()
    logprint('aggregating listpg_de_seen')
    df_out['listpg_de_seen'] = df_grouped['listpg_de_seen'].max()
    logprint('anonymizing')
    df_out = df_out.sample(frac=1).reset_index(drop=True)
    logprint('success: done with aggregating users')
    return(df_out)

def delete_local_parquet(FILE_local):
    localfile_exists = os.path.isdir(FILE_local)
    if (localfile_exists):
        logprint("file exists locally")

    else:
        logprint("file does not exist locally")
        return("file does not exist locally")
    os.system('rm -r ' + FILE_local)
    localfile_exists = os.path.isdir(FILE_local)
    if (localfile_exists):
        logprint("file exists locally")
        return("error: file still exists locally")
    else:
        logprint("file does not exist locally")
        return("file does not exist locally")
# execute

logprint("starting for " + str(exec_year) +  str(exec_month).zfill(2) +  str(exec_day).zfill(2))

df_pd = get_day_of_data(
    year=exec_year,
    month=exec_month,
    day=exec_day,
    filename = "day_of_data_"
)

clean_data_make_csv(
    df_pd = df_pd,
    FILE_out_base = 'output/csv/day_of_data_' + str(exec_year) + str(exec_month).zfill(2) + str(exec_day).zfill(2)
)


df_agg_users = agg_users(df_pd)
report_uniques(
    df_agg_users[[
        'sp_createaccount',
        'sp_upload',
        'landing_seen',
        'listpg_us_seen',
        'listpg_de_seen'
    ]]
)
logprint('exporting to csv')
df_agg_users.to_csv('output/csv/day_of_data_' + str(exec_year) + str(exec_month).zfill(2) + str(exec_day).zfill(2) + '_agg_users.csv')
df_agg_users.head()
logprint('done, dont forget to delete data!')
