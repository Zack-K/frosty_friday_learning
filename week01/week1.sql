/*
FrostyFriday Inc., your benevolent employer, has an S3 bucket that is filled with .csv data dumps. This data is needed for analysis. Your task is to create an external stage, and load the csv files directly from that stage into a table.

The S3 bucket’s URI is: s3://frostyfridaychallenges/challenge_1/

目的：S3バケットからのファイル取得とテーブル作成
*/

/*
目的：S3バケットからのファイル取得とテーブル作成
*/

drop schema FROSTY_FRIDAY.ANSWER;

create database if not exists FROSTY_FRIDAY;
create schema if not exists ANSWER;
use schema FROSTY_FRIDAY.ANSWER;

CREATE OR REPLACE STAGE s3
    URL = 's3://frostyfridaychallenges/challenge_1/'
;

list@FROSTY_FRIDAY.ANSWER.S3;

create or replace file format csv_file_format
    type = csv
;

-- データの値とメタデータを突き合わせて読み込みを行い、データのチェック
select $1, metadata$filename, metadata$file_row_number from @FROSTY_FRIDAY.ANSWER.S3 (file_format=>'csv_file_format');

-- ファイルフォーマットの更新 NULLを許さない形で登録する設定
create or replace file format csv_file_format
    type = csv 
    skip_header = 1
    null_if = ('NULL', 'total_empty')
    skip_blank_lines = true
    comment = '"null_if" is used to eliminate useless values'
;

create or replace table week1_csv(
    result varchar,
    filename varchar,
    file_row_number int,
    loaded_at timestamp_ltz
)
;

copy into week1_csv from(
    select
        $1,
        metadata$filename, 
        metadata$file_row_number,
        metadata$start_scan_time
    from @FROSTY_FRIDAY.ANSWER.S3)
     file_format = (format_name = 'csv_file_format')
;
delete from week1_csv where result is null;

select * from week1_csv;

drop database FROSTY_FRIDAY;