/*
FrostyFriday Inc., your benevolent employer, has an S3 bucket that is filled with .csv data dumps. This data is needed for analysis. Your task is to create an external stage, and load the csv files directly from that stage into a table.

The S3 bucket’s URI is: s3://frostyfridaychallenges/challenge_1/

目的：S3バケットからのファイル取得とテーブル作成
*/

/*
目的：S3バケットからのファイル取得とテーブル作成
*/


-- データベースとスキーマの使用
USE FROSTY_FRIDAY.ANSWER;

-- 1. 外部ステージを作成 (URLのスペース修正)
CREATE OR REPLACE STAGE week1_stage_infer_schema
    URL = 's3://frostyfridaychallenges/challenge_1/';

-- 2. ファイルパス指定
SET stage_path = '@week1_stage_infer_schema';

-- 3. ファイルフォーマット指定
CREATE OR REPLACE FILE FORMAT my_csv_format
    TYPE = CSV
    SKIP_HEADER = 1;

-- --- INFER_SCHEMAによるスキーマ推論（表示用：単独実行を推奨）---
-- FILE_FORMATは引用符なしの大文字名で参照します
SELECT * FROM TABLE(
    INFER_SCHEMA(
        LOCATION => $stage_path, 
        FILE_FORMAT => 'my_csv_format' 
    )
);

-- 4. スキーマを基にテーブル作成 (FILE_FORMATの引用符と参照名を修正)
CREATE OR REPLACE TABLE raw_data_infer
    USING TEMPLATE (
        SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
        FROM TABLE(
            INFER_SCHEMA(
                LOCATION => $stage_path, 
                FILE_FORMAT => 'my_csv_format' 
            )
        )
    );

-- 5. データのロード (ステージ名とFILE_FORMATを修正)
COPY INTO raw_data_infer
FROM @week1_stage_infer_schema
FILE_FORMAT = MY_CSV_FORMAT 
PATTERN = '.*week1.*';

-- 6. 結果の確認 (単独実行を推奨)
SELECT * FROM raw_data_infer;