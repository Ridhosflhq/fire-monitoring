import os
import json
import pandas as pd
import geopandas as gpd
import gspread
import requests

from gspread_dataframe import set_with_dataframe
from datetime import datetime, timedelta, timezone

# =========================================================
# GOOGLE AUTH
# =========================================================

service_account_info = json.loads(
    os.environ["GCP_CREDENTIALS"]
)

gc = gspread.service_account_from_dict(
    service_account_info
)

# =========================================================
# SOURCE SPREADSHEET
# =========================================================

spreadsheet_id_source = (
    "1s7jnrnlSpyGdKHuNYK1XasveO7u_ESTv1mkpxQXTHAI"
)

sh_source = gc.open_by_key(
    spreadsheet_id_source
)

worksheet_source = sh_source.get_worksheet(0)

df = pd.DataFrame(
    worksheet_source.get_all_records()
)

# =========================================================
# CHECK EMPTY
# =========================================================

if df.empty:

    print("No hotspot data found.")

else:

    # =====================================================
    # CLEAN COLUMN
    # =====================================================

    df.columns = (
        df.columns
        .astype(str)
        .str.strip()
        .str.lower()
    )

    # =====================================================
    # REQUIRED COLUMN CHECK
    # =====================================================

    required_cols = [
        "latitude",
        "longitude",
        "acq_date",
        "acq_time",
        "satellite",
        "instrument"
    ]

    missing_cols = [
        c for c in required_cols
        if c not in df.columns
    ]

    if missing_cols:

        raise Exception(
            f"Missing columns: {missing_cols}"
        )

    # =====================================================
    # SELECT COLUMNS
    # =====================================================

    df = df[required_cols]

    # =====================================================
    # CLEAN COORDINATE
    # =====================================================

    df["latitude"] = (
        df["latitude"]
        .astype(str)
        .str.replace(",", ".")
        .astype(float)
    )

    df["longitude"] = (
        df["longitude"]
        .astype(str)
        .str.replace(",", ".")
        .astype(float)
    )

    # =====================================================
    # UTC → WIB CONVERSION
    # =====================================================

    def convert_datetime(row):

        try:

            acq_date = str(row["acq_date"])

            acq_time = str(
                int(row["acq_time"])
            ).zfill(4)

            hh = int(acq_time[:2])

            mm = int(acq_time[2:])

            utc_dt = datetime.strptime(
                f"{acq_date} {hh}:{mm}",
                "%Y-%m-%d %H:%M"
            )

            utc_dt = utc_dt.replace(
                tzinfo=timezone.utc
            )

            wib = timezone(
                timedelta(hours=7)
            )

            wib_dt = utc_dt.astimezone(wib)

            return pd.Series({

                "Tanggal":
                wib_dt.strftime("%Y-%m-%d"),

                "Jam":
                wib_dt.strftime("%H:%M:%S")

            })

        except Exception as e:

            print(
                f"Datetime conversion error: {e}"
            )

            return pd.Series({

                "Tanggal": None,
                "Jam": None

            })

    df[["Tanggal", "Jam"]] = df.apply(
        convert_datetime,
        axis=1
    )

    # =====================================================
    # REMOVE INVALID DATETIME
    # =====================================================

    df = df.dropna(
        subset=["Tanggal", "Jam"]
    )

    # =====================================================
    # CREATE GEODATAFRAME
    # =====================================================

    gdf_points = gpd.GeoDataFrame(

        df,

        geometry=gpd.points_from_xy(
            df["longitude"],
            df["latitude"]
        ),

        crs="EPSG:4326"

    )

    # =====================================================
    # LOAD SPATIAL DATA
    # =====================================================

    desa_path = "data/Desa.json"

    pemilik_path = "data/PemilikLahan.json"

    blok_path = "data/blok.json"

    print("Loading Desa layer...")
    gdf_desa = gpd.read_file(
        desa_path
    ).to_crs("EPSG:4326")

    print("Loading Owner layer...")
    gdf_pemilik = gpd.read_file(
        pemilik_path
    ).to_crs("EPSG:4326")

    print("Loading Blok layer...")
    gdf_blok = gpd.read_file(
        blok_path
    ).to_crs("EPSG:4326")

    # =====================================================
    # LOAD LULC
    # =====================================================

    lulc_url = (
        "https://drive.google.com/uc?"
        "export=download&id="
        "1uy1VJruyiwsZBcdv5YYRTI9EcAWZVB2O"
    )

    os.makedirs("data", exist_ok=True)

    lulc_path = "data/LULC.json"

    if not os.path.exists(lulc_path):

        print("Downloading LULC layer...")

        r = requests.get(
            lulc_url,
            timeout=60
        )

        r.raise_for_status()

        with open(lulc_path, "wb") as f:

            f.write(r.content)

    print("Loading LULC layer...")

    gdf_lulc = gpd.read_file(
        lulc_path
    ).to_crs("EPSG:4326")

    # =====================================================
    # SPATIAL JOIN
    # =====================================================

    print("Spatial join Desa...")

    gdf_join = gpd.sjoin(

        gdf_points,

        gdf_desa[
            ["nama_kel", "geometry"]
        ],

        predicate="intersects"

    ).drop(
        columns=["index_right"]
    )

    print("Spatial join Owner...")

    gdf_join = gpd.sjoin(

        gdf_join,

        gdf_pemilik[
            ["Owner", "geometry"]
        ],

        predicate="intersects"

    ).drop(
        columns=["index_right"]
    )

    print("Spatial join Blok...")

    gdf_join = gpd.sjoin(

        gdf_join,

        gdf_blok[
            ["Blok", "geometry"]
        ],

        predicate="intersects"

    ).drop(
        columns=["index_right"]
    )

    print("Spatial join LULC...")

    gdf_join = gpd.sjoin(

        gdf_join,

        gdf_lulc[
            ["Class23", "geometry"]
        ],

        predicate="intersects"

    ).drop(
        columns=["index_right"]
    )

    # =====================================================
    # RENAME COLUMN
    # =====================================================

    gdf_result = gdf_join.rename(columns={

        "nama_kel": "Desa",

        "Owner": "Owner",

        "Class23": "Penutup Lahan"

    })

    # =====================================================
    # SAFE COLUMN
    # =====================================================

    for col in [
        "Owner",
        "Desa",
        "Penutup Lahan",
        "Blok"
    ]:

        if col not in gdf_result.columns:

            gdf_result[col] = None

    # =====================================================
    # FORMAT COLUMN
    # =====================================================

    gdf_result["Ket"] = "Titik Api"

    gdf_result["Desa"] = (
        gdf_result["Desa"]
        .astype(str)
        .str.title()
    )

    gdf_result["Blok"] = (
        gdf_result["Blok"]
        .astype(str)
    )

    gdf_result["Blok"] = gdf_result["Blok"].apply(

        lambda x:

        f"Blok {x}"

        if (
            pd.notnull(x)
            and x != "None"
            and not x.startswith("Blok")
        )

        else x

    )

    # =====================================================
    # FINAL COLUMN
    # =====================================================

    final_cols = [

        "latitude",
        "longitude",

        "Tanggal",
        "Jam",

        "satellite",
        "instrument",

        "Owner",
        "Desa",

        "Penutup Lahan",

        "Blok",

        "Ket"

    ]

    gdf_result = gdf_result[final_cols]

    # =====================================================
    # TARGET SPREADSHEET
    # =====================================================

    spreadsheet_id_target = (
        "1o6MMYiH4CWORlONHBbG42PMMJWVJRBWTYdgPKtArb30"
    )

    sh_target = gc.open_by_key(
        spreadsheet_id_target
    )

    worksheet_target = sh_target.get_worksheet(0)

    # =====================================================
    # EXISTING DATA
    # =====================================================

    df_existing = pd.DataFrame(
        worksheet_target.get_all_records()
    )

    # =====================================================
    # REMOVE DUPLICATE
    # =====================================================

    if not df_existing.empty:

        for col in [
            "Tanggal",
            "Jam",
            "satellite"
        ]:

            if col not in df_existing.columns:

                df_existing[col] = ""

        df_existing["key"] = (

            df_existing["latitude"].astype(str) + "_" +

            df_existing["longitude"].astype(str) + "_" +

            df_existing["Tanggal"].astype(str) + "_" +

            df_existing["Jam"].astype(str) + "_" +

            df_existing["satellite"].astype(str)

        )

        gdf_result["key"] = (

            gdf_result["latitude"].astype(str) + "_" +

            gdf_result["longitude"].astype(str) + "_" +

            gdf_result["Tanggal"].astype(str) + "_" +

            gdf_result["Jam"].astype(str) + "_" +

            gdf_result["satellite"].astype(str)

        )

        before_count = len(gdf_result)

        gdf_result = gdf_result[
            ~gdf_result["key"].isin(
                df_existing["key"]
            )
        ]

        after_count = len(gdf_result)

        print(
            f"Removed duplicates: "
            f"{before_count - after_count}"
        )

        gdf_result = gdf_result.drop(
            columns=["key"]
        )

    # =====================================================
    # SORT DATETIME
    # =====================================================

    gdf_result["datetime_sort"] = pd.to_datetime(

        gdf_result["Tanggal"] + " " +

        gdf_result["Jam"]

    )

    gdf_result = gdf_result.sort_values(

        by="datetime_sort",

        ascending=True

    )

    gdf_result = gdf_result.drop(
        columns=["datetime_sort"]
    )

    # =====================================================
    # APPEND TO SHEET
    # =====================================================

    if not gdf_result.empty:

        start_row = len(df_existing) + 2

        set_with_dataframe(

            worksheet_target,

            gdf_result,

            row=start_row,

            include_index=False,

            include_column_header=False

        )

        print(
            f"Added {len(gdf_result)} new hotspots."
        )

    else:

        print("No new hotspot data.")

# =========================================================
# LOG LAST RUNTIME
# =========================================================

try:

    sh_target = gc.open_by_key(
        "1o6MMYiH4CWORlONHBbG42PMMJWVJRBWTYdgPKtArb30"
    )

    try:

        worksheet_log = sh_target.worksheet(
            "RunTime"
        )

    except gspread.exceptions.WorksheetNotFound:

        worksheet_log = sh_target.add_worksheet(

            title="RunTime",

            rows="10",

            cols="2"

        )

    WIB = timezone(
        timedelta(hours=7)
    )

    now = datetime.now(WIB).strftime(
        "%Y-%m-%d %H:%M:%S WIB"
    )

    df_log = pd.DataFrame({

        "Last_Run": [now]

    })

    worksheet_log.clear()

    set_with_dataframe(

        worksheet_log,

        df_log,

        include_index=False,

        include_column_header=True

    )

    print(
        f"Runtime logged: {now}"
    )

except Exception as e:

    print(
        f"Failed runtime logging: {e}"
    )
