import os
import json
import pandas as pd
import geopandas as gpd
import gspread
import requests
from gspread_dataframe import set_with_dataframe
from datetime import datetime

service_account_info = json.loads(os.environ["GCP_CREDENTIALS"])
gc = gspread.service_account_from_dict(service_account_info)

spreadsheet_id_source = "1s7jnrnlSpyGdKHuNYK1XasveO7u_ESTv1mkpxQXTHAI"
sh_source = gc.open_by_key(spreadsheet_id_source)
worksheet_source = sh_source.get_worksheet(0)
df = pd.DataFrame(worksheet_source.get_all_records())

if df.empty:
    print("Data is empty. No data to process.")
else:
    df.columns = df.columns.astype(str).str.strip().str.lower()
    df = df.rename(columns={"acq_date": "date"})
    selected_cols = ["latitude", "longitude", "date", "satellite", "instrument"]
    df = df[selected_cols]

    df["latitude"] = df["latitude"].astype(str).str.replace(",", ".").astype(float)
    df["longitude"] = df["longitude"].astype(str).str.replace(",", ".").astype(float)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    gdf_points = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df["longitude"], df["latitude"]),
        crs="EPSG:4326"
    )

    desa_path = "data/Desa.json"
    pemilik_path = "data/PemilikLahan.json"

    gdf_desa = gpd.read_file(desa_path).to_crs("EPSG:4326")
    gdf_pemilik = gpd.read_file(pemilik_path).to_crs("EPSG:4326")

    lulc_url = "https://drive.google.com/uc?export=download&id=1TdLkZuxUAbjLhY6WyJIkXwe3q49Uu5fg"
    lulc_path = "data/LULC.json"

    os.makedirs("data", exist_ok=True)
    r = requests.get(lulc_url)
    with open(lulc_path, "wb") as f:
        f.write(r.content)

    gdf_lulc = gpd.read_file(lulc_path).to_crs("EPSG:4326")

    gdf_join = gpd.sjoin(
        gdf_points, gdf_desa[["nama_kel", "geometry"]], predicate="within"
    ).drop(columns=["index_right"])

    gdf_join = gpd.sjoin(
        gdf_join, gdf_pemilik[["Owner", "geometry"]], predicate="within"
    ).drop(columns=["index_right"])

    gdf_join = gpd.sjoin(
        gdf_join, gdf_lulc[["LC23", "Blok", "geometry"]], predicate="within"
    ).drop(columns=["index_right"])

    gdf_result = gdf_join.rename(columns={
        "nama_kel": "village",
        "Owner": "owner",
        "LC23": "LC"
    })
    gdf_result["Ket"] = "Titik Api"

    final_cols = ["latitude", "longitude", "date", "satellite", "instrument",
                  "owner", "village", "LC", "Blok", "Ket"]
    gdf_result = gdf_result[final_cols]

    spreadsheet_id_target = "1QRsiwK-3vlEU8991xsFsFvWdmyeuMTvSnATxxWRZEfk"
    sh_target = gc.open_by_key(spreadsheet_id_target)
    worksheet_target = sh_target.get_worksheet(0)

    df_existing = pd.DataFrame(worksheet_target.get_all_records())

    if not df_existing.empty:
        df_existing["key"] = (
            df_existing["latitude"].astype(str) + "_" +
            df_existing["longitude"].astype(str) + "_" +
            df_existing["date"].astype(str)
        )
        gdf_result["key"] = (
            gdf_result["latitude"].astype(str) + "_" +
            gdf_result["longitude"].astype(str) + "_" +
            gdf_result["date"].astype(str)
        )
        gdf_result = gdf_result[~gdf_result["key"].isin(df_existing["key"])]
        gdf_result = gdf_result.drop(columns=["key"])

    if not gdf_result.empty:
        gdf_result["date"] = pd.to_datetime(gdf_result["date"], errors="coerce")
        gdf_result = gdf_result.sort_values(by="date", ascending=True)

        start_row = len(df_existing) + 2
        set_with_dataframe(
            worksheet_target,
            gdf_result,
            row=start_row,
            include_index=False,
            include_column_header=False
        )

try:
    sh_target = gc.open_by_key("1QRsiwK-3vlEU8991xsFsFvWdmyeuMTvSnATxxWRZEfk")

    try:
        worksheet_log = sh_target.worksheet("RunTime")
    except gspread.exceptions.WorksheetNotFound:
        worksheet_log = sh_target.add_worksheet(title="RunTime", rows="10", cols="2")

    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    df_log = pd.DataFrame({"Last_Run": [now]})

    worksheet_log.clear()
    set_with_dataframe(
        worksheet_log,
        df_log,
        include_index=False,
        include_column_header=True
    )

    print(f"Logged latest run time: {now}")
except Exception as e:
    print(f"Failed to log run time: {e}")
