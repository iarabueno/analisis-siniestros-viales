"""
ETL: siniestros viales de CABA (2019-2025) -> esquema estrella.

Lee el CSV de hechos publicado en BA Data, limpia las columnas sucias y
genera una tabla de hechos + dimensiones listas para cargar en cualquier
herramienta OLAP (en el proyecto original se usó Google Looker Studio).

Uso:
    python3 limpieza_siniestros.py [ruta_csv_entrada] [carpeta_salida]

    Por defecto lee "siniestros_viales_hechos.csv" y escribe en "salida/".

Requiere: pandas
"""

import re
import sys
from pathlib import Path

import pandas as pd

ENTRADA_DEFAULT = "siniestros_viales_hechos.csv"
SALIDA_DEFAULT = "salida"

SIN_DATO = "SIN DATO"
VALORES_NULOS = {"", "SD", "S/D", "NAN", "#¡REF!", "#REF!", "SIN DATO"}

# Columnas de coordenadas del CSV original.
COL_LATITUD = "latitud_siniestro"
COL_LONGITUD = "longitud_siniestro"

DIAS_SEMANA = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"]
MESES = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio",
         "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]


# ---------------------------------------------------------------
# Funciones de limpieza
# ---------------------------------------------------------------
def normalizar_categoria(valor):
    """Uppercase, sin espacios de más, y todos los nulos/basura -> SIN DATO."""
    if pd.isna(valor):
        return SIN_DATO
    s = str(valor).strip().upper()
    return SIN_DATO if s in VALORES_NULOS else s


def normalizar_participantes(valor):
    """'moto-SD' -> 'MOTO-SIN DATO'; 'SD-SD' o vacío -> 'SIN DATO'.

    Normaliza cada lado del par por separado, porque el valor original
    combina víctima y contraparte en un solo string.
    """
    if pd.isna(valor):
        return SIN_DATO
    partes = [normalizar_categoria(p) for p in str(valor).split("-")]
    if all(p == SIN_DATO for p in partes):
        return SIN_DATO
    return "-".join(partes)


def normalizar_comuna(valor):
    """'Comuna 8' / '8' / ' comuna 08 ' -> 'Comuna 8'."""
    if pd.isna(valor):
        return SIN_DATO
    match = re.search(r"\d+", str(valor))
    if match and 1 <= int(match.group()) <= 15:
        return f"Comuna {int(match.group())}"
    return SIN_DATO


def derivar_turno(hora):
    if hora < 0:
        return SIN_DATO
    if hora <= 5:
        return "Madrugada"
    if hora <= 11:
        return "Mañana"
    if hora <= 18:
        return "Tarde"
    return "Noche"


def agregar_surrogate_key(df, nombre_id):
    df = df.reset_index(drop=True)
    df.insert(0, nombre_id, df.index + 1)
    return df


# ---------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------
def limpiar(df):
    reporte = {}

    # Fecha: fuente de verdad. Año/mes/día se derivan de acá, no de las
    # columnas sueltas del CSV (que tienen inconsistencias: p. ej. una fila
    # con fecha 2025-11-28 y dia_siniestro = 29).
    df["fecha"] = pd.to_datetime(df["fecha_siniestro"], errors="coerce")
    if "dia_siniestro" in df.columns:
        dia_orig = pd.to_numeric(df["dia_siniestro"], errors="coerce")
        reporte["filas con dia_siniestro inconsistente con la fecha"] = int(
            (df["fecha"].notna() & (dia_orig != df["fecha"].dt.day)).sum())
    reporte["filas descartadas por fecha inválida"] = int(df["fecha"].isna().sum())
    df = df[df["fecha"].notna()].copy()

    df["comuna_siniestro"] = df["comuna_siniestro"].apply(normalizar_comuna)
    for col in ["tipo_de_via_siniestro", "contraparte_siniestro",
                "modo_desplazamiento_victima", "gravedad_siniestro"]:
        df[col] = df[col].apply(normalizar_categoria)
    df["participantes_siniestro"] = df["participantes_siniestro"].apply(normalizar_participantes)
    df["direccion_normalizada_siniestro"] = (
        df["direccion_normalizada_siniestro"].fillna(SIN_DATO).astype(str).str.strip()
        .replace("", SIN_DATO))

    df["hora"] = pd.to_numeric(df["rango_horario"], errors="coerce").fillna(-1).astype(int)
    df.loc[~df["hora"].between(-1, 23), "hora"] = -1
    df["turno"] = df["hora"].apply(derivar_turno)

    # Víctimas: el total se recalcula como suma de leves + graves + mortales.
    # Decisión: el desglose por gravedad es el dato más granular, así que se
    # toma como fuente de verdad. Cubre dos casos distintos del dataset:
    #   - total informado como "SD" (sin dato) -> se completa con la suma;
    #   - total informado que no coincide con el desglose -> se corrige.
    cols_vic = ["numero_victimas_leve_siniestro", "numero_victimas_grave_siniestro",
                "numero_victimas_mortal_siniestro"]
    for col in cols_vic:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    total_informado = pd.to_numeric(df["numero_total_de_victimas"], errors="coerce")
    suma = df[cols_vic].sum(axis=1)
    reporte["filas con total de víctimas sin dato (completado con la suma)"] = int(total_informado.isna().sum())
    reporte["filas con total de víctimas inconsistente (corregido)"] = int(
        (total_informado.notna() & (total_informado != suma)).sum())
    df["numero_total_de_victimas"] = suma

    duplicados = df["id_siniestro"].duplicated().sum()
    reporte["filas duplicadas por id_siniestro eliminadas"] = int(duplicados)
    df = df.drop_duplicates("id_siniestro")

    return df, reporte


def construir_dim_tiempo(df):
    fechas = pd.Series(df["fecha"].drop_duplicates().sort_values().values, name="fecha")
    dim = pd.DataFrame({"fecha": fechas})
    dim["anio"] = dim["fecha"].dt.year
    dim["trimestre"] = dim["fecha"].dt.quarter
    dim["mes"] = dim["fecha"].dt.month
    dim["nombre_mes"] = dim["mes"].map(lambda m: MESES[m - 1])
    dim["dia"] = dim["fecha"].dt.day
    dim["dia_semana"] = dim["fecha"].dt.dayofweek.map(lambda d: DIAS_SEMANA[d])
    dim["es_fin_de_semana"] = dim["fecha"].dt.dayofweek >= 5
    dim["fecha"] = dim["fecha"].dt.strftime("%Y-%m-%d")
    return agregar_surrogate_key(dim, "id_tiempo")


def construir_dim_simple(df, col_origen, nombre):
    d = (df[[col_origen]].drop_duplicates().rename(columns={col_origen: nombre})
         .sort_values(nombre))
    return agregar_surrogate_key(d, f"id_{nombre}")


def construir_dim_comuna_geo(df):
    """Centroide aproximado (promedio de coordenadas) por comuna, para mapas."""
    if COL_LATITUD not in df.columns or COL_LONGITUD not in df.columns:
        print(f"  [aviso] No encontré las columnas '{COL_LATITUD}'/'{COL_LONGITUD}': "
              "no se genera dim_comuna_geo.csv")
        return None
    geo = df[["comuna_siniestro", COL_LATITUD, COL_LONGITUD]].copy()
    geo[COL_LATITUD] = pd.to_numeric(geo[COL_LATITUD].astype(str).str.replace(",", "."), errors="coerce")
    geo[COL_LONGITUD] = pd.to_numeric(geo[COL_LONGITUD].astype(str).str.replace(",", "."), errors="coerce")
    # Filtro grueso: descartar coordenadas fuera de CABA.
    geo = geo[geo[COL_LATITUD].between(-34.71, -34.52) & geo[COL_LONGITUD].between(-58.54, -58.33)]
    geo = geo[geo["comuna_siniestro"] != SIN_DATO]
    return (geo.groupby("comuna_siniestro")
            .agg(latitud_promedio=(COL_LATITUD, "mean"),
                 longitud_promedio=(COL_LONGITUD, "mean"),
                 cantidad_siniestros=(COL_LATITUD, "size"))
            .round(6).reset_index().rename(columns={"comuna_siniestro": "comuna"}))


def construir_modelo(df):
    df = df.copy()
    df["fecha_str"] = df["fecha"].dt.strftime("%Y-%m-%d")

    dims = {
        "dim_tiempo": construir_dim_tiempo(df),
        # Jerarquía de navegación comuna -> tipo de vía -> dirección.
        # Ojo: no es estricta (una avenida cruza varias comunas); la dirección
        # sí queda anidada porque la clave es la combinación de las tres.
        "dim_ubicacion": agregar_surrogate_key(
            df[["comuna_siniestro", "tipo_de_via_siniestro", "direccion_normalizada_siniestro"]]
            .drop_duplicates()
            .rename(columns={"comuna_siniestro": "comuna", "tipo_de_via_siniestro": "tipo_via",
                             "direccion_normalizada_siniestro": "direccion"})
            .sort_values(["comuna", "tipo_via", "direccion"]),
            "id_ubicacion"),
        "dim_franja_horaria": agregar_surrogate_key(
            df[["turno", "hora"]].drop_duplicates().sort_values("hora"), "id_franja_horaria"),
        "dim_gravedad": construir_dim_simple(df, "gravedad_siniestro", "gravedad"),
        "dim_modo_desplazamiento": construir_dim_simple(df, "modo_desplazamiento_victima", "modo_desplazamiento"),
        "dim_contraparte": construir_dim_simple(df, "contraparte_siniestro", "contraparte"),
        "dim_participantes": construir_dim_simple(df, "participantes_siniestro", "participantes"),
    }

    fact = (df
            .merge(dims["dim_tiempo"][["id_tiempo", "fecha"]], left_on="fecha_str", right_on="fecha",
                   suffixes=("", "_dim"))
            .merge(dims["dim_ubicacion"],
                   left_on=["comuna_siniestro", "tipo_de_via_siniestro", "direccion_normalizada_siniestro"],
                   right_on=["comuna", "tipo_via", "direccion"])
            .merge(dims["dim_franja_horaria"], on=["turno", "hora"])
            .merge(dims["dim_gravedad"], left_on="gravedad_siniestro", right_on="gravedad")
            .merge(dims["dim_modo_desplazamiento"], left_on="modo_desplazamiento_victima",
                   right_on="modo_desplazamiento")
            .merge(dims["dim_contraparte"], left_on="contraparte_siniestro", right_on="contraparte")
            .merge(dims["dim_participantes"], left_on="participantes_siniestro", right_on="participantes"))

    # Inner joins: si se perdió alguna fila, algo está mal en las claves.
    assert len(fact) == len(df), f"Se perdieron filas en los joins: {len(df)} -> {len(fact)}"

    fact_siniestro = fact[[
        "id_siniestro", "id_tiempo", "id_ubicacion", "id_franja_horaria", "id_gravedad",
        "id_modo_desplazamiento", "id_contraparte", "id_participantes",
        "numero_total_de_victimas", "numero_victimas_leve_siniestro",
        "numero_victimas_grave_siniestro", "numero_victimas_mortal_siniestro",
    ]].rename(columns={
        "numero_total_de_victimas": "total_victimas",
        "numero_victimas_leve_siniestro": "victimas_leves",
        "numero_victimas_grave_siniestro": "victimas_graves",
        "numero_victimas_mortal_siniestro": "victimas_mortales",
    })
    fact_siniestro.insert(8, "cantidad_siniestros", 1)

    tablas = {"fact_siniestro": fact_siniestro, **dims}
    geo = construir_dim_comuna_geo(df)
    if geo is not None:
        tablas["dim_comuna_geo"] = geo
    return tablas


def main():
    entrada = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(ENTRADA_DEFAULT)
    salida = Path(sys.argv[2]) if len(sys.argv) > 2 else Path(SALIDA_DEFAULT)
    salida.mkdir(parents=True, exist_ok=True)

    print(f"Leyendo {entrada} ...")
    crudo = pd.read_csv(entrada, sep=";", encoding="utf-8-sig", low_memory=False)
    print(f"  {len(crudo)} filas leídas")

    limpio, reporte = limpiar(crudo)
    print("\nReporte de calidad:")
    for k, v in reporte.items():
        print(f"  {k}: {v}")

    tablas = construir_modelo(limpio)
    print(f"\nArchivos generados en {salida}/:")
    for nombre, tabla in tablas.items():
        tabla.to_csv(salida / f"{nombre}.csv", index=False)
        print(f"  {nombre + '.csv':<30} {len(tabla):>7} filas")


if __name__ == "__main__":
    main()
