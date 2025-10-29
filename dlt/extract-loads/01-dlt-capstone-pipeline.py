import os
import dlt, pandas as pd

# barmm
@dlt.resource(name="barmm_basilan")
def barmm_basilan():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Basilan - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="barmm_interim")
def barmm_interim():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Interim Province - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="barmm_lanaods")
def barmm_lanaods():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Lanao Del Sur - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="barmm_maguindanao")
def barmm_maguindanao():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Maguindanao - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="barmm_sulu")
def barmm_sulu():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Sulu - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="barmm_tawi")
def barmm_tawi():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Tawi Tawi - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

# car
@dlt.resource(name="car_abra")
def car_abra():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Abra - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="car_apayao")
def car_apayao():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Apayao - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="car_baguio")
def car_baguio():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Baguio City - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="car_benguet")
def car_benguet():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Benguet - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="car_ifugao")
def car_ifugao():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Ifugao - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="car_kalinga")
def car_kalinga():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Kalinga - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="car_mountain")
def car_mountain():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Mountain Province - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

# xiii - caraga
@dlt.resource(name="xiii_agusan_dn")
def xiii_agusan_dn():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Agusan Del Norte - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="xiii_agusan_ds")
def xiii_agusan_ds():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Agusan Del Sur - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="xiii_butuan")
def xiii_butuan():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Butuan City - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="xiii_dinagat")
def xiii_dinagat():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Dinagat Islands - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="xiii_surigao_dn")
def xiii_surigao_dn():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Surigao Del Norte - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="xiii_surigao_ds")
def xiii_surigao_ds():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Surigao Del Sur - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

# ncr
@dlt.resource(name="ncr_caloocan")
def ncr_caloocan():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Caloocan - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="ncr_laspinas")
def ncr_laspinas():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Las Piñas - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="ncr_makati")
def ncr_makati():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Makati - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="ncr_malabon")
def ncr_malabon():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Malabon - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="ncr_mandaluyong")
def ncr_mandaluyong():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Mandaluyong - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="ncr_manila")
def ncr_manila():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Manila - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="ncr_marikina")
def ncr_marikina():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Marikina - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="ncr_muntinlupa")
def ncr_muntinlupa():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Muntinlupa - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="ncr_navotas")
def ncr_navotas():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Navotas - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="ncr_paranaque")
def ncr_paranaque():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Parañaque - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="ncr_pasay")
def ncr_pasay():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Pasay - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="ncr_pasig")
def ncr_pasig():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Pasig - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="ncr_pateros")
def ncr_pateros():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Pateros - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="ncr_quezon")
def ncr_quezon():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Quezon City - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="ncr_sanjuan")
def ncr_sanjuan():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 San Juan - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="ncr_taguig")
def ncr_taguig():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Taguig - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="ncr_valenzuela")
def ncr_valenzuela():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Valenzuela - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

# region i
@dlt.resource(name="i_ilocos_n")
def i_ilocos_n():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Ilocos Norte - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="i_ilocos_s")
def i_ilocos_s():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Ilocos Sur - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="i_launion")
def i_launion():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 La Union - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="i_pangasinan")
def i_pangasinan():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Pangasinan MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

# region ii
@dlt.resource(name="ii_batanes")
def ii_batanes():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Batanes - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="ii_cagayan")
def ii_cagayan():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Cagayan - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="ii_isabela")
def ii_isabela():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Isabela - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="ii_nueva_v")
def ii_nueva_v():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Nueva Vizcaya - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="ii_quirino")
def ii_quirino():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Quirino - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

# region iii
@dlt.resource(name="iii_angeles")
def iII_angeles():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Angeles City - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="iii_aurora")
def iii_aurora():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Aurora - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="iii_bataan")
def iii_bataan():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Bataan - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="iii_bulacan")
def iii_bulacan():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Bulacan - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="iii_nueva_e")
def iii_nueva_e():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Nueva Ecija - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="iii_olongapo")
def iii_olongapo():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Ologapo City - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="iii_pampanga")
def iii_pampanga():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Pampanga - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="iii_tarlac")
def iii_tarlac():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Tarlac - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="iii_zambales")
def iii_zambales():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Zambales - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

#region iv-a
@dlt.resource(name="iva_batangas")
def iva_batangas():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Batangas - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="iva_cavite")
def iva_cavite():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Cavite - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="iva_laguna")
def iva_laguna():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Laguna - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="iva_lucena")
def iva_lucena():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Lucena - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="iva_quezon")
def iva_quezon():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Quezon - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

@dlt.resource(name="iva_rizal")
def iva_rizal():
    ROOT_DIR = os.path.dirname(__file__)
    STAGING_DIR = os.path.join(ROOT_DIR, "staging", "capstone")
    FILE_PATH = os.path.join(STAGING_DIR, "CPH PUF 2020 Rizal - MEMBERS.CSV")
    yield pd.read_csv(FILE_PATH).astype(str)

def run():
    p = dlt.pipeline(
        pipeline_name="01-dlt-capstone-pipeline",
        destination="clickhouse",
        dataset_name="psa_census_2020",
    )
    print("Fetching and loading...")

    # barmm
    info1 = p.run(barmm_basilan())
    print("records loaded:", info1)
    info1 = p.run(barmm_interim())
    print("records loaded:", info1)
    info1 = p.run(barmm_lanaods())
    print("records loaded:", info1)
    info1 = p.run(barmm_maguindanao())
    print("records loaded:", info1)
    info1 = p.run(barmm_sulu())
    print("records loaded:", info1)
    info1 = p.run(barmm_tawi())
    print("records loaded:", info1)

    # car
    info1 = p.run(car_abra())
    print("records loaded:", info1)
    info1 = p.run(car_apayao())
    print("records loaded:", info1)
    info1 = p.run(car_baguio())
    print("records loaded:", info1)
    info1 = p.run(car_benguet())
    print("records loaded:", info1)
    info1 = p.run(car_ifugao())
    print("records loaded:", info1)
    info1 = p.run(car_kalinga())
    print("records loaded:", info1)
    info1 = p.run(car_mountain())
    print("records loaded:", info1)

    # xiii
    info1 = p.run(xiii_agusan_dn())
    print("records loaded:", info1)
    info1 = p.run(xiii_agusan_ds())
    print("records loaded:", info1)
    info1 = p.run(xiii_butuan())
    print("records loaded:", info1)
    info1 = p.run(xiii_dinagat())
    print("records loaded:", info1)
    info1 = p.run(xiii_surigao_dn())
    print("records loaded:", info1)
    info1 = p.run(xiii_surigao_ds())
    print("records loaded:", info1)

    # ncr
    info1 = p.run(ncr_caloocan())
    print("records loaded:", info1)
    info1 = p.run(ncr_laspinas())
    print("records loaded:", info1)
    info1 = p.run(ncr_makati())
    print("records loaded:", info1)
    info1 = p.run(ncr_malabon())
    print("records loaded:", info1)
    info1 = p.run(ncr_mandaluyong())
    print("records loaded:", info1)
    info1 = p.run(ncr_manila())
    print("records loaded:", info1)
    info1 = p.run(ncr_marikina())
    print("records loaded:", info1)
    info1 = p.run(ncr_muntinlupa())
    print("records loaded:", info1)
    info1 = p.run(ncr_navotas())
    print("records loaded:", info1)
    info1 = p.run(ncr_paranaque())
    print("records loaded:", info1)
    info1 = p.run(ncr_pasay())
    print("records loaded:", info1)
    info1 = p.run(ncr_pasig())
    print("records loaded:", info1)
    info1 = p.run(ncr_pateros())
    print("records loaded:", info1)
    info1 = p.run(ncr_quezon())
    print("records loaded:", info1)
    info1 = p.run(ncr_sanjuan())
    print("records loaded:", info1)
    info1 = p.run(ncr_taguig())
    print("records loaded:", info1)
    info1 = p.run(ncr_valenzuela())
    print("records loaded:", info1)

# region i
    info1 = p.run(i_ilocos_n())
    print("records loaded:", info1)
    info1 = p.run(i_ilocos_s())
    print("records loaded:", info1)
    info1 = p.run(i_launion())
    print("records loaded:", info1)
    info1 = p.run(i_pangasinan())
    print("records loaded:", info1)

# region ii
    info1 = p.run(ii_batanes())
    print("records loaded:", info1)
    info1 = p.run(ii_cagayan())
    print("records loaded:", info1)
    info1 = p.run(ii_isabela())
    print("records loaded:", info1)
    info1 = p.run(ii_nueva_v())
    print("records loaded:", info1)
    info1 = p.run(ii_quirino())
    print("records loaded:", info1)

# region iii
    info1 = p.run(ii_batanes())
    print("records loaded:", info1)
    info1 = p.run(ii_cagayan())
    print("records loaded:", info1)
    info1 = p.run(ii_isabela())
    print("records loaded:", info1)
    info1 = p.run(ii_nueva_v())
    print("records loaded:", info1)
    info1 = p.run(ii_quirino())
    print("records loaded:", info1)

# region iv-a
    info1 = p.run(iva_batangas())
    print("records loaded:", info1)
    info1 = p.run(iva_cavite())
    print("records loaded:", info1)
    info1 = p.run(iva_laguna())
    print("records loaded:", info1)
    info1 = p.run(iva_lucena())
    print("records loaded:", info1)
    info1 = p.run(iva_quezon())
    print("records loaded:", info1)
    info1 = p.run(iva_rizal())
    print("records loaded:", info1)



if __name__ == "__main__":
    run()

    # to run
    # docker compose --profile jobs run --rm \  
    # dlt python extract-loads/01-dlt-capstone-pipeline.py
