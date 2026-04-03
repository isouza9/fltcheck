import re
from datetime import datetime
from db_connection import get_td_connection
from tabulate import tabulate



# -----------------------------
# Validation functions
# -----------------------------
def validate_vsl_cons_nbr(value: str) -> str:
    """
    vsl_cons_nbr must be exactly 12 digits
    """
    if not re.fullmatch(r"\d{12}", value):
        raise ValueError("vsl_cons_nbr must be a 12-digit numeric value.")
    return value


def validate_flt_zulu_dt(value: str) -> str:
    """
    flt_zulu_dt must be in YYYY-MM-DD format and a valid date
    """
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        raise ValueError("flt_zulu_dt must be a valid date in YYYY-MM-DD format.")
    return value


# -----------------------------
# Main execution
# -----------------------------
def run_query(vsl_cons_nbr: str, flt_zulu_dt: str):
    query = """
    WITH flt_summary AS (
        SELECT
            flt_nbr,
            actl_orig_cd,
            actl_dest_cd,
            flt_zulu_dt,
            vsl_cons_nbr,
            cntnr_nbr,
            pos_cons_nbr
        FROM SCAN_PROD_VIEW_DB.glad_flt_dt_leg_pos_sum
        WHERE vsl_cons_nbr = ?
          AND flt_zulu_dt = ?
    ),
    dcons_check AS (
        SELECT DISTINCT
            par_cons_nbr
        FROM SCAN_PROD_VIEW_DB.fx_package_event_history
        WHERE scan_type_cd = '14'
          AND pkg_trk_src_typ_cd = '{'
    )
    SELECT 
        f.*,
        CASE 
            WHEN dc.par_cons_nbr IS NOT NULL THEN 'DCONS'
            ELSE NULL
        END AS flag
    FROM flt_summary f
    LEFT JOIN dcons_check dc
        ON f.pos_cons_nbr = dc.par_cons_nbr;
    """

    try:
        with get_td_connection() as conn:
            cur = conn.cursor()
            cur.execute(query, (vsl_cons_nbr, flt_zulu_dt))

            columns = [col[0] for col in cur.description]
            rows = cur.fetchall()

            if not rows:
                print("No results found.")
                return

            for row in rows:
                print(tabulate(rows, headers=columns, tablefmt="grid"))

    except Exception as err:
        print(f"Database error: {err}")



# -----------------------------
# Entry point
# -----------------------------
if __name__ == "__main__":
    try:
        vsl_input = input("Enter vsl_cons_nbr (12 digits): ").strip()
        flt_input = input("Enter flt_zulu_dt (YYYY-MM-DD): ").strip()

        vsl_cons_nbr = validate_vsl_cons_nbr(vsl_input)
        flt_zulu_dt = validate_flt_zulu_dt(flt_input)

        run_query(vsl_cons_nbr, flt_zulu_dt)

    except ValueError as ve:
        print(tabulate(rows, headers=columns, tablefmt="grid"))
