import streamlit as st
import pandas as pd
from datetime import date
from db_connection import get_td_connection


st.set_page_config(page_title="VSL Cons Check", layout="wide")

st.title("VSL Cons / Flight Check")

with st.form("query_form"):
    vsl_cons_nbr = st.text_input(
        "VSL Cons Number",
        max_chars=12,
        help="Must be exactly 12 digits"
    )

    flt_zulu_dt = st.date_input(
        "Flight Date",
        value=date.today()
    )

    submitted = st.form_submit_button("Run Query")


def is_valid_vsl(vsl):
    return vsl.isdigit() and len(vsl) == 12


if submitted:
    if not is_valid_vsl(vsl_cons_nbr):
        st.error("VSL Cons Number must be exactly 12 digits.")
    else:
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
            ON f.pos_cons_nbr = dc.par_cons_nbr
        order by flag desc;
        """

        with st.spinner("Running query..."):
            try:
                with get_td_connection() as conn:
                    df = pd.read_sql(
                        query,
                        conn,
                        params=(
                            vsl_cons_nbr,
                            flt_zulu_dt.strftime("%Y-%m-%d")
                        )
                    )

                if df.empty:
                    st.warning("No results found.")
                else:
                    st.success(f"{len(df)} rows returned")
                    st.dataframe(df, use_container_width=True)

            except Exception as e:
                st.error(f"Database error: {e}")