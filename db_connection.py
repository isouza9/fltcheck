import os
import teradatasql

def get_td_connection():
    try:
        return teradatasql.connect(
            host=os.getenv("TD_HOST"),
            user=os.getenv("TD_USER"),
            password=os.getenv("TD_PASSWORD")
        )
    except Exception as e:
        raise RuntimeError(f"Failed to create Teradata connection: {e}")
