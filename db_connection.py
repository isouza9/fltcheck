import os
import teradatasql
from dotenv import load_dotenv

# Load variables from .env file into the environment
load_dotenv()


def get_td_connection():
    """
    Returns a Teradata connection using values from a .env file.

    Required variables in .env:
      - TD_HOST
      - TD_USER
      - TD_PASSWORD
    """
    try:
        return teradatasql.connect(
            host=os.environ["TD_HOST"],
            user=os.environ["TD_USER"],
            password=os.environ["TD_PASSWORD"]
        )

    except KeyError as e:
        raise RuntimeError(
            f"Missing required environment variable: {e.args[0]}"
        )

    except Exception as err:
        raise RuntimeError(
            f"Failed to create Teradata connection: {err}"
        )