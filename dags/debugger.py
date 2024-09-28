from src.soccer_statics.extract_data import Request_Data
import sys

def extract_data():
    try:
        pf = Request_Data()
        global data_extract
        data_extract = pf.request_return()
        print("****************** Success process update basics datamart finance")
    except:
        print(
            "****************** Unexpected error update basics datamart finance: ",
            sys.exc_info(),
        )
        raise

def print_info():
    print(data_extract)

extract_data()
print_info()