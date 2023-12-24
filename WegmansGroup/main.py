
import json
import xml.dom.minidom
import requests
import os
import datetime
from scrapinghelp import htmlhelper
from data import data
from mainlink import mainlink
from mainlink1 import MainLink
from extractdata import extractdata
from pandas.tests.io.excel.test_openpyxl import openpyxl
from cookie_generate import getcookie,user_context_gen
import Location
store_list = []



def _main(start, end):
    global dir_path
    global _zip
    global _job_number
    global _cookie
    global store_id

    store_file = "Store_Info.xlsx"
    wb_store = openpyxl.load_workbook(store_file)
    sheet_store = wb_store.active

    for row in sheet_store.iter_rows(values_only=True):
        row_list = [row[0], row[1], row[2], row[3], row[4], row[5],row[6],row[7], row[8], row[9],row[10]]
        if row_list[0] is None:
            break
        else:
            store_list.append(row_list)
    for x in range(start, end + 1):
        _Zip = str(store_list[x][0]).strip()
        _JobNumber = str(store_list[x][1]).strip()
        _Url = str(store_list[x][2]).strip()
        _Url = (_Url).replace('.com/','.com')
        _Store_id = str(store_list[x][8]).strip()
        _Address = str(store_list[x][9]).strip()
        _Type = str(store_list[x][10]).strip()
        _Type = _Type.lower()
        _Authorization = str(store_list[x][5]).strip()

        location_(_Zip,_Store_id,_Address,_Type,_Url, _Authorization, _JobNumber)






def location_(_Zip,_Store_id,_Address,_Type,_Url, _Authorization, _JobNumber):
        location=''

        while location=='':

            _Cookie = getcookie(_Zip,_Store_id,_Address,_Type,_Url)
            _User_context = user_context_gen(_Type,_Store_id,_Url)
            x=Location.get_all_cat(_Url, _Zip, _Cookie, _User_context, _Authorization, _JobNumber, _Store_id, _Address,_Type)
            if x != '':
                location="Shopping Mode is " + str(x)

        _SiteDomain = htmlhelper.returnvalue(_Url, "www.", ".com")
        _Date = datetime.date.today().strftime("%m/%d/%Y")

        MainLink.mainlink(_Url, _Zip, _JobNumber, _Store_id)  # -----parent-child case
        # mainlink.get_all_cat(_Url, _Zip, _Cookie, _User_context, _Authorization, _JobNumber, _Store_id, _Address,
        #                      _Type)

        # mainlink.mainlink(_Url, _Zip, _JobNumber, _Cookie, _User_context, _Authorization, _Store_id, _Address,_Type)---don't uncomment this
        data.data(_Url, _Cookie, _Authorization, _User_context, _JobNumber)
        extractdata.extractdata(_Zip, _Cookie, _User_context, _Url, _JobNumber, _Store_id, _Address)

def location(_Website, _Cookie, _User_context, _Authorization, _JobNumber, _Zip, _Address):
    _Url = _Website
    if "wegmans" in _Url:
       location_url = _Url + '/api/v2/user'
    else:
        location_url = _Url + '/api/v2/user/profile'

    if 'foodlion' in _Url or 'shopthefastlane' in _Url or 'lowesfoods' in _Url:
        headers = {"cookie": _Cookie, "user-context": _User_context, "authorization": _Authorization}
    else:
        headers = {"cookie": _Cookie, "user-context": _User_context}

    loc_source = requests.get(location_url, headers=headers, verify=False)
    location_source = loc_source.json()
    directory = os.path.dirname(__file__) + "/SubsetOrWeisLog/" + str(_JobNumber)
    try:
        os.mkdir(directory)
    except Exception as e:
        pass
    file_path = os.path.dirname(__file__) + "/SubsetOrWeisLog/" + str(_JobNumber) + "/LocationLog.txt"
    with open(file_path, 'w') as outfile1:
        json.dump(location_source, outfile1)
    # =======================================================================
    postal_code = ""
    address = ""
    extra3 = ""
    Location_addrss = os.path.dirname(__file__) + "/SubsetOrWeisLog/" + str(_JobNumber) + "/LocationLog.txt"
    if os.path.exists(Location_addrss):
        with open(Location_addrss) as Location_addrss:
            try:
                Location_addrss = json.load(Location_addrss)
            except Exception as e:
                print(e)
        # extra1 = Location_addrss['context']['shopping_mode']
        extra1='pickup'
        extra2 = Location_addrss['context']['storeNumber']
        address = Location_addrss['store']['address']['address1']
        postal_code = Location_addrss['store']['address']['postal_code']
        extra3 = str(address) + ", " + str(postal_code)

    if postal_code == _Zip and address == _Address:
        print('Address is fine')
    else:
        print("Website Address: ", extra3)
        print("Excel Address: ", _Address, " ", _Zip)
        question(postal_code)
    # ========================================================================


def question(postal_code):
    i = 0
    while i < 2:
        answer = input("Address is not matching are you want to continue...(Yes/No) ")
        if any(answer.lower() == f for f in ["yes", 'y', '1', 'ye']):
            print("Yes")
            break
        elif any(answer.lower() == f for f in ['no', 'n', '0']):
            print("No")
            exit()
        else:
            i += 1
            if i < 2:
                print('Please enter yes or no')
            else:
                print("Nothing done")
