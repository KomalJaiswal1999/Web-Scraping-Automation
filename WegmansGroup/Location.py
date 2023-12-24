
from scrapinghelp import htmlhelper
import requests
import os
import json
import datetime
from multiprocessing.pool import ThreadPool as Pool
from cookie_generate import getcookie,user_context_gen
import DBConfig
from mainlink import mainlink
import main
from fuzzywuzzy import fuzz,process



def get_all_cat(_Url :str, _Zip :str, _Cookie, _User_context, _Authorization, _JobNumber, _Store_id, _Address, _Type):
    # =====================================================================
    directory = os.path.dirname(__file__) + "/Log/" + str(_JobNumber)
    try:
        os.mkdir(directory)
    except Exception as e:
        pass
    try:
        extra1=''
        if 'wegmans' in _Url or 'foodlion' in _Url or 'sprout' in _Url or 'pricechopper' in _Url or 'topsmarkets' in _Url:
            location_url = _Url + '/api/v3/user_init?with_configs=true'
        else:
            location_url = _Url + "/api/v2/user"
        post_data = '{"binary":"web-ecom","binary_version":"4.33.23","is_retina":false,"os_version":"Win32","pixel_density":"1.5","push_token":"","screen_height":720,"screen_width":1280}'


        #=====================for authorization=========

        auth_source = requests.post(location_url, data=post_data, proxies=proxies)
        print(auth_source)






        if 'foodlion' in _Url or 'shopthefastlane' in _Url or 'lowesfoods' in _Url:
            headers = {"cookie": _Cookie, "user-context": _User_context, "Content-Type": "application/json",
                       "authorization": _Authorization,
                       "User-Agent": "Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36"}
        else:
            headers = {"cookie": _Cookie, "user-context": _User_context, "Content-Type": "application/json",
                       "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"}

        if 'wegmans' in _Url or 'foodlion' in _Url or 'sprout' in _Url or 'pricechopper' in _Url or 'topsmarkets' in _Url:
            loc_source = requests.post(location_url, data=post_data, headers=headers, proxies=proxies)

        else:
            loc_source = requests.get(location_url, headers=headers, verify=False, proxies=proxies)
        print(loc_source)
        location_source = loc_source.json()

        try:
            if location_source['user']['context']['intent'] == None:
                if 'wegmans' in _Url or 'foodlion' in _Url or 'sprout' in _Url or 'pricechopper' in _Url or 'topsmarkets' in _Url:
                    loc_source = requests.post(location_url, data=post_data, headers=headers, proxies=proxies)

                else:
                    loc_source = requests.get(location_url, headers=headers, verify=False, proxies=proxies)
                print(loc_source)
                location_source = loc_source.json()


        except Exception as e:
            print(e, "Shopping mode not found")

        file_url1 = os.path.dirname(__file__) + "/Log/" + str(_JobNumber) + "/LocationLog.txt"
        with open(file_url1, 'w') as outfile1:
            json.dump(location_source, outfile1)
        # =======================================================================
        Location_addrss = os.path.dirname(__file__) + "/Log/" + str(_JobNumber) + "/LocationLog.txt"
        if os.path.exists(Location_addrss):
            with open(Location_addrss) as Location_addrss:
                try:
                    Location_addrss = json.load(Location_addrss)
                except Exception as e:
                    print(e)

            if 'wegmans' in _Url or 'foodlion' in _Url or 'sprout' in _Url or 'pricechopper' in _Url or 'topsmarkets' in _Url:
                try:
                    extra1 = Location_addrss['user']['context']['shopping_mode']
                    if 'KeyError' in extra1:
                        main.location_(_Zip,_Store_id,_Address,_Type,_Url, _Authorization, _JobNumber)

                except Exception as e:
                    print(e)


                extra2 = Location_addrss['user']['store']['id']
                address = Location_addrss['user']['store']['address']['address1']
                postal_code = Location_addrss['user']['store']['address']['postal_code']
                extra3 = str(address) + ", " + str(postal_code)
            else:
                try:
                    extra1 = Location_addrss['context']['shopping_mode']
                    if 'KeyError' in extra1:
                        main.location_(_Zip,_Store_id,_Address,_Type,_Url, _Authorization, _JobNumber)

                except Exception as e:
                    print(e)

                extra2 = Location_addrss['context']['storeNumber']
                address = Location_addrss['store']['address']['address1']
                postal_code = Location_addrss['store']['address']['postal_code']
                extra3 = str(address) + ", " + str(postal_code)

        if postal_code == _Zip and address == _Address:
            print('Address is fine')
        else:
            print("Website Address: ", extra3)
            print("Excel Address: ", _Address, " ", _Zip)
            # mainlink.question(postal_code)

            ratio = f"Similarity score: {fuzz.ratio(extra3, _Address)}"
            print(ratio)

            if ratio <=str(70):
                mainlink.question(postal_code)




        return extra1
    except Exception as e:
        print(e)
        return ""



