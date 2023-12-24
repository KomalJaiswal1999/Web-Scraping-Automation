from scrapinghelp import htmlhelper
import requests
import os
import json
import datetime
from multiprocessing.pool import ThreadPool as Pool
from cookie_generate import getcookie,user_context_gen
import DBConfig
parent_id_list = []
url_list = []
mainlink_list = []
_Count = 0

class mainlink:
    asa = '1'
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
    def mainlink(_Url: str, _Zip: str, _JobNumber: str, _Cookie: str,_User_context:str, _Authorization: str, _Store_id: str, _Address: str,_Type):
        global mainlink_list
        mainlink_list = []
        print("Mainlink Started : "+str(datetime.datetime.now()))
        pool_size = 50
        pool = Pool(pool_size)
        pool.apply_async(mainlink.get_all_cat(_Url, _Zip, _Cookie, _User_context, _Authorization,_JobNumber, _Store_id, _Address,_Type))
        pool.close()
        pool.join()
        print("Mainlink End : " + str(datetime.datetime.now()))

    def get_all_cat(_Url:str, _Zip :str, _Cookie, _User_context, _Authorization, _JobNumber, _Store_id, _Address,_Type):
        global mainlink_list
        mainlink_list = []
        url_list=[]
        category_url = _Url + "/api/v2/categories"
        if 'wegmans' in _Url:
            category_url = _Url + "/api/v2/categories/store/" + str(_Store_id)
        if 'foodlion' in _Url or 'shopthefastlane' in _Url or 'lowesfoods' in _Url:
            headers = {"cookie": _Cookie, "user-context": _User_context, "authorization": _Authorization}
        else:
            headers = {"cookie": _Cookie, "user-context": _User_context}

        if 'pricechopper' in _Url:
            content = requests.get(category_url, headers=headers, verify=False, proxies=proxies)
        else:
            if 'wegmans' in _Url:
                headers = {"cookie": _Cookie}
                content = requests.get(category_url, headers=headers, verify=False, proxies=proxies)
            else:
                content = requests.get(category_url, headers=headers, verify=False, proxies=proxies)
        htmlhelper.logtxt(content.text, "main_source", _JobNumber)
        source = content.json()
        items = source['items']
        global _Count
        _Count=0
        for x in range(len(items)):
            name = items[x]['name'].replace(',', '')
            link = _Url + "/shop" + items[x]['href']
            id = items[x]['id']
            parentid = items[x]['parent_id']
            parent_id_list.append(parentid)
            list = [name, link, id, parentid]
            url_list.append(list)

        for x in range(len(url_list)):
            id = url_list[x][2]
            if id not in parent_id_list:
                try:
                    Cat_name = url_list[x][0]
                    Url = url_list[x][1]
                    id = url_list[x][2]
                    parent_id = url_list[x][3]
                    formatting = _Url + "/api/v2/store_products?category_id=" + str(
                        id) + "&limit=100&offset=0&sort=popular"
                    _Count = _Count + 1
                    mainlink_list.append(
                        htmlhelper.mainlinksinsert(_Count, '_Date', _Zip, link, Cat_name, id, parentid, formatting, '',
                                                   "Valid", "", "", "", ""))
                except Exception as e:
                    print(e)
            else:
                pass
        if 'wegmans' in _Url:
            lst = ['Mozzarella & Burrata', 'https://shop.wegmans.com/shop/categories/539', '539', '1',
                   "https://shop.wegmans.com/api/v2/store_products?category_id=539&limit=100&offset=0&sort=popular"]
            Cat_name = 'Mozzarella & Burrata'
            formatting = "https://shop.wegmans.com/api/v2/store_products?category_id=539&limit=100&offset=0&sort=popular"
            link = 'https://shop.wegmans.com/shop/categories/539'
            id = '539'
            parentid = '1'
            _Count = _Count + 1
            mainlink_list.append(
                htmlhelper.mainlinksinsert(_Count, '_Date', _Zip, link, Cat_name, id, parentid, formatting, '', "Valid",
                                           "", "", "", ""))
        if 'tops' in _Url:
            ProducturlJsonsource = ""
            _Count = _Count + 1
            formatting = "https://shop.topsmarkets.com/product/39228/tops-pure-cane-granulated-sugar"
            mainlink_list.append(
                htmlhelper.mainlinksinsert(_Count, '_Date', _Zip, link, "Tops Pure Cane Granulated Sugar", "id", "pid",
                                           formatting, '', "Valid",
                                           "", "", "", ""))

        filename = "mainlink_" + _JobNumber
        directory = os.path.dirname(__file__) + "/Log/" + str(_JobNumber) + "/"
        try:
            if os.path.exists(directory):
                if os.path.exists(directory + filename + ".txt"):
                    os.remove(directory + filename + ".txt")
            else:
                os.mkdir(directory)
        except Exception as e:
            print('Error: deleting. ' + directory + e)

        with open(directory + filename + ".txt", 'w') as outfile:
            json.dump(mainlink_list, outfile)

        print('Total mainlink :-  ', _Count)

        # category_url = _Url + "/api/v2/categories"
        # if 'wegmans' in _Url:
        #     category_url = _Url + "/api/v2/categories/store/" + str(_Store_id)
        # if 'foodlion' in _Url or 'shopthefastlane' in _Url or 'lowesfoods' in _Url:
        #     headers = {"cookie": _Cookie, "user-context": _User_context, "authorization": _Authorization}
        # else:
        #     headers = {"cookie": _Cookie, "user-context": _User_context}
        #
        # if 'pricechopper' in _Url:
        #     content = requests.get(category_url, headers=headers, verify=False,proxies=proxies)
        # else:
        #     if 'wegmans' in _Url:
        #         headers = {"cookie": _Cookie}
        #         content = requests.get(category_url, headers=headers, verify=False,proxies=proxies)
        #     else:
        #         content = requests.get(category_url, headers=headers, verify=False,proxies=proxies)
        # htmlhelper.logtxt(content.text, "main_source", _JobNumber)
        # source = content.json()
        # items = source['items']
        # global _Count
        # for x in range(len(items)):
        #     name = items[x]['name'].replace(',', '')
        #     link = _Url + "/shop" + items[x]['href']
        #     id = items[x]['id']
        #     parentid = items[x]['parent_id']
        #     parent_id_list.append(parentid)
        #     list = [name, link, id, parentid]
        #     url_list.append(list)
        #
        # for x in range(len(url_list)):
        #     id = url_list[x][2]
        #     if id not in parent_id_list:
        #         try:
        #             Cat_name = url_list[x][0]
        #             Url = url_list[x][1]
        #             id = url_list[x][2]
        #             parent_id = url_list[x][3]
        #             formatting = _Url + "/api/v2/store_products?category_id=" + str(id) + "&limit=100&offset=0&sort=popular"
        #             _Count = _Count + 1
        #             mainlink_list.append(htmlhelper.mainlinksinsert(_Count, '_Date', _Zip, link, Cat_name, id, parentid, formatting, '', "Valid", "", "", "", ""))
        #         except Exception as e:
        #             print(e)
        #     else:
        #         pass
        # if 'wegmans' in _Url:
        #
        #     lst = ['Mozzarella & Burrata', 'https://shop.wegmans.com/shop/categories/539', '539', '1',
        #            "https://shop.wegmans.com/api/v2/store_products?category_id=539&limit=100&offset=0&sort=popular"]
        #     Cat_name='Mozzarella & Burrata'
        #     formatting="https://shop.wegmans.com/api/v2/store_products?category_id=539&limit=100&offset=0&sort=popular"
        #     link='https://shop.wegmans.com/shop/categories/539'
        #     id='539'
        #     parentid='1'
        #     _Count = _Count + 1
        #     mainlink_list.append(
        #         htmlhelper.mainlinksinsert(_Count, '_Date', _Zip, link, Cat_name, id, parentid, formatting, '', "Valid",
        #                                    "", "", "", ""))
        # if 'tops' in _Url:
        #     ProducturlJsonsource = ""
        #     _Count = _Count + 1
        #     formatting = "https://shop.topsmarkets.com/product/39228/tops-pure-cane-granulated-sugar"
        #     mainlink_list.append(htmlhelper.mainlinksinsert(_Count, '_Date', _Zip, link, "Tops Pure Cane Granulated Sugar", "id", "pid", formatting, '', "Valid",
        #                                "", "", "", ""))
        #
        #
        # filename = "mainlink_" + _JobNumber
        # directory = os.path.dirname(__file__) + "/Log/" + str(_JobNumber) + "/"
        # try:
        #     if os.path.exists(directory):
        #         if os.path.exists(directory + filename + ".txt"):
        #             os.remove(directory + filename + ".txt")
        #     else:
        #         os.mkdir(directory)
        # except Exception as e:
        #     print('Error: deleting. ' + directory + e)
        #
        # with open(directory + filename + ".txt", 'w') as outfile:
        #     json.dump(mainlink_list, outfile)
        #
        # print('Total mainlink :-  ', _Count)
