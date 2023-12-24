from scrapinghelp import htmlhelper
import requests
import os
import json
from multiprocessing.pool import ThreadPool as Pool
from datetime import datetime
from pytz import timezone
import DBConfig

class data:
    def data(_Url: str, _Cookie: str,_Authorization: str, _user_context: str, _JobNumber: str):
        print("Data Started")
        filename = os.path.dirname(__file__) + "/Log/" + str(_JobNumber) + "/mainlink_" + str(_JobNumber)+".txt"
        infile = open(filename, 'r')
        mainlink_list = json.load(infile)
        infile.close()
        print("Mainlink Loaded to a list")
        pool_size = 100
        pool = Pool(pool_size)
        for link in mainlink_list:
            pool.apply_async(data.getdatafromweb, (link, _Url, _Cookie, _Authorization, _user_context, _JobNumber, mainlink_list))

        pool.close()
        pool.join()
        print("Data End")

    def getdatafromweb(link, _Url, _Cookie, _Authorization, _user_context, _JobNumber, mainlink_list):
        id = link["id"]
        category = link["cat1"]
        cat_id = link["cat2"]
        mlp_id = link["cat4"]
        if(cat_id != ''):
            totalcount = ""
            noofpage = 1
            startpage = 0
            i = 1
            while i <= noofpage:
                # ProducturlJson = "https://shop.wegmans.com/api/v2/collections/"+str(cat_id)+"?ads_enabled=true&ads_offset=0&ads_pagination_improvements=false&collection_id="+str(cat_id)+"&key=category_id&limit=60&mlp_id="+str(mlp_id)+"&mlp_slug=Stock-for-Success-Essentials&offset=0&sort=popular"
                ProducturlJson = mlp_id
                ProducturlJson = _Url + "/api/v2/store_products?ads_enabled=true&ads_offset=0&ads_pagination_improvements=false&category_id="+str(cat_id)+"&category_ids="+str(cat_id)+"&limit=100&offset="+str(startpage)+"&page="+str(noofpage)+"&sort=popular"
                if 'tops' in _Url:
                   if category == "Tops Pure Cane Granulated Sugar":
                       ProducturlJson = "https://shop.topsmarkets.com/api/v2/store_products?ads_enabled=true&ads_offset=0&ads_pagination_improvements=false&allow_autocorrect=true&limit=60&offset=0&search_is_autocomplete=false&search_provider=ic&search_term=Tops+Pure+Cane+Granulated+Sugar&secondary_results=true&sort=rank&unified_search_shadow_test_enabled=false"
                if 'foodlion' in _Url or 'shopthefastlane' in _Url or 'lowesfoods' in _Url:
                    headers = {"cookie": _Cookie, "user-context": _user_context, "authorization": _Authorization}
                else:
                    headers = {"cookie": _Cookie, "user-context": _user_context}

                ProducturlJsonsource = ""
                while ProducturlJsonsource == "" or (ProducturlJsonsource.status_code != 200 and ProducturlJsonsource.status_code != 206):
                    try:
                        ProducturlJsonsource = requests.get(ProducturlJson, headers=headers, verify=False,proxies=proxies)
                        if ProducturlJsonsource.status_code != 200:
                            print(str(ProducturlJsonsource)+" : "+ProducturlJson)
                    except:
                        ProducturlJsonsource = ""
                ProducturlJsonsource = ProducturlJsonsource.json()
                totalcount = ProducturlJsonsource["item_count"]
                if i == 1:
                    noofpage = htmlhelper.returnnumberofpages(int(totalcount), 100)

                print("Scrapping: id-" + str(id) + "/" + str(len(mainlink_list)) + " totalcount-" + str(totalcount) + " pages-" + str(i) + "/" + str(noofpage) + " - Time-" + str(datetime.now()))
                outfilename = os.path.dirname(__file__) + "/Log/" + str(_JobNumber) + "/Data_" + str(id) + "_" + str(i) + ".txt"
                fileout = open(outfilename, 'w')
                DATE = "TIMESTAMP:["+datetime.now(timezone('US/Eastern')).strftime("%m/%d/%Y %I:%M:%S %p")+"]"
                json.dump((DATE,ProducturlJsonsource), fileout)
                fileout.close()
                # with open(outfilename, 'w') as outfile:
                #     json.dump(DATE, outfile)
                startpage = startpage + 100
                i = i + 1
