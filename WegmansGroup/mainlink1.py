import requests
import json
from scrapinghelp import htmlhelper
import os




mainlink_list = []
child_id = []
par_id = []
prod_ids = []
categoryname2 = ''

class MainLink:
    def mainlink(_Url, _Zip, _JobNumber,_Store_id):
        global count,categoryname2
        count = 0

        url = _Url + "/api/v2/categories/store/" + str(_Store_id)
        # url = 'https://shop.thefreshmarket.com/api/v2/categories/store/196'
        r = ''
        while r == '' or r.status_code != 200:
            try:
                r = requests.get(url, proxies = proxies,timeout=20)
                print(r,r.elapsed.total_seconds())
            except Exception as e:
                print(e)
        formatted_content = htmlhelper.returnformatedhtml(r.text)
        main_category = json.loads(formatted_content)
        par_id.clear()
        prod_ids.clear()
        child_id.clear()
        for data in main_category['items']:
            l1_name = data['name']
            par_code = str(data['parent_id']).replace('[', '').replace(']', '').replace("'", "").strip()
            prod_code = data['id']

            # if 'shop.sprouts' in url:
            #     if prod_code == '69' or par_code=='68':
            #         pass
            #     elif prod_code == '209' or par_code=='208':
            #         pass
            #     elif prod_code == '381' or par_code=='248':
            #         pass
            #     elif prod_code == '191' or par_code=='181':
            #         pass
            #     elif prod_code == '544' or par_code=='542':
            #         pass
            #     elif prod_code == '551' or par_code=='48':
            #         pass
            # elif prod_code=="":
            #     pass
            # else:
            l = [l1_name, prod_code, par_code]
            par_id.append(par_code)
            prod_ids.append(l)
        for x in prod_ids:
            if x[1] not in par_id:
                child_id.append(x)
        for x in child_id:
            categorycode = x[1]
            categoryname = ""
            category = cat(categorycode, prod_ids, categoryname)
            categoryname2=''
            category= category.strip('>')
            category=category.split('>')
            category=category[::-1]
            category = ">".join(category)
            print(category)
            mainlink_list.append(
                htmlhelper.mainlinksinsert(str(count), 'date', 'zip', "", category, categorycode, "", "", "",

                                           "Valid",
                                           "", "", "", ""))
            count += 1



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

        fileout = open(directory + filename + ".txt", 'w')

        json.dump(mainlink_list, fileout)
        fileout.close()
        mainlink_list.clear()

        print("Total mainlink: " + str(count))

def cat(prodcode, fulllist, categoryname):
    global categoryname2
    parentcode = prodcode
    # print(categoryname)
    if parentcode == "" or parentcode != 'None':
        while parentcode == "" or parentcode != 'None':
            for y in fulllist:
                if prodcode == y[1]:
                    categoryname2 = categoryname2 + ">" + y[0]
                    parentcode = y[2]
                    break
            cat(parentcode, fulllist, categoryname2)
            return categoryname2
    else:
        pass

# s = MainLink
# s.mainlink(_Url, _Zip, _JobNumber,_Store_id)