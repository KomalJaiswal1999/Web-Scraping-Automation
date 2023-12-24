import re
import os
import DBConfig
import pyodbc


class htmlhelper:

    def returnformatedhtml(content: str):
        result = re.sub("\\s+", " ", content).replace(" \"", "\"").replace("\" >", "\">").replace(" >", ">").replace(
            "> <", "><").replace("\" />", "\"/>").replace(" =", "=").replace("= ", "=")
        return result

    def formatstring(content: str):
        return re.sub("<(.|\n)*?>", " ", content).strip()

    def returnnumberofpages(total: int, pagesize: int):
        num = 0
        result = 0
        try:
            if (total > pagesize):
                num = total / pagesize
                num2 = total % pagesize
                if (num2 > 0):
                    num = num + 1
            else:
                if (total > 0 and total <= pagesize):
                    num = 1
            result = num
        except:
            result = 0
        return int(result)

    def returnvalue(content: str, start: str, end: str):
        result = ""
        pattern = re.compile(re.escape(start) + "(.*?)" + re.escape(end))
        match = pattern.search(content)
        if match:
            result = match.groups()[0].strip()
        return result

    def returnvalueafter(content: str, startafter: str, start: str, end: str):
        result = ""
        pattern = re.compile(re.escape(startafter))
        match = pattern.search(content)
        if match:
            startafterloc = match.end()
            pattern = re.compile(re.escape(start) + "(.*?)" + re.escape(end))
            match = pattern.search(content, startafterloc)
            if match:
                result = match.groups()[0].strip()
        return result

    def returnvaluelastoccurance(content: str, start: str, end: str):
        result = ""
        pattern = re.compile(re.escape(start) + "(.*?)" + re.escape(end))
        match = pattern.findall(content)
        if match:
            result = match[-1].strip()
        return result

    def returnvalueafterlastoccurance(content: str, startafter: str, start: str, end: str):
        result = ""
        pattern = re.compile(re.escape(startafter))
        match = pattern.search(content)
        if match:
            startafterloc = match.end()
            pattern = re.compile(re.escape(start) + "(.*?)" + re.escape(end))
            match = pattern.findall(content, startafterloc)
            if match:
                result = match[-1].strip()
        return result

    def collecturl(content: str, prefix: str, start: str, end: str):
        result = []
        pattern = re.compile(re.escape(start) + "(.*?)" + re.escape(end))
        match = pattern.findall(content)
        if match:
            result = [prefix + sub.strip() for sub in match]
        return result

    def returnvaluebefore(content: str, start: str, end: str):
        result = ""
        pattern = re.compile("^.*?(?=" + start.replace("(", "\\(").replace(")", "\\)") + ")")
        match = pattern.search(content)
        if match:
            match = match.group().split(re.escape(end))
            if len(match) > 1:
                result = match[-1].strip()
        return result

    def logtxt(content: str, filename: str, zip: str):
        directory = os.path.dirname(__file__) + "/Log/" + zip + "/"
        if len(filename) > 190:
            filename = filename[0:190]
        filename = re.sub("[^a-zA-Z0-9@_.]+", "", filename.replace("/", "@"))
        try:
            if not os.path.exists(directory):
                os.makedirs(directory)
        except OSError:
            print('Error: Creating directory. ' + directory)
        f = open(directory + filename + ".txt", "a")
        content = content.encode('utf_8')
        f.writelines(str(content))
        f.close()

    def splitsourcebybracket(text: str):
        br1 = 0
        FirstBracket = "true"
        rr = ""
        result = []
        for i in range(0, (len(text) - 1)):
            if FirstBracket:
                rr = rr + text[i]
                if (text[i] == "{"):
                    FirstBracket = ""
                    result.append(str(rr))
                    rr = ""
            else:
                if ( text[i] == "{"):
                    rr = rr + text[i]
                    br1 = br1 + 1
                elif (text[i] == "}"):
                    br1 = br1 - 1
                    if (br1 == 0):
                        rr = rr + text[i]
                        br1 = 0
                        result.append(str(rr))
                        rr = ""
                    else:
                        rr = rr + text[i]
                elif (i == (len(text) - 2)):
                    rr = rr + text[i]
                    br1 = 0
                    result.append(str(rr))
                    rr = ""
                else:
                    rr = rr + text[i]
        return result

    def mainlinksinsert(id: str, date: str, zip: str, mainlink: str, cat1: str, cat2: str, cat3: str, cat4: str,
                        cat5: str, iscomplete: str, totalcount: str, extra1: str, extra2: str, extra3: str):
        return {"id": id, "date": date, "zip": zip, "mainlink": mainlink, "cat1": cat1, "cat2": cat2, "cat3": cat3,
                "cat4": cat4, "cat5": cat5, "iscomplete": iscomplete, "totalcount": totalcount, "extra1": extra1,
                "extra2": extra2, "extra3": extra3}

    def addheader(filepath: str):
        return "ID|SITE|ZIP|DATE|IMAGELINK|PRICE|PRICE_MULT|EPRICE|EINDICATOR|ALTPRICE|ALTPRICEMULT|EALTPRICE|ALTINDICATOR|PACK|UNIT|UOM|SIZE|PRODUCT_ID|UPC|ASIN|CATEGORY|DESCRIPTION|NOTE|RATING|VALID|PAGEIMAGE|AISLE|ORIG_UPC|JOB_NUMBER|SALE_ENDDATE|BRAND|EXTRA1|EXTRA2|EXTRA3|EXTRA4|EXTRA5|EXTRA6|EXTRA7|EXTRA8|EXTRA9|LINKID\n"

    @staticmethod
    def createpsv(id:str,site: str, zip: str, date: str, Imagelink: str, price: str, price_mult: str, Eprice: str,
                  Eindicator: str, altprice: str, Altpricemult: str, Ealtprice: str, Altindicator: str, pack: str,
                  unit: str, uom: str, size: str, product_id: str, upc: str, asin: str, category: str, description: str,
                  note: str, rating: str, valid: str, pageimage: str, aisle: str, orig_upc: str, job_number: str,
                  sale_enddate: str, brand: str, extra1: str, extra2: str, extra3: str, extra4: str, extra5: str,
                  extra6: str, extra7: str, extra8: str, extra9: str, LinkID: str):
        description = description.replace("|", "-").replace("'", "''")
        note = note.replace("|", "-").replace("'", "''")
        category = category.replace("|", "-").replace("'", "''")
        return "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}\n".format(
            id,
            site,
            zip,
            date,
            Imagelink,
            price,
            price_mult,
            Eprice,
            Eindicator,
            altprice,
            Altpricemult,
            Ealtprice,
            Altindicator,
            pack,
            unit,
            uom,
            size,
            product_id,
            upc,
            asin,
            category,
            description,
            note,
            rating,
            valid,
            pageimage,
            aisle,
            orig_upc,
            job_number,
            sale_enddate,
            brand,
            extra1,
            extra2,
            extra3,
            extra4,
            extra5,
            extra6,
            extra7,
            extra8,
            extra9,
            LinkID)

    @staticmethod
    def create_data_list(site: str, zip: str, date: str, Imagelink: str, price: str, price_mult: str,
                         Eprice: str, Eindicator: str, altprice: str, Altpricemult: str, Ealtprice: str,
                         Altindicator: str, pack: str, unit: str, uom: str, size: str, product_id: str, upc: str,
                         asin: str, category: str, description: str, note: str, rating: str, valid: str, pageimage: str,
                         aisle: str, orig_upc: str, job_number: str, sale_enddate: str, brand: str, extra1: str,
                         extra2: str, extra3: str, extra4: str, extra5: str, extra6: str, extra7: str, extra8: str,
                         extra9: str, LinkID: str):
        description = description.replace("|", "-").replace("'", "''")
        note = note.replace("|", "-").replace("'", "''")
        category = category.replace("|", "-").replace("'", "''")
        return tuple(str(x) for x in (
            site, zip, date, Imagelink, price, price_mult, Eprice, Eindicator, altprice, Altpricemult, Ealtprice,
            Altindicator, pack, unit, uom, size, product_id, upc, asin, category, description, note, rating, valid,
            pageimage, aisle, orig_upc, job_number, sale_enddate, brand, extra1, extra2, extra3, extra4, extra5, extra6,
            extra7, extra8, extra9, LinkID))

    @staticmethod
    def insert_data(tablename, data):
        conn = ''
        if DBConfig.SQlServer == "CloudProduction":
            conn = pyodbc.connect(
                "Driver={SQL SERVER};"
                "Server=rdpdb-scp-1.database.windows.net;"
                "Database=WebscrapeIntegration;"
                "uid=rdbatch;pwd=N#mI&7afO5Tp")
        if DBConfig.SQlServer == "CloudDevelopment":
            conn = pyodbc.connect(
                "Driver={SQL SERVER};"
                "Server=rdddb-scp-1.database.windows.net;"
                "Database=WebscrapeIntegrationAuto;"
                "uid=rdbatch;pwd=6*MBF2LezCCQ")
        if DBConfig.SQlServer == "125":
            conn = pyodbc.connect(
                "Driver={SQL SERVER};"
                "Server=95.217.196.125;"
                "Database=webscrape;"
                "uid=sa;pwd=Dup(e)0@98!")

        cursor = conn.cursor()
        a = "insert into " + str(
            tablename) + " ( site, zip, date, Imagelink, price, price_mult, Eprice, Eindicator, altprice, Altpricemult, Ealtprice, Altindicator, pack, unit, uom, size, product_id, upc, asin, category, description, note, rating, valid, pageimage, aisle, orig_upc, job_number, sale_enddate, brand, extra1, extra2, extra3, extra4, extra5, extra6, extra7, extra8, extra9, LinkID) values (  ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        cursor.fast_executemany = True
        try:
            cursor.executemany(a, data)
            conn.commit()
            print('Data inserted into the ', tablename)
        except Exception as e:
            print(e)
        cursor.close()
