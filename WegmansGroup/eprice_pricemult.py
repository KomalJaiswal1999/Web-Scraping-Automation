class pricecols:

    def GetEquivalentValue(strPrice:str):
        pricemult = ""
        eprice = ""
        dictionary = {}
        try:
            strPrice = strPrice.replace("Â", "").replace("â€¢", "").replace("â€", "").replace("@", "").strip()
            strPrice = strPrice.replace("Â", "").replace("â€¢", "").replace("â€", "").replace("@", "").strip()
            strPrice = strPrice.replace("  ", " ")

            if('for' in strPrice.lower()):
                pricemult = strPrice.split('for')[0].strip().replace(" ", "")
                eprice = strPrice.split('for')[-1].split('/')[0].strip().replace(" ", "")
            elif('/' in strPrice):
                pricemult = "1"
                eprice = strPrice.replace("$", "").split('/')[0].strip()
            else:
                pricemult = "1"
                eprice = strPrice.replace("$","").strip()

            if(eprice==""):
                pricemult=""

            dictionary["pricemult"] = pricemult
            dictionary["eprice"] = eprice
        except:
            dictionary["pricemult"] = pricemult
            dictionary["eprice"] = eprice

        return dictionary
