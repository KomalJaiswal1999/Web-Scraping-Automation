
# driver_path = "C:\Program Files\Google\Chrome\Application\chrome.exe"
# driver_path="C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
import base64
import json
import time

b=''

def getcookie(_Zip,_Store_id,_Address,_Type,_Url):
    global b
    try:
        option=webdriver.ChromeOptions()
        option.add_argument("--incognito")

        driver = webdriver.Chrome()
        driver.get(str(_Url)+"/")
        actions = ActionChains(driver)

        time.sleep(5)
        driver.find_element(By.ID, "shopping-selector-shop-context-intent-" + str(_Type)).click()
        time.sleep(3)
        # if 'https://shop.wegmans.com' in str(_Url) :
        #     classes = driver.find_elements(By.CLASS_NAME,"css-bt1vbo")
        #     classes[1].click()
        #
        # time.sleep(3)

        if 'instore' in _Type:
            driver.find_element(By.XPATH, "//button[@data-test='store-button']").click()

        # if 'https://shop.topsmarket.com' in str(_Url):
        #     driver.find_element(By.ID, "shopping-selector-shop-context-intent-" + str(_Type)).click()
        #     classes = driver.find_elements(By.CLASS_NAME, "css-l0ts54")
        #     classes[1].click()

        # time.sleep(3)
        # try:
        #     Zip_Textbox = driver.find_element(By.NAME,"cityZipInput")
        # except:

        Zip_Textbox = driver.find_element(By.NAME,"cityZipInput")
        Zip_Textbox.send_keys(str(_Zip))
        actions.send_keys(Keys.ENTER)

        postcode = str(_Address)
        WebDriverWait(driver, 40).until(EC.element_to_be_clickable((By.XPATH,
                                                                    '//button[@id=\'shopping-selector-update-home-store-' + str(
                                                                        _Store_id) + '-' + str(
                                                                        _Type) + '\']'))).send_keys(postcode)

        driver.get(str(_Url)+"/api/v2/store_products/facets?category_ids=265")
        print(_Url)
        cookies = driver.get_cookies()

        b=''
        a=[]

        for cookie in cookies:
            for i in cookie:

                if 'name' in i:
                    a.append(cookie[i])
                if 'value' in i:
                    var = a[0]+'='+str(cookie[i])+'; '
                    b=b+var
                    a.clear()
        # print(cookie)
        print(b[:-2])
        driver.close()
        return b[:-2]
        b.clear()

    except Exception as e:
        print(e)
        driver.close()
        getcookie(_Zip, _Store_id, _Address, _Type, _Url)

# getcookie('02155','82','oj88','instore')

def user_context_gen(_Type,_Store_id,_Url):
    sample_string = {"FulfillmentType": "instore", "Platform": "desktop", "StoreId": "72", "TrialUser": True}
    sample_string['FulfillmentType']=_Type
    sample_string['StoreId'] = _Store_id

    a = base64.urlsafe_b64encode(json.dumps(sample_string).encode()).decode()
    return a

